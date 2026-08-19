import type { Channel } from "amqplib";
import { prisma } from "../config/database.js";
import { logger } from "../config/logger.js";
import { createChannel } from "../config/rabbitmq.js";
import { simulation } from "../lib/simulation.js";
import { metrics } from "../lib/metrics.js";

const POLL_INTERVAL_MS = 5000;
const BATCH_SIZE = 10;
const MAX_RETRY_ATTEMPTS = 5;

let isRunning = false;

interface OutboxEventRow {
  id: string;
  event_type: string;
  aggregate_type: string;
  aggregate_id: string;
  payload: unknown;
  attempts: number;
}

function getBackoffDelay(attempts: number): number {
  return Math.min(1000 * Math.pow(2, attempts), 30000);
}

export async function startOutboxPublisher(): Promise<void> {
  isRunning = true;
  logger.info("Outbox publisher starting");
  metrics.increment("publisher.start.count");

  while (isRunning) {
    try {
      const channel = await createChannel();
      await channel.assertExchange("order.events", "topic", { durable: true });

      while (isRunning) {
        try {
          await pollAndPublish(channel);
        } catch (err) {
          logger.error({ err }, "Outbox publisher poll cycle failed");
          metrics.increment("publisher.poll.error.count");
        }
        await sleep(POLL_INTERVAL_MS);
      }
    } catch (err) {
      logger.error({ err }, "RabbitMQ connection failed, retrying in 5s");
      metrics.increment("publisher.connection.error.count");
      await sleep(5000);
    }
  }

  logger.info("Outbox publisher stopped");
}

export function stopOutboxPublisher(): void {
  isRunning = false;
}

async function pollAndPublish(channel: Channel): Promise<void> {
  const pollTimer = metrics.timer("publisher.poll.duration");

  const events = await prisma.$transaction(async (tx) => {
    const rows = await tx.$queryRaw<OutboxEventRow[]>`
      SELECT
        id,
        event_type,
        aggregate_type,
        aggregate_id,
        payload,
        attempts
      FROM outbox_events
      WHERE status = 'PENDING'
        AND available_at <= NOW()
      ORDER BY created_at ASC
      LIMIT ${BATCH_SIZE}
      FOR UPDATE SKIP LOCKED
    `;
    return rows;
  });

  pollTimer.stop();

  if (events.length === 0) {
    return;
  }

  logger.debug(
    { count: events.length },
    "Claimed outbox events for publishing",
  );
  metrics.record("publisher.claimed.count", events.length, "count");

  for (const event of events) {
    const publishTimer = metrics.timer("publisher.publish.duration");

    try {
      const messageBuffer = Buffer.from(
        JSON.stringify({
          eventId: event.id,
          eventType: event.event_type,
          aggregateType: event.aggregate_type,
          aggregateId: event.aggregate_id,
          payload: event.payload,
          occurredAt: new Date().toISOString(),
        }),
      );

      const routingKey = event.event_type;

      const published = channel.publish(
        "order.events",
        routingKey,
        messageBuffer,
        {
          persistent: true,
          messageId: event.id,
          contentType: "application/json",
        },
      );

      if (!published) {
        throw new Error("RabbitMQ channel write buffer full");
      }

      if (simulation.shouldSkipMarkAfterPublish()) {
        logger.warn(
          { eventId: event.id },
          "[SIMULATION] Skipping mark-as-published after successful publish",
        );
        continue;
      }

      await prisma.outboxEvent.update({
        where: { id: event.id },
        data: {
          status: "PUBLISHED",
          publishedAt: new Date(),
          lastError: null,
        },
      });

      publishTimer.stop();
      metrics.increment("publisher.publish.success.count");

      logger.info(
        { eventId: event.id, routingKey, aggregateId: event.aggregate_id },
        "Outbox event published successfully",
      );
    } catch (err) {
      publishTimer.stop();
      metrics.increment("publisher.publish.error.count");

      const errorMessage = err instanceof Error ? err.message : String(err);
      const newAttempts = event.attempts + 1;

      if (newAttempts >= MAX_RETRY_ATTEMPTS) {
        await prisma.outboxEvent.update({
          where: { id: event.id },
          data: {
            status: "FAILED",
            attempts: newAttempts,
            lastError: errorMessage,
          },
        });
        metrics.increment("publisher.publish.failed.count");
        logger.error(
          { eventId: event.id, attempts: newAttempts, error: errorMessage },
          "Outbox event exceeded max retry attempts",
        );
      } else {
        const backoffMs = getBackoffDelay(newAttempts);
        await prisma.outboxEvent.update({
          where: { id: event.id },
          data: {
            attempts: newAttempts,
            availableAt: new Date(Date.now() + backoffMs),
            lastError: errorMessage,
          },
        });
        metrics.increment("publisher.publish.retry.count");
        logger.warn(
          {
            eventId: event.id,
            attempts: newAttempts,
            backoffMs,
            error: errorMessage,
          },
          "Failed to publish outbox event, scheduled retry",
        );
      }
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
