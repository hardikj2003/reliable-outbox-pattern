import type { Channel, ConsumeMessage } from "amqplib";
import { createChannel } from "../config/rabbitmq.js";
import { logger } from "../config/logger.js";
import { withIdempotency } from "../lib/idempotency.js";
import { simulation } from "../lib/simulation.js";
import { metrics } from "../lib/metrics.js";

const EXCHANGE_NAME = "order.events";
const RETRY_EXCHANGE = "order.retry";
const DLX_EXCHANGE = "order.dlx";
const QUEUE_NAME = "notifications.order.created";
const RETRY_QUEUE = `${QUEUE_NAME}.retry`;
const DLQ_NAME = `${QUEUE_NAME}.dlq`;
const ROUTING_KEY = "order.created";
const MAX_RETRIES = 3;
const RETRY_TTL_MS = 5000;

let consumerChannel: Channel | null = null;

export async function startOrderCreatedConsumer(): Promise<void> {
  consumerChannel = await createChannel();

  await consumerChannel.assertExchange(EXCHANGE_NAME, "topic", {
    durable: true,
  });
  await consumerChannel.assertExchange(RETRY_EXCHANGE, "direct", {
    durable: true,
  });
  await consumerChannel.assertExchange(DLX_EXCHANGE, "direct", {
    durable: true,
  });

  await consumerChannel.assertQueue(RETRY_QUEUE, {
    durable: true,
    arguments: {
      "x-message-ttl": RETRY_TTL_MS,
      "x-dead-letter-exchange": EXCHANGE_NAME,
      "x-dead-letter-routing-key": ROUTING_KEY,
    },
  });
  await consumerChannel.bindQueue(RETRY_QUEUE, RETRY_EXCHANGE, QUEUE_NAME);

  await consumerChannel.assertQueue(DLQ_NAME, { durable: true });
  await consumerChannel.bindQueue(DLQ_NAME, DLX_EXCHANGE, QUEUE_NAME);

  await consumerChannel.assertQueue(QUEUE_NAME, {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": RETRY_EXCHANGE,
      "x-dead-letter-routing-key": QUEUE_NAME,
    },
  });
  await consumerChannel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

  consumerChannel.prefetch(1);

  logger.info(
    {
      queue: QUEUE_NAME,
      retryQueue: RETRY_QUEUE,
      dlq: DLQ_NAME,
      maxRetries: MAX_RETRIES,
      retryTtlMs: RETRY_TTL_MS,
    },
    "OrderCreated consumer started with retry/DLQ",
  );

  consumerChannel.consume(QUEUE_NAME, async (msg: ConsumeMessage | null) => {
    if (!msg) {
      logger.warn("Consumer cancelled by broker");
      return;
    }

    const processTimer = metrics.timer("consumer.process.duration");

    try {
      const content = JSON.parse(msg.content.toString());
      const eventId = content.eventId as string;
      const eventType = content.eventType as string;

      const headers = msg.properties.headers || {};
      const xDeath = headers["x-death"] as
        | Array<{ queue: string; count: number }>
        | undefined;
      const retryEntry = xDeath?.find((d) => d.queue === RETRY_QUEUE);
      const retryCount = retryEntry?.count || 0;

      logger.info(
        {
          eventId,
          eventType,
          aggregateId: content.aggregateId,
          deliveryTag: msg.fields.deliveryTag,
          retryCount,
        },
        "Received order.created event",
      );

      if (retryCount >= MAX_RETRIES) {
        logger.error(
          { eventId, retryCount, maxRetries: MAX_RETRIES },
          "Message exceeded max retries, routing to DLQ",
        );
        metrics.increment("consumer.dlq.count");

        consumerChannel!.publish(DLX_EXCHANGE, QUEUE_NAME, msg.content, {
          ...msg.properties,
          headers: {
            ...headers,
            "x-dlq-reason": "max_retries_exceeded",
            "x-dlq-at": new Date().toISOString(),
          },
        });
        consumerChannel!.ack(msg);
        processTimer.stop();
        return;
      }

      if (simulation.shouldCrashBeforeAck()) {
        logger.warn(
          { eventId, deliveryTag: msg.fields.deliveryTag },
          "[SIMULATION] Simulating consumer crash before ACK",
        );
        throw new Error("[SIMULATION] Consumer crash before ACK");
      }

      if (simulation.shouldFailRepeatedly()) {
        logger.warn(
          { eventId, retryCount },
          "[SIMULATION] Simulating repeated consumer failure",
        );
        throw new Error("[SIMULATION] Repeated consumer failure");
      }

      const { isDuplicate } = await withIdempotency(
        eventId,
        eventType,
        async () => {
          logger.info(
            {
              eventId,
              orderId: content.aggregateId,
              customerEmail: content.payload?.customerEmail,
            },
            "Processing order.created side effect",
          );
          await simulateSideEffect();
          logger.info({ eventId }, "Order.created side effect completed");
        },
      );

      if (isDuplicate) {
        logger.info({ eventId }, "Duplicate event safely acknowledged");
        metrics.increment("consumer.duplicate.count");
      } else {
        metrics.increment("consumer.process.success.count");
      }

      consumerChannel!.ack(msg);
      processTimer.stop();
    } catch (err) {
      processTimer.stop();
      metrics.increment("consumer.process.error.count");
      logger.error(
        { err, deliveryTag: msg.fields.deliveryTag },
        "Processing failed, sending to retry queue",
      );
      consumerChannel!.nack(msg, false, false);
    }
  });
}

export async function stopOrderCreatedConsumer(): Promise<void> {
  if (consumerChannel) {
    await consumerChannel.close();
    consumerChannel = null;
    logger.info("OrderCreated consumer stopped");
  }
}

async function simulateSideEffect(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 50));
}
