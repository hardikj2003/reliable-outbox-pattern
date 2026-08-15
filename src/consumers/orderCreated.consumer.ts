import type { Channel, ConsumeMessage } from "amqplib";
import { createChannel } from "../config/rabbitmq.js";
import { logger } from "../config/logger.js";
import { withIdempotency } from "../lib/idempotency.js";

const EXCHANGE_NAME = "order.events";
const QUEUE_NAME = "notifications.order.created";
const ROUTING_KEY = "order.created";

let consumerChannel: Channel | null = null;

export async function startOrderCreatedConsumer(): Promise<void> {
  consumerChannel = await createChannel();

  await consumerChannel.assertExchange(EXCHANGE_NAME, "topic", {
    durable: true,
  });
  const { queue } = await consumerChannel.assertQueue(QUEUE_NAME, {
    durable: true,
  });
  await consumerChannel.bindQueue(queue, EXCHANGE_NAME, ROUTING_KEY);
  consumerChannel.prefetch(1);

  logger.info(
    { queue, routingKey: ROUTING_KEY },
    "OrderCreated consumer started",
  );

  consumerChannel.consume(queue, async (msg: ConsumeMessage | null) => {
    if (!msg) {
      logger.warn("Consumer cancelled by broker");
      return;
    }

    try {
      const content = JSON.parse(msg.content.toString());
      const eventId = content.eventId as string;
      const eventType = content.eventType as string;

      logger.info(
        {
          eventId,
          eventType,
          aggregateId: content.aggregateId,
          deliveryTag: msg.fields.deliveryTag,
        },
        "Received order.created event",
      );

      //
      // Idempotent processing:
      // - If event was already processed → skip side effects, ACK
      // - If new event → execute side effects, record in processed_events, ACK
      // - If race condition → unique constraint catches it, treat as duplicate, ACK
      //
      const { isDuplicate } = await withIdempotency(
        eventId,
        eventType,
        async () => {
          //
          // BUSINESS LOGIC GOES HERE.
          //
          // Example: send notification email, update analytics, etc.
          // For this portfolio project, we simulate the side effect with a log.
          //
          logger.info(
            {
              eventId,
              orderId: content.aggregateId,
              customerEmail: content.payload?.customerEmail,
            },
            "Processing order.created side effect",
          );

          // Simulate async work (e.g., calling an email service)
          await simulateSideEffect();

          logger.info({ eventId }, "Order.created side effect completed");
        },
      );

      if (isDuplicate) {
        logger.info({ eventId }, "Duplicate event safely acknowledged");
      }

      // Always ACK — duplicates are handled, so we don't want RabbitMQ
      // to redeliver. The idempotency layer guarantees exactly-once effects.
      consumerChannel!.ack(msg);
    } catch (err) {
      logger.error(
        { err, deliveryTag: msg.fields.deliveryTag },
        "Failed to process order.created event",
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
  // Simulate a real side effect like sending an email or calling an external API.
  // In production, this would be: await emailService.sendOrderConfirmation(...)
  await new Promise((resolve) => setTimeout(resolve, 50));
}
