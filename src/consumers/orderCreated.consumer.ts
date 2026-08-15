import type { Channel, ConsumeMessage } from 'amqplib';
import { createChannel } from '../config/rabbitmq.js';
import { logger } from '../config/logger.js';

const EXCHANGE_NAME = 'order.events';
const QUEUE_NAME = 'notifications.order.created';
const ROUTING_KEY = 'order.created';

let consumerChannel: Channel | null = null;

export async function startOrderCreatedConsumer(): Promise<void> {
  consumerChannel = await createChannel();

  // Assert exchange (idempotent — safe to call multiple times)
  await consumerChannel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });

  // Assert durable queue (survives broker restart)
  const { queue } = await consumerChannel.assertQueue(QUEUE_NAME, { durable: true });

  // Bind queue to exchange: only receive messages matching this routing key
  await consumerChannel.bindQueue(queue, EXCHANGE_NAME, ROUTING_KEY);

  // Prefetch = 1: only deliver 1 unacked message at a time to this consumer
  // This prevents overwhelming a slow consumer and enables fair work distribution
  consumerChannel.prefetch(1);

  logger.info({ queue, routingKey: ROUTING_KEY }, 'OrderCreated consumer started');

  consumerChannel.consume(queue, async (msg: ConsumeMessage | null) => {
    if (!msg) {
      // Broker cancelled the consumer (e.g., queue deleted)
      logger.warn('Consumer cancelled by broker');
      return;
    }

    try {
      const content = JSON.parse(msg.content.toString());

      logger.info(
        {
          eventId: content.eventId,
          eventType: content.eventType,
          aggregateId: content.aggregateId,
          deliveryTag: msg.fields.deliveryTag,
        },
        'Received order.created event'
      );

      // Phase 6: Idempotency check goes here
      // Phase 6: Business logic goes here (e.g., send notification email)

      // Manual acknowledgement: tell RabbitMQ we processed successfully
      // Only ack AFTER successful processing. If we crash before this line,
      // RabbitMQ will redeliver the message to another consumer.
      consumerChannel!.ack(msg);

      logger.debug({ eventId: content.eventId }, 'Event acknowledged');
    } catch (err) {
      logger.error(
        { err, deliveryTag: msg.fields.deliveryTag },
        'Failed to process order.created event'
      );

      // Negative acknowledgement without requeue
      // Phase 7: This will route the message to a Dead Letter Queue
      consumerChannel!.nack(msg, false, false);
    }
  });
}

export async function stopOrderCreatedConsumer(): Promise<void> {
  if (consumerChannel) {
    await consumerChannel.close();
    consumerChannel = null;
    logger.info('OrderCreated consumer stopped');
  }
}
