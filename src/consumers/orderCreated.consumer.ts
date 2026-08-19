import type { Channel, ConsumeMessage } from 'amqplib';
import { createChannel } from '../config/rabbitmq.js';
import { logger } from '../config/logger.js';
import { withIdempotency } from '../lib/idempotency.js';

const EXCHANGE_NAME = 'order.events';
const RETRY_EXCHANGE = 'order.retry';
const DLX_EXCHANGE = 'order.dlx';
const QUEUE_NAME = 'notifications.order.created';
const RETRY_QUEUE = `${QUEUE_NAME}.retry`;
const DLQ_NAME = `${QUEUE_NAME}.dlq`;
const ROUTING_KEY = 'order.created';
const MAX_RETRIES = 3;
const RETRY_TTL_MS = 5000;

let consumerChannel: Channel | null = null;

export async function startOrderCreatedConsumer(): Promise<void> {
  consumerChannel = await createChannel();

  //
  // EXCHANGES
  //
  // order.events  — main topic exchange (publisher sends here)
  // order.retry   — direct exchange for retry routing (main queue's DLX)
  // order.dlx     — direct exchange for dead letter routing (manual DLQ publish)
  //
  await consumerChannel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });
  await consumerChannel.assertExchange(RETRY_EXCHANGE, 'direct', { durable: true });
  await consumerChannel.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });

  //
  // RETRY QUEUE
  //
  // Messages nacked from the main queue land here via DLX.
  // They sit for RETRY_TTL_MS, then are dead-lettered back to
  // order.events exchange with the original routing key.
  // This routes them back to the main queue for reprocessing.
  //
  await consumerChannel.assertQueue(RETRY_QUEUE, {
    durable: true,
    arguments: {
      'x-message-ttl': RETRY_TTL_MS,
      'x-dead-letter-exchange': EXCHANGE_NAME,
      'x-dead-letter-routing-key': ROUTING_KEY,
    },
  });
  await consumerChannel.bindQueue(RETRY_QUEUE, RETRY_EXCHANGE, QUEUE_NAME);

  //
  // DEAD LETTER QUEUE (DLQ)
  //
  // Messages that exceeded MAX_RETRIES are published here manually
  // by the consumer. They sit until a human or automated process
  // inspects and handles them.
  //
  await consumerChannel.assertQueue(DLQ_NAME, { durable: true });
  await consumerChannel.bindQueue(DLQ_NAME, DLX_EXCHANGE, QUEUE_NAME);

  //
  // MAIN QUEUE
  //
  // x-dead-letter-exchange: when a message is nacked with requeue=false,
  // it is routed to the order.retry exchange instead of being dropped.
  //
  // x-dead-letter-routing-key: the routing key used when dead-lettering.
  // This ensures the message routes to the retry queue.
  //
  await consumerChannel.assertQueue(QUEUE_NAME, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': RETRY_EXCHANGE,
      'x-dead-letter-routing-key': QUEUE_NAME,
    },
  });
  await consumerChannel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

  consumerChannel.prefetch(1);

  logger.info(
    { queue: QUEUE_NAME, retryQueue: RETRY_QUEUE, dlq: DLQ_NAME, maxRetries: MAX_RETRIES, retryTtlMs: RETRY_TTL_MS },
    'OrderCreated consumer started with retry/DLQ'
  );

  consumerChannel.consume(QUEUE_NAME, async (msg: ConsumeMessage | null) => {
    if (!msg) {
      logger.warn('Consumer cancelled by broker');
      return;
    }

    try {
      const content = JSON.parse(msg.content.toString());
      const eventId = content.eventId as string;
      const eventType = content.eventType as string;

      //
      // Check how many times this message has been through the retry loop.
      //
      // RabbitMQ's x-death header tracks each dead-lettering event.
      // Each time the message expires from the retry queue and returns
      // to the main queue, a new entry is added for the retry queue.
      // We count these entries to know how many retries have occurred.
      //
      const headers = msg.properties.headers || {};
      const xDeath = headers['x-death'] as Array<{ queue: string; count: number }> | undefined;
      const retryEntry = xDeath?.find((d) => d.queue === RETRY_QUEUE);
      const retryCount = retryEntry?.count || 0;

      logger.info(
        { eventId, eventType, aggregateId: content.aggregateId, deliveryTag: msg.fields.deliveryTag, retryCount },
        'Received order.created event'
      );

      //
      // MAX RETRIES EXCEEDED → Route to DLQ
      //
      // If the message has already been retried MAX_RETRIES times,
      // we don't want to keep retrying forever. We publish it to the
      // DLQ and acknowledge the original message to remove it from
      // the main queue. A human or automated process can inspect
      // the DLQ later.
      //
      if (retryCount >= MAX_RETRIES) {
        logger.error(
          { eventId, retryCount, maxRetries: MAX_RETRIES },
          'Message exceeded max retries, routing to DLQ'
        );

        consumerChannel!.publish(DLX_EXCHANGE, QUEUE_NAME, msg.content, {
          ...msg.properties,
          headers: {
            ...headers,
            'x-dlq-reason': 'max_retries_exceeded',
            'x-dlq-at': new Date().toISOString(),
          },
        });
        consumerChannel!.ack(msg);
        return;
      }

      //
      // PROCESS WITH IDEMPOTENCY
      //
      const { isDuplicate } = await withIdempotency(
        eventId,
        eventType,
        async () => {
          logger.info(
            { eventId, orderId: content.aggregateId, customerEmail: content.payload?.customerEmail },
            'Processing order.created side effect'
          );
          await simulateSideEffect();
          logger.info({ eventId }, 'Order.created side effect completed');
        }
      );

      if (isDuplicate) {
        logger.info({ eventId }, 'Duplicate event safely acknowledged');
      }

      //
      // SUCCESS → ACK
      //
      // Manual acknowledgement tells RabbitMQ this message is done.
      // It is removed from the queue and not redelivered.
      //
      consumerChannel!.ack(msg);
    } catch (err) {
      //
      // FAILURE → NACK with requeue=false
      //
      // requeue=false triggers the dead-letter exchange (DLX) configured
      // on the main queue. The message is routed to order.retry exchange,
      // then to the retry queue where it sits for RETRY_TTL_MS before
      // being routed back to the main queue.
      //
      logger.error(
        { err, deliveryTag: msg.fields.deliveryTag },
        'Processing failed, sending to retry queue'
      );
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

async function simulateSideEffect(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 50));
}
