import { connect, type ChannelModel, type Channel } from 'amqplib';
import { env } from './env.js';
import { logger } from './logger.js';

let connection: ChannelModel | null = null;
let channel: Channel | null = null;

export async function getRabbitMQChannel(): Promise<Channel> {
  if (channel) {
    return channel;
  }

  connection = await connect(env.RABBITMQ_URL);
  channel = await connection.createChannel();

  logger.info('RabbitMQ connection established');

  connection.on('error', (err: Error) => {
    logger.error({ err }, 'RabbitMQ connection error');
    channel = null;
    connection = null;
  });

  connection.on('close', () => {
    logger.warn('RabbitMQ connection closed');
    channel = null;
    connection = null;
  });

  return channel;
}

export async function closeRabbitMQ(): Promise<void> {
  try {
    if (channel) {
      await channel.close();
      channel = null;
    }

    if (connection) {
      await connection.close();
      connection = null;
    }

    logger.info('RabbitMQ connection closed');
  } catch (err) {
    logger.error({ err }, 'Error closing RabbitMQ connection');
  }
}