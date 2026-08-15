import { connect, type ChannelModel, type Channel } from 'amqplib';
import { env } from './env.js';
import { logger } from './logger.js';

let connection: ChannelModel | null = null;
const channels = new Set<Channel>();

async function getConnection(): Promise<ChannelModel> {
  if (connection) {
    return connection;
  }

  connection = await connect(env.RABBITMQ_URL);
  logger.info('RabbitMQ connection established');

  connection.on('error', (err: Error) => {
    logger.error({ err }, 'RabbitMQ connection error');
    connection = null;
    channels.clear();
  });

  connection.on('close', () => {
    logger.warn('RabbitMQ connection closed');
    connection = null;
    channels.clear();
  });

  return connection;
}

export async function createChannel(): Promise<Channel> {
  const conn = await getConnection();
  const channel = await conn.createChannel();
  channels.add(channel);

  channel.on('close', () => {
    channels.delete(channel);
  });

  channel.on('error', (err: Error) => {
    logger.error({ err }, 'RabbitMQ channel error');
    channels.delete(channel);
  });

  return channel;
}
export async function closeRabbitMQ(): Promise<void> {
  try {
    for (const ch of channels) {
      await ch.close();
    }
    channels.clear();

    if (connection) {
      await connection.close();
      connection = null;
    }
    logger.info('RabbitMQ connection closed');
  } catch (err) {
    logger.error({ err }, 'Error closing RabbitMQ connection');
  }
}
