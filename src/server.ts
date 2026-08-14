import 'dotenv/config';
import { createApp } from './app.js';
import { logger } from './config/logger.js';
import { env } from './config/env.js';
import { prisma } from './config/database.js';
import { startOutboxPublisher, stopOutboxPublisher } from './workers/outbox.publisher.js';
import { closeRabbitMQ } from './config/rabbitmq.js';

const app = createApp();

const server = app.listen(env.PORT, () => {
  logger.info(`Server listening on port ${env.PORT} in ${env.NODE_ENV} mode`);
});

// Start outbox publisher in background
startOutboxPublisher().catch((err) => {
  logger.fatal({ err }, 'Outbox publisher failed to start');
});

async function shutdown(signal: string): Promise<void> {
  logger.info({ signal }, 'Shutting down gracefully');

  // Signal the publisher to stop after its current iteration
  stopOutboxPublisher();

  // Allow the publisher to finish its current poll cycle
  await new Promise((resolve) => setTimeout(resolve, 1500));

  server.close(async () => {
    await closeRabbitMQ();
    await prisma.$disconnect();
    logger.info('Server and connections closed');
    process.exit(0);
  });

  setTimeout(() => {
    logger.error('Forced shutdown due to timeout');
    process.exit(1);
  }, 10000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
