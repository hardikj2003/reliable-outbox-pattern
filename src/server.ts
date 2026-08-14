import 'dotenv/config';
import { createApp } from './app.js';
import { logger } from './config/logger.js';
import { env } from './config/env.js';
import { prisma } from './config/database.js';

const app = createApp();

const server = app.listen(env.PORT, () => {
  logger.info(`Server listening on port ${env.PORT} in ${env.NODE_ENV} mode`);
});

async function shutdown(signal: string): Promise<void> {
  logger.info({ signal }, 'Shutting down gracefully');

  server.close(async () => {
    await prisma.$disconnect();
    logger.info('Server and database connections closed');
    process.exit(0);
  });

  setTimeout(() => {
    logger.error('Forced shutdown due to timeout');
    process.exit(1);
  }, 10000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
