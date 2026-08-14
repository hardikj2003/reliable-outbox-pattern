import express from 'express';
import { requestId } from './middleware/requestId.js';
import { errorHandler } from './middleware/errorHandler.js';
import healthRoutes from './routes/health.routes.js';
import orderRoutes from './routes/order.routes.js';

export function createApp(): express.Application {
  const app = express();

  app.use(express.json());
  app.use(requestId);

  app.use('/health', healthRoutes);
  app.use('/orders', orderRoutes);

  app.use(errorHandler);

  return app;
}
