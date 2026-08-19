import express from 'express';
import { errorHandler } from './middleware/errorHandler.js';
import healthRoutes from './routes/health.routes.js';
import orderRoutes from './routes/order.routes.js';
import simulationRoutes from './routes/simulation.routes.js';

export function createApp(): express.Application {
  const app = express();

  app.use(express.json());

  app.use('/health', healthRoutes);
  app.use('/orders', orderRoutes);

  // Simulation routes are only available in non-production environments
  if (process.env.NODE_ENV !== 'production') {
    app.use('/simulations', simulationRoutes);
  }

  app.use(errorHandler);

  return app;
}
