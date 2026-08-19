import type { Request, Response } from 'express';
import { prisma } from '../config/database.js';
import { logger } from '../config/logger.js';
import { metrics } from '../lib/metrics.js';

export function healthCheck(req: Request, res: Response): void {
  logger.debug({ reqId: req.id }, 'Health check requested');
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
  });
}

export async function readinessCheck(req: Request, res: Response): Promise<void> {
  try {
    await prisma.$queryRaw`SELECT 1`;
    res.json({
      status: 'ready',
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    logger.error({ err, reqId: req.id }, 'Readiness check failed');
    res.status(503).json({
      status: 'not ready',
      error: 'Database unavailable',
    });
  }
}

export function metricsEndpoint(_req: Request, res: Response): void {
  const summary = metrics.getSummary();
  const allMetrics = metrics.getAll();

  // Outbox backlog
  prisma.outboxEvent.count({ where: { status: 'PENDING' } }).then((pendingCount) => {
    res.json({
      timestamp: new Date().toISOString(),
      summary,
      backlog: {
        pendingOutboxEvents: pendingCount,
      },
      recentMetrics: allMetrics.slice(-50),
    });
  }).catch((err) => {
    logger.error({ err }, 'Failed to fetch metrics');
    res.status(500).json({ error: 'Failed to fetch metrics' });
  });
}
