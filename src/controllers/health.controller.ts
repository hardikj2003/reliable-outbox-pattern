import type { Request, Response } from 'express';
import { prisma } from '../config/database.js';
import { logger } from '../config/logger.js';

export function healthCheck(req: Request, res: Response): void {
  logger.debug({ reqId: req.id }, 'Health check requested');
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
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
