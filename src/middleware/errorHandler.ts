import type { Request, Response, NextFunction } from 'express';
import { logger } from '../config/logger.js';
import { getCurrentContext } from '../lib/asyncContext.js';

interface HttpError extends Error {
  statusCode?: number;
}

export function errorHandler(
  err: HttpError,
  req: Request,
  res: Response,
  _next: NextFunction
): void {
  const statusCode = err.statusCode ?? 500;
  const isDev = process.env.NODE_ENV === 'development';
  const context = getCurrentContext();

  logger.error(
    {
      err: isDev ? err : { message: err.message, name: err.name },
      reqId: req.id,
      requestId: context?.requestId,
      statusCode,
      path: req.path,
      method: req.method,
    },
    err.message || 'Unhandled error'
  );

  res.status(statusCode).json({
    error: {
      message: isDev ? err.message : 'Internal Server Error',
      requestId: req.id,
    },
  });
}
