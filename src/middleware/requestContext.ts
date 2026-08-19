import type { Request, Response, NextFunction } from 'express';
import { randomUUID } from 'node:crypto';
import { asyncLocalStorage } from '../lib/asyncContext.js';

export function requestContext(req: Request, res: Response, next: NextFunction): void {
  const requestId = req.get('x-request-id') || randomUUID();
  req.id = requestId;
  res.setHeader('x-request-id', requestId);

  // Run the rest of the request lifecycle inside an AsyncLocalStorage context.
  // This allows any async code (services, repositories, workers) to access
  // the requestId without threading it through every function parameter.
  asyncLocalStorage.run({ requestId }, () => {
    next();
  });
}
