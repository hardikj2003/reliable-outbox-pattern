import type { Request, Response, NextFunction } from 'express';
import { randomUUID } from 'node:crypto';

export function requestId(req: Request, res: Response, next: NextFunction): void {
  const id = req.get('x-request-id') || randomUUID();
  req.id = id;
  res.setHeader('x-request-id', id);
  next();
}
