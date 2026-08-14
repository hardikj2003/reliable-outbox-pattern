import type { Request, Response, NextFunction } from 'express';
import { ZodSchema } from 'zod';
import { logger } from '../config/logger.js';

export function requestValidator<T>(schema: ZodSchema<T>) {
  return (req: Request, res: Response, next: NextFunction): void => {
    const result = schema.safeParse(req.body);

    if (!result.success) {
      const issues = result.error.issues.map((issue) => ({
        path: issue.path.join('.'),
        message: issue.message,
      }));

      logger.warn({ reqId: req.id, issues }, 'Request validation failed');

      res.status(400).json({
        error: 'Validation failed',
        issues,
      });
      return;
    }

    req.body = result.data;
    next();
  };
}
