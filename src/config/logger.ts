import pino from 'pino';
import { env } from './env.js';

const baseLogger = pino({
  level: env.LOG_LEVEL,
  transport:
    env.NODE_ENV === 'development'
      ? { target: 'pino-pretty', options: { colorize: true, translateTime: 'HH:MM:ss.l' } }
      : undefined,
  base: {
    pid: process.pid,
    env: env.NODE_ENV,
  },
});

/**
 * Creates a child logger bound to a request/correlation context.
 * Every log emitted by this child automatically includes the context fields.
 */
export function getLogger(context: Record<string, unknown> = {}) {
  return baseLogger.child(context);
}

export const logger = baseLogger;
