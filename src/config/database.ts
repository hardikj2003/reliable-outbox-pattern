import { PrismaClient } from '@prisma/client';
import { env } from './env.js';

const globalForPrisma = globalThis as unknown as {
  prisma: PrismaClient | undefined;
};

const logOptions: Array<'query' | 'warn' | 'error'> =
  env.NODE_ENV === 'development' ? ['query', 'warn', 'error'] : ['error'];

export const prisma =
  globalForPrisma.prisma ??
  new PrismaClient({
    log: logOptions,
  });

if (env.NODE_ENV !== 'production') {
  globalForPrisma.prisma = prisma;
}
