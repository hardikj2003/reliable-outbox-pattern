import { afterAll, afterEach, beforeAll } from 'vitest';
import { prisma } from '../src/config/database.js';

// Clean database before all tests
export async function cleanupDatabase(): Promise<void> {
  await prisma.processedEvent.deleteMany();
  await prisma.outboxEvent.deleteMany();
  await prisma.orderItem.deleteMany();
  await prisma.order.deleteMany();
}

beforeAll(async () => {
  await cleanupDatabase();
});

afterEach(async () => {
  await cleanupDatabase();
});

afterAll(async () => {
  await prisma.$disconnect();
});
