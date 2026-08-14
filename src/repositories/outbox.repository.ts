import { prisma } from '../config/database.js';
import type { Prisma } from '@prisma/client';

export interface CreateOutboxEventData {
  eventType: string;
  aggregateType: string;
  aggregateId: string;
  payload: Prisma.InputJsonValue;
}

export const outboxRepository = {
  async create(data: CreateOutboxEventData, tx?: Prisma.TransactionClient) {
    const client = tx ?? prisma;

    return client.outboxEvent.create({
      data: {
        eventType: data.eventType,
        aggregateType: data.aggregateType,
        aggregateId: data.aggregateId,
        payload: data.payload,
        status: 'PENDING',
        attempts: 0,
        availableAt: new Date(),
      },
    });
  },
};
