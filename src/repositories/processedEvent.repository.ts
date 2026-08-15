import { prisma } from '../config/database.js';
import type { Prisma } from '@prisma/client';

export interface CreateProcessedEventData {
  eventId: string;
  eventType: string;
}

export const processedEventRepository = {
  async findByEventId(eventId: string, tx?: Prisma.TransactionClient) {
    const client = tx ?? prisma;
    return client.processedEvent.findUnique({
      where: { eventId },
    });
  },

  async create(data: CreateProcessedEventData, tx?: Prisma.TransactionClient) {
    const client = tx ?? prisma;
    return client.processedEvent.create({
      data: {
        eventId: data.eventId,
        eventType: data.eventType,
      },
    });
  },
};
