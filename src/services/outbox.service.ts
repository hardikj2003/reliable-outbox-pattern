import { outboxRepository } from '../repositories/outbox.repository.js';
import type { Prisma } from '@prisma/client';
import type { OrderOutput } from '../types/index.js';
import { OUTBOX_EVENT_TYPES } from '../events/order.events.js';

export const outboxService = {
  async createOrderCreatedEvent(
    order: OrderOutput,
    tx?: Prisma.TransactionClient
  ) {
    return outboxRepository.create(
      {
        eventType: OUTBOX_EVENT_TYPES.ORDER_CREATED,
        aggregateType: 'order',
        aggregateId: order.id,
        payload: {
          orderId: order.id,
          customerEmail: order.customerEmail,
          totalAmount: order.totalAmount,
          status: order.status,
          items: order.items,
          createdAt: order.createdAt.toISOString(),
        }as unknown as Prisma.InputJsonValue,
      },
      tx
    );
  },
};
