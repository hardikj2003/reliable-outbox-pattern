import { Decimal } from 'decimal.js';
import { prisma } from '../config/database.js';
import { orderRepository } from '../repositories/order.repository.js';
import { outboxService } from './outbox.service.js';
import { logger } from '../config/logger.js';
import type { CreateOrderInput, OrderOutput } from '../types/index.js';

function calculateTotal(items: CreateOrderInput['items']): Decimal {
  return items.reduce(
    (sum, item) => sum.plus(new Decimal(item.unitPrice).times(item.quantity)),
    new Decimal(0)
  );
}

function toOrderOutput(order: Awaited<ReturnType<typeof orderRepository.create>>): OrderOutput {
  return {
    id: order.id,
    customerEmail: order.customerEmail,
    totalAmount: order.totalAmount.toString(),
    status: order.status,
    createdAt: order.createdAt,
    updatedAt: order.updatedAt,
    items: order.items.map((item) => ({
      id: item.id,
      productId: item.productId,
      quantity: item.quantity,
      unitPrice: item.unitPrice.toString(),
    })),
  };
}

export const orderService = {
  async createOrder(input: CreateOrderInput): Promise<OrderOutput> {
    if (!input.items.length) {
      const error = new Error('Order must contain at least one item');
      (error as Error & { statusCode: number }).statusCode = 400;
      throw error;
    }

    const totalAmount = calculateTotal(input.items);

    logger.info(
      { customerEmail: input.customerEmail, itemCount: input.items.length },
      'Creating order'
    );

    const order = await prisma.$transaction(async (tx) => {
      const createdOrder = await orderRepository.create(
        {
          customerEmail: input.customerEmail,
          totalAmount,
          status: 'PENDING',
          items: input.items.map((item) => ({
            productId: item.productId,
            quantity: item.quantity,
            unitPrice: new Decimal(item.unitPrice),
          })),
        },
        tx
      );

      const orderOutput = toOrderOutput(createdOrder);

      await outboxService.createOrderCreatedEvent(orderOutput, tx);

      return createdOrder;
    });

    logger.info({ orderId: order.id }, 'Order created with outbox event');

    return toOrderOutput(order);
  },

  async getOrderById(id: string): Promise<OrderOutput | null> {
    const order = await orderRepository.findById(id);

    if (!order) {
      return null;
    }

    return toOrderOutput(order);
  },
};
