import { prisma } from '../config/database.js';
import type { Prisma } from '@prisma/client';

export interface CreateOrderData {
  customerEmail: string;
  totalAmount: Prisma.Decimal;
  status: 'PENDING';
  items: {
    productId: string;
    quantity: number;
    unitPrice: Prisma.Decimal;
  }[];
}

export const orderRepository = {
  async create(data: CreateOrderData, tx?: Prisma.TransactionClient) {
    const client = tx ?? prisma;

    return client.order.create({
      data: {
        customerEmail: data.customerEmail,
        totalAmount: data.totalAmount,
        status: data.status,
        items: {
          create: data.items,
        },
      },
      include: {
        items: true,
      },
    });
  },

  async findById(id: string, tx?: Prisma.TransactionClient) {
    const client = tx ?? prisma;

    return client.order.findUnique({
      where: { id },
      include: { items: true },
    });
  },
};
