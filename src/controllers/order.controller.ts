import type { Request, Response, NextFunction } from 'express';
import { z } from 'zod';
import { orderService } from '../services/order.service.js';
import { logger } from '../config/logger.js';

const createOrderSchema = z.object({
  customerEmail: z.string().email('Invalid email address'),
  items: z
    .array(
      z.object({
        productId: z.string().min(1, 'Product ID is required'),
        quantity: z.number().int().positive('Quantity must be a positive integer'),
        unitPrice: z.number().positive('Unit price must be positive'),
      })
    )
    .min(1, 'At least one item is required'),
});

export const orderSchemas = {
  create: createOrderSchema,
};

export async function createOrder(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const order = await orderService.createOrder(req.body);
    res.status(201).json(order);
  } catch (err) {
    next(err);
  }
}

export async function getOrderById(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const { id } = req.params;
    const order = await orderService.getOrderById(id);

    if (!order) {
      logger.warn({ reqId: req.id, orderId: id }, 'Order not found');
      res.status(404).json({ error: 'Order not found' });
      return;
    }

    res.json(order);
  } catch (err) {
    next(err);
  }
}
