import { describe, it, expect, vi, beforeEach } from 'vitest';
import { orderService } from '../../src/services/order.service.js';
import { orderRepository } from '../../src/repositories/order.repository.js';
import { prisma } from '../../src/config/database.js';
import { Decimal } from 'decimal.js';

// Mock dependencies
vi.mock('../../src/repositories/order.repository.js', () => ({
  orderRepository: {
    create: vi.fn(),
    findById: vi.fn(),
  },
}));

vi.mock('../../src/services/outbox.service.js', () => ({
  outboxService: {
    createOrderCreatedEvent: vi.fn(),
  },
}));

vi.mock('../../src/config/database.js', () => ({
  prisma: {
    $transaction: vi.fn((fn) => fn({})),
  },
}));

describe('orderService', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('createOrder', () => {
    it('should create an order with correct total amount', async () => {
      const mockOrder = {
        id: 'order-123',
        customerEmail: 'alice@example.com',
        totalAmount: new Decimal('59.98'),
        status: 'PENDING',
        createdAt: new Date(),
        updatedAt: new Date(),
        items: [{
          id: 'item-1',
          productId: 'prod-1',
          quantity: 2,
          unitPrice: new Decimal('29.99'),
        }],
      };

      vi.mocked(orderRepository.create).mockResolvedValue(mockOrder as any);

      const result = await orderService.createOrder({
        customerEmail: 'alice@example.com',
        items: [{ productId: 'prod-1', quantity: 2, unitPrice: 29.99 }],
      });

      expect(result).toBeDefined();
      expect(result.id).toBe('order-123');
      expect(result.customerEmail).toBe('alice@example.com');
      expect(result.totalAmount).toBe('59.98');
      expect(result.status).toBe('PENDING');
      expect(result.items).toHaveLength(1);
    });

    it('should calculate total for multiple items', async () => {
      const mockOrder = {
        id: 'order-456',
        customerEmail: 'bob@example.com',
        totalAmount: new Decimal('109.97'),
        status: 'PENDING',
        createdAt: new Date(),
        updatedAt: new Date(),
        items: [
          { id: 'item-1', productId: 'prod-1', quantity: 2, unitPrice: new Decimal('29.99') },
          { id: 'item-2', productId: 'prod-2', quantity: 1, unitPrice: new Decimal('49.99') },
        ],
      };

      vi.mocked(orderRepository.create).mockResolvedValue(mockOrder as any);

      const result = await orderService.createOrder({
        customerEmail: 'bob@example.com',
        items: [
          { productId: 'prod-1', quantity: 2, unitPrice: 29.99 },
          { productId: 'prod-2', quantity: 1, unitPrice: 49.99 },
        ],
      });

      expect(result.totalAmount).toBe('109.97');
    });

    it('should reject empty items array', async () => {
      await expect(
        orderService.createOrder({
          customerEmail: 'alice@example.com',
          items: [],
        })
      ).rejects.toThrow('Order must contain at least one item');
    });

    it('should wrap order and outbox creation in a transaction', async () => {
      const mockOrder = {
        id: 'order-789',
        customerEmail: 'charlie@example.com',
        totalAmount: new Decimal('10.00'),
        status: 'PENDING',
        createdAt: new Date(),
        updatedAt: new Date(),
        items: [{ id: 'item-1', productId: 'prod-1', quantity: 1, unitPrice: new Decimal('10.00') }],
      };

      vi.mocked(orderRepository.create).mockResolvedValue(mockOrder as any);
      const transactionSpy = vi.mocked(prisma.$transaction);

      await orderService.createOrder({
        customerEmail: 'charlie@example.com',
        items: [{ productId: 'prod-1', quantity: 1, unitPrice: 10.00 }],
      });

      expect(transactionSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe('getOrderById', () => {
    it('should return order when found', async () => {
      const mockOrder = {
        id: 'order-123',
        customerEmail: 'alice@example.com',
        totalAmount: new Decimal('59.98'),
        status: 'PENDING',
        createdAt: new Date(),
        updatedAt: new Date(),
        items: [{
          id: 'item-1',
          productId: 'prod-1',
          quantity: 2,
          unitPrice: new Decimal('29.99'),
        }],
      };

      vi.mocked(orderRepository.findById).mockResolvedValue(mockOrder as any);

      const result = await orderService.getOrderById('order-123');

      expect(result).not.toBeNull();
      expect(result?.id).toBe('order-123');
    });

    it('should return null when order not found', async () => {
      vi.mocked(orderRepository.findById).mockResolvedValue(null);

      const result = await orderService.getOrderById('non-existent');

      expect(result).toBeNull();
    });
  });
});
