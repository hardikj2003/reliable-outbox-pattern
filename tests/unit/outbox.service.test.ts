import { describe, it, expect, vi, beforeEach } from 'vitest';
import { outboxService } from '../../src/services/outbox.service.js';
import { outboxRepository } from '../../src/repositories/outbox.repository.js';
import { OUTBOX_EVENT_TYPES } from '../../src/events/order.events.js';

vi.mock('../../src/repositories/outbox.repository.js', () => ({
  outboxRepository: {
    create: vi.fn(),
  },
}));

describe('outboxService', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should create order.created event with correct payload', async () => {
    vi.mocked(outboxRepository.create).mockResolvedValue({
      id: 'outbox-123',
      eventType: 'order.created',
      aggregateType: 'order',
      aggregateId: 'order-456',
      payload: {},
      status: 'PENDING',
      attempts: 0,
      availableAt: new Date(),
      createdAt: new Date(),
    } as any);

    const order = {
      id: 'order-456',
      customerEmail: 'alice@example.com',
      totalAmount: '59.98',
      status: 'PENDING',
      createdAt: new Date('2026-08-19T10:00:00Z'),
      updatedAt: new Date('2026-08-19T10:00:00Z'),
      items: [
        { id: 'item-1', productId: 'prod-1', quantity: 2, unitPrice: '29.99' },
      ],
    };

    const result = await outboxService.createOrderCreatedEvent(order);

    expect(result).toBeDefined();
    expect(outboxRepository.create).toHaveBeenCalledWith(
      {
        eventType: OUTBOX_EVENT_TYPES.ORDER_CREATED,
        aggregateType: 'order',
        aggregateId: 'order-456',
        payload: expect.objectContaining({
          orderId: 'order-456',
          customerEmail: 'alice@example.com',
          totalAmount: '59.98',
          items: expect.any(Array),
          createdAt: '2026-08-19T10:00:00.000Z',
        }),
      },
      undefined
    );
  });

  it('should accept transaction client', async () => {
    vi.mocked(outboxRepository.create).mockResolvedValue({} as any);

    const mockTx = { outboxEvent: { create: vi.fn() } } as any;

    await outboxService.createOrderCreatedEvent(
      {
        id: 'order-789',
        customerEmail: 'bob@example.com',
        totalAmount: '10.00',
        status: 'PENDING',
        createdAt: new Date(),
        updatedAt: new Date(),
        items: [],
      },
      mockTx
    );

    expect(outboxRepository.create).toHaveBeenCalledWith(
      expect.any(Object),
      mockTx
    );
  });
});
