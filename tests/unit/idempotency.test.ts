import { describe, it, expect, vi, beforeEach } from 'vitest';
import { withIdempotency } from '../../src/lib/idempotency.js';
import { processedEventRepository } from '../../src/repositories/processedEvent.repository.js';
import { prisma } from '../../src/config/database.js';

vi.mock('../../src/repositories/processedEvent.repository.js', () => ({
  processedEventRepository: {
    findByEventId: vi.fn(),
    create: vi.fn(),
  },
}));

vi.mock('../../src/config/database.js', () => ({
  prisma: {
    $transaction: vi.fn((fn) => fn({})),
  },
}));

describe('withIdempotency', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should execute handler for new events', async () => {
    vi.mocked(processedEventRepository.findByEventId).mockResolvedValue(null);
    vi.mocked(processedEventRepository.create).mockResolvedValue({
      id: 'proc-1',
      eventId: 'event-123',
      eventType: 'order.created',
      processedAt: new Date(),
    } as any);

    const handler = vi.fn().mockResolvedValue('result');

    const { isDuplicate, result } = await withIdempotency(
      'event-123',
      'order.created',
      handler
    );

    expect(isDuplicate).toBe(false);
    expect(result).toBe('result');
    expect(handler).toHaveBeenCalledTimes(1);
    expect(processedEventRepository.create).toHaveBeenCalledWith(
      { eventId: 'event-123', eventType: 'order.created' },
      expect.anything()
    );
  });

  it('should skip handler for duplicate events', async () => {
    vi.mocked(processedEventRepository.findByEventId).mockResolvedValue({
      id: 'proc-1',
      eventId: 'event-123',
      eventType: 'order.created',
      processedAt: new Date(),
    } as any);

    const handler = vi.fn();

    const { isDuplicate } = await withIdempotency(
      'event-123',
      'order.created',
      handler
    );

    expect(isDuplicate).toBe(true);
    expect(handler).not.toHaveBeenCalled();
  });

  it('should handle race condition inside transaction', async () => {
    // First check outside tx returns null (event not processed yet)
    vi.mocked(processedEventRepository.findByEventId)
      .mockResolvedValueOnce(null)   // Outside tx
      .mockResolvedValueOnce({       // Inside tx (another consumer won)
        id: 'proc-1',
        eventId: 'event-123',
        eventType: 'order.created',
        processedAt: new Date(),
      } as any);

    const handler = vi.fn();

    const { isDuplicate } = await withIdempotency(
      'event-123',
      'order.created',
      handler
    );

    expect(isDuplicate).toBe(true);
    expect(handler).not.toHaveBeenCalled();
  });

  it('should treat unique constraint violation as duplicate', async () => {
    vi.mocked(processedEventRepository.findByEventId).mockResolvedValue(null);
    vi.mocked(processedEventRepository.create).mockRejectedValue(
      new Error('P2002: Unique constraint failed on processed_events')
    );

    const handler = vi.fn().mockResolvedValue('result');

    const { isDuplicate } = await withIdempotency(
      'event-123',
      'order.created',
      handler
    );

    expect(isDuplicate).toBe(true);
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('should re-throw non-unique errors', async () => {
    vi.mocked(processedEventRepository.findByEventId).mockResolvedValue(null);
    vi.mocked(processedEventRepository.create).mockRejectedValue(
      new Error('Database connection lost')
    );

    const handler = vi.fn().mockResolvedValue('result');

    await expect(
      withIdempotency('event-123', 'order.created', handler)
    ).rejects.toThrow('Database connection lost');
  });
});
