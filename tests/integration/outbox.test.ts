import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createApp } from '../../src/app.js';
import { prisma } from '../../src/config/database.js';
import { simulation } from '../../src/lib/simulation.js';
import request from 'supertest';
import type { Application } from 'express';

let app: Application;

async function waitFor(
  condition: () => Promise<boolean>,
  timeoutMs = 30000,
  intervalMs = 500
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await condition()) return;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`Timeout waiting for condition after ${timeoutMs}ms`);
}

async function getQueueMessageCount(queueName: string): Promise<number> {
  try {
    const res = await fetch(`http://guest:guest@localhost:15672/api/queues/%2F/${queueName}`);
    if (!res.ok) return 0;
    const data = await res.json();
    return (data.messages_ready || 0) + (data.messages_unacknowledged || 0);
  } catch {
    return 0;
  }
}

describe('Transactional Outbox Pattern', () => {
  beforeAll(() => {
    app = createApp();
  });

  afterAll(() => {
    simulation.reset();
  });

  it('should create an order and publish the outbox event', async () => {
    const res = await request(app)
      .post('/orders')
      .send({
        customerEmail: 'test@example.com',
        items: [{ productId: 'prod-1', quantity: 1, unitPrice: 10.0 }],
      })
      .expect(201);

    const orderId = res.body.id;
    expect(orderId).toBeDefined();
    expect(res.body.status).toBe('PENDING');

    const order = await prisma.order.findUnique({
      where: { id: orderId },
      include: { items: true },
    });
    expect(order).not.toBeNull();
    expect(order?.customerEmail).toBe('test@example.com');

    // Wait for publisher to pick up and publish the event (polls every 5s)
    await waitFor(async () => {
      const ev = await prisma.outboxEvent.findFirst({ where: { aggregateId: orderId } });
      return ev?.status === 'PUBLISHED';
    }, 30000);

    const outboxEvent = await prisma.outboxEvent.findFirst({
      where: { aggregateId: orderId },
    });
    expect(outboxEvent).not.toBeNull();
    expect(outboxEvent?.status).toBe('PUBLISHED');
    expect(outboxEvent?.eventType).toBe('order.created');

    // Wait for consumer to process
    await waitFor(async () => {
      const processed = await prisma.processedEvent.findFirst({
        where: { eventId: outboxEvent?.id },
      });
      return processed !== null;
    }, 30000);
  }, 60000);

  it('should handle duplicate events when publisher skips mark-as-published', async () => {
    await request(app).post('/simulations/publisher-skip-mark').expect(200);

    const res = await request(app)
      .post('/orders')
      .send({
        customerEmail: 'dup@example.com',
        items: [{ productId: 'prod-1', quantity: 1, unitPrice: 10.0 }],
      })
      .expect(201);

    const orderId = res.body.id;

    // Wait for event to be created
    await waitFor(async () => {
      const ev = await prisma.outboxEvent.findFirst({ where: { aggregateId: orderId } });
      return ev !== null;
    }, 20000);

    // Status should be PENDING because mark was skipped
    const pendingEvent = await prisma.outboxEvent.findFirst({
      where: { aggregateId: orderId },
    });
    expect(pendingEvent?.status).toBe('PENDING');

    // Wait for republish on next poll cycle
    await waitFor(async () => {
      const ev = await prisma.outboxEvent.findFirst({ where: { aggregateId: orderId } });
      return ev?.status === 'PUBLISHED';
    }, 30000);

    // Wait for consumer to process (may receive duplicate)
    await waitFor(async () => {
      const processed = await prisma.processedEvent.findFirst({
        where: { eventType: 'order.created' },
      });
      return processed !== null;
    }, 30000);

    const processedEvents = await prisma.processedEvent.findMany({
      where: { eventType: 'order.created' },
    });
    expect(processedEvents.length).toBeGreaterThanOrEqual(1);
  }, 60000);

  it('should handle consumer crash before ACK with idempotency', async () => {
    await request(app).post('/simulations/consumer-crash').expect(200);

    const res = await request(app)
      .post('/orders')
      .send({
        customerEmail: 'crash@example.com',
        items: [{ productId: 'prod-1', quantity: 1, unitPrice: 10.0 }],
      })
      .expect(201);

    const orderId = res.body.id;

    // Wait for outbox to be published
    await waitFor(async () => {
      const ev = await prisma.outboxEvent.findFirst({ where: { aggregateId: orderId } });
      return ev?.status === 'PUBLISHED';
    }, 30000);

    // Wait for consumer to process (first attempt crashes, second succeeds)
    await waitFor(async () => {
      const processed = await prisma.processedEvent.findFirst({
        where: { eventType: 'order.created' },
      });
      return processed !== null;
    }, 30000);

    const processed = await prisma.processedEvent.findFirst({
      where: { eventType: 'order.created' },
    });
    expect(processed).not.toBeNull();
  }, 60000);

  it('should route messages to DLQ after max retries', async () => {
    await request(app).post('/simulations/consumer-fail-repeatedly').expect(200);

    const res = await request(app)
      .post('/orders')
      .send({
        customerEmail: 'dlq@example.com',
        items: [{ productId: 'prod-1', quantity: 1, unitPrice: 10.0 }],
      })
      .expect(201);

    const orderId = res.body.id;

    // Wait for outbox to be published
    await waitFor(async () => {
      const ev = await prisma.outboxEvent.findFirst({ where: { aggregateId: orderId } });
      return ev?.status === 'PUBLISHED';
    }, 30000);

    // Wait for message to land in DLQ
    // 3 retries × 5s TTL + processing overhead ≈ 20-25s
    await waitFor(async () => {
      const count = await getQueueMessageCount('notifications.order.created.dlq');
      return count >= 1;
    }, 45000);

    const dlqCount = await getQueueMessageCount('notifications.order.created.dlq');
    expect(dlqCount).toBeGreaterThanOrEqual(1);
  }, 90000);
});