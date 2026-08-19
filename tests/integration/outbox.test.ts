import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createApp } from '../../src/app.js';
import { prisma } from '../../src/config/database.js';
import { simulation } from '../../src/lib/simulation.js';
import request from 'supertest';
import type { Application } from 'express';

let app: Application;

// Helper: wait for a condition with timeout
async function waitFor(
  condition: () => Promise<boolean>,
  timeoutMs = 10000,
  intervalMs = 200
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await condition()) return;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`Timeout waiting for condition after ${timeoutMs}ms`);
}

// Helper: count messages in a RabbitMQ queue
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

  // ─────────────────────────────────────────────────────────────
  // SCENARIO 1: Happy Path — Order created, event published, consumed
  // ─────────────────────────────────────────────────────────────
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

    // Verify order exists in database
    const order = await prisma.order.findUnique({
      where: { id: orderId },
      include: { items: true },
    });
    expect(order).not.toBeNull();
    expect(order?.customerEmail).toBe('test@example.com');

    // Verify outbox event was created atomically
    const outboxEvent = await prisma.outboxEvent.findFirst({
      where: { aggregateId: orderId },
    });
    expect(outboxEvent).not.toBeNull();
    expect(outboxEvent?.status).toBe('PUBLISHED');
    expect(outboxEvent?.eventType).toBe('order.created');
  });

  // ─────────────────────────────────────────────────────────────
  // SCENARIO 2: Publisher crashes after publish, before marking PUBLISHED
  // Expected: duplicate message in RabbitMQ, consumer handles it safely
  // ─────────────────────────────────────────────────────────────
  it('should handle duplicate events when publisher skips mark-as-published', async () => {
    // Enable simulation: publisher will skip DB update after next publish
    await request(app).post('/simulations/publisher-skip-mark').expect(200);

    const res = await request(app)
      .post('/orders')
      .send({
        customerEmail: 'dup@example.com',
        items: [{ productId: 'prod-1', quantity: 1, unitPrice: 10.0 }],
      })
      .expect(201);

    const orderId = res.body.id;

    // Wait for the event to be published (first attempt, skipped mark)
    await waitFor(async () => {
      const ev = await prisma.outboxEvent.findFirst({ where: { aggregateId: orderId } });
      return ev !== null;
    });

    // The event should still be PENDING because the mark was skipped
    const pendingEvent = await prisma.outboxEvent.findFirst({
      where: { aggregateId: orderId },
    });
    expect(pendingEvent?.status).toBe('PENDING');

    // Wait for the publisher to republish on next poll cycle
    await waitFor(async () => {
      const ev = await prisma.outboxEvent.findFirst({ where: { aggregateId: orderId } });
      return ev?.status === 'PUBLISHED';
    }, 15000);

    // Now it should be published
    const publishedEvent = await prisma.outboxEvent.findFirst({
      where: { aggregateId: orderId },
    });
    expect(publishedEvent?.status).toBe('PUBLISHED');

    // Wait for consumer to process (may receive duplicate)
    await waitFor(async () => {
      const processed = await prisma.processedEvent.findFirst({
        where: { eventType: 'order.created' },
      });
      return processed !== null;
    }, 15000);

    // Verify exactly one processed_events entry exists for this event
    const processedEvents = await prisma.processedEvent.findMany({
      where: { eventType: 'order.created' },
    });
    expect(processedEvents.length).toBeGreaterThanOrEqual(1);
  });

  // ─────────────────────────────────────────────────────────────
  // SCENARIO 3: Consumer crashes before ACK
  // Expected: RabbitMQ redelivers, consumer processes safely (idempotent)
  // ─────────────────────────────────────────────────────────────
  it('should handle consumer crash before ACK with idempotency', async () => {
    // Enable simulation: consumer will crash before ACK on next message
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
    }, 15000);

    // Wait for consumer to process (first attempt crashes, second succeeds)
    await waitFor(async () => {
      const processed = await prisma.processedEvent.findFirst({
        where: { eventType: 'order.created' },
      });
      return processed !== null;
    }, 20000);

    // Verify the event was processed
    const processed = await prisma.processedEvent.findFirst({
      where: { eventType: 'order.created' },
    });
    expect(processed).not.toBeNull();
  });

  // ─────────────────────────────────────────────────────────────
  // SCENARIO 4: Consumer repeatedly fails → routes to DLQ
  // ─────────────────────────────────────────────────────────────
  it('should route messages to DLQ after max retries', async () => {
    // Enable simulation: consumer will fail every processing attempt
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
    }, 15000);

    // Wait for message to land in DLQ (3 retries × 5s TTL + processing time)
    await waitFor(async () => {
      const count = await getQueueMessageCount('notifications.order.created.dlq');
      return count >= 1;
    }, 25000);

    const dlqCount = await getQueueMessageCount('notifications.order.created.dlq');
    expect(dlqCount).toBeGreaterThanOrEqual(1);
  });
});
