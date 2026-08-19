import { afterAll, afterEach, beforeAll } from "vitest";
import { prisma } from "../src/config/database.js";
import {
  startOutboxPublisher,
  stopOutboxPublisher,
} from "../src/workers/outbox.publisher.js";
import {
  startOrderCreatedConsumer,
  stopOrderCreatedConsumer,
} from "../src/consumers/orderCreated.consumer.js";
import { closeRabbitMQ } from "../src/config/rabbitmq.js";

export async function cleanupDatabase(): Promise<void> {
  await prisma.processedEvent.deleteMany();
  await prisma.outboxEvent.deleteMany();
  await prisma.orderItem.deleteMany();
  await prisma.order.deleteMany();
}

beforeAll(async () => {
  await cleanupDatabase();

  startOutboxPublisher().catch((err) => {
    console.error("Publisher failed to start in tests:", err);
  });

  startOrderCreatedConsumer().catch((err) => {
    console.error("Consumer failed to start in tests:", err);
  });

  // Wait for workers to fully connect to RabbitMQ and start polling
  await new Promise((r) => setTimeout(r, 5000));
}, 30000);

afterEach(async () => {
  await cleanupDatabase();
});

afterAll(async () => {
  stopOutboxPublisher();
  await stopOrderCreatedConsumer();
  await new Promise((r) => setTimeout(r, 1500));
  await closeRabbitMQ();
  await prisma.$disconnect();
}, 30000);
