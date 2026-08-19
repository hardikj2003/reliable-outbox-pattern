import { Decimal } from "decimal.js";
import { prisma } from "../config/database.js";
import { orderRepository } from "../repositories/order.repository.js";
import { outboxService } from "./outbox.service.js";
import { logger } from "../config/logger.js";
import { metrics } from "../lib/metrics.js";
import { getCurrentContext } from "../lib/asyncContext.js";
import type { CreateOrderInput, OrderOutput } from "../types/index.js";

function calculateTotal(items: CreateOrderInput["items"]): Decimal {
  return items.reduce(
    (sum, item) => sum.plus(new Decimal(item.unitPrice).times(item.quantity)),
    new Decimal(0),
  );
}

function toOrderOutput(
  order: Awaited<ReturnType<typeof orderRepository.create>>,
): OrderOutput {
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
      const error = new Error("Order must contain at least one item");
      (error as Error & { statusCode: number }).statusCode = 400;
      throw error;
    }

    const totalAmount = calculateTotal(input.items);
    const context = getCurrentContext();
    const reqId = context?.requestId as string | undefined;

    const timer = metrics.timer("order.create.duration", { status: "success" });

    logger.info(
      {
        customerEmail: input.customerEmail,
        itemCount: input.items.length,
        requestId: reqId,
      },
      "Creating order",
    );

    try {
      const order = await prisma.$transaction(async (tx) => {
        const createdOrder = await orderRepository.create(
          {
            customerEmail: input.customerEmail,
            totalAmount,
            status: "PENDING",
            items: input.items.map((item) => ({
              productId: item.productId,
              quantity: item.quantity,
              unitPrice: new Decimal(item.unitPrice),
            })),
          },
          tx,
        );

        const orderOutput = toOrderOutput(createdOrder);

        await outboxService.createOrderCreatedEvent(orderOutput, tx);

        return createdOrder;
      });

      timer.stop();
      metrics.increment("order.create.count", { status: "success" });

      logger.info(
        {
          orderId: order.id,
          requestId: reqId,
          durationMs:
            metrics.getSummary()['order.create.duration:{"status":"success"}']
              ?.avg,
        },
        "Order created with outbox event",
      );

      return toOrderOutput(order);
    } catch (err) {
      metrics.increment("order.create.count", { status: "error" });
      throw err;
    }
  },

  async getOrderById(id: string): Promise<OrderOutput | null> {
    const timer = metrics.timer("order.get.duration");
    const order = await orderRepository.findById(id);
    timer.stop();

    if (!order) {
      return null;
    }

    return toOrderOutput(order);
  },
};
