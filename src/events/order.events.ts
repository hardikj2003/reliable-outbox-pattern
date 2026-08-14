export interface OrderCreatedPayload {
  orderId: string;
  customerEmail: string;
  totalAmount: string;
  status: string;
  items: Array<{
    productId: string;
    quantity: number;
    unitPrice: string;
  }>;
  createdAt: string;
}

export type OutboxEventType = 'order.created';

export const OUTBOX_EVENT_TYPES = {
  ORDER_CREATED: 'order.created' as const,
};
