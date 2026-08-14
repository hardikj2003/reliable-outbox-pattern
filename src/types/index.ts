export interface OrderItemInput {
  productId: string;
  quantity: number;
  unitPrice: number;
}

export interface CreateOrderInput {
  customerEmail: string;
  items: OrderItemInput[];
}

export interface OrderItemOutput {
  id: string;
  productId: string;
  quantity: number;
  unitPrice: string;
}

export interface OrderOutput {
  id: string;
  customerEmail: string;
  totalAmount: string;
  status: string;
  createdAt: Date;
  updatedAt: Date;
  items: OrderItemOutput[];
}
