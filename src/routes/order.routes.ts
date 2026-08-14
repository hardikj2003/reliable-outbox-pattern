import { Router } from 'express';
import { createOrder, getOrderById, orderSchemas } from '../controllers/order.controller.js';
import { requestValidator } from '../middleware/requestValidator.js';

const router = Router();

router.post('/', requestValidator(orderSchemas.create), createOrder);
router.get('/:id', getOrderById);

export default router;
