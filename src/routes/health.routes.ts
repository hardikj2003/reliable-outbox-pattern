import { Router } from 'express';
import { healthCheck, readinessCheck, metricsEndpoint } from '../controllers/health.controller.js';

const router = Router();

router.get('/', healthCheck);
router.get('/ready', readinessCheck);
router.get('/metrics', metricsEndpoint);

export default router;
