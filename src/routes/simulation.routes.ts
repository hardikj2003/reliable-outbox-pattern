import { Router } from 'express';
import {
  enablePublisherSkipMark,
  enableConsumerCrash,
  enableConsumerFailRepeatedly,
  resetSimulations,
  getSimulationState,
} from '../controllers/simulation.controller.js';

const router = Router();

router.post('/publisher-skip-mark', enablePublisherSkipMark);
router.post('/consumer-crash', enableConsumerCrash);
router.post('/consumer-fail-repeatedly', enableConsumerFailRepeatedly);
router.post('/reset', resetSimulations);
router.get('/state', getSimulationState);

export default router;
