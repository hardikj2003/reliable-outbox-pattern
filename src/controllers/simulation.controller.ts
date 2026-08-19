import type { Request, Response } from 'express';
import { simulation } from '../lib/simulation.js';
import { logger } from '../config/logger.js';

export function enablePublisherSkipMark(req: Request, res: Response): void {
  simulation.enablePublisherSkipMark();
  logger.info({ reqId: req.id }, 'Simulation: publisher skip-mark enabled');
  res.json({ status: 'ok', scenario: 'publisher_skip_mark_after_publish' });
}

export function enableConsumerCrash(req: Request, res: Response): void {
  simulation.enableConsumerCrashBeforeAck();
  logger.info({ reqId: req.id }, 'Simulation: consumer crash enabled');
  res.json({ status: 'ok', scenario: 'consumer_crash_before_ack' });
}

export function enableConsumerFailRepeatedly(req: Request, res: Response): void {
  simulation.enableConsumerFailRepeatedly();
  logger.info({ reqId: req.id }, 'Simulation: consumer repeated failure enabled');
  res.json({ status: 'ok', scenario: 'consumer_fail_repeatedly' });
}

export function resetSimulations(req: Request, res: Response): void {
  simulation.reset();
  logger.info({ reqId: req.id }, 'Simulation: all scenarios reset');
  res.json({ status: 'ok', action: 'reset' });
}

export function getSimulationState(_req: Request, res: Response): void {
  res.json({ status: 'ok', state: simulation.getState() });
}
