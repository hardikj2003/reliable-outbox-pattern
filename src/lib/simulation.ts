import { logger } from '../config/logger.js';

/**
 * Failure Simulation Controls
 *
 * This module allows controlled injection of failure scenarios
 * for testing and portfolio demonstration. It is a NO-OP in production.
 *
 * Scenarios:
 * 1. Publisher skips DB update after publish (simulates crash after publish)
 * 2. Consumer crashes before ACK (simulates consumer crash mid-processing)
 * 3. Consumer fails repeatedly (simulates persistent consumer failure → DLQ)
 */

const isSimEnabled = process.env.NODE_ENV !== 'production';

interface SimulationState {
  publisherSkipMarkAfterPublish: boolean;
  consumerCrashBeforeAck: boolean;
  consumerFailRepeatedly: boolean;
}

const state: SimulationState = {
  publisherSkipMarkAfterPublish: false,
  consumerCrashBeforeAck: false,
  consumerFailRepeatedly: false,
};

export const simulation = {
  enablePublisherSkipMark(): void {
    if (!isSimEnabled) return;
    state.publisherSkipMarkAfterPublish = true;
    logger.warn('[SIMULATION] Publisher will skip mark-as-published after next publish');
  },

  enableConsumerCrashBeforeAck(): void {
    if (!isSimEnabled) return;
    state.consumerCrashBeforeAck = true;
    logger.warn('[SIMULATION] Consumer will crash before ACK on next message');
  },

  enableConsumerFailRepeatedly(): void {
    if (!isSimEnabled) return;
    state.consumerFailRepeatedly = true;
    logger.warn('[SIMULATION] Consumer will fail repeatedly (will route to DLQ)');
  },

  reset(): void {
    if (!isSimEnabled) return;
    state.publisherSkipMarkAfterPublish = false;
    state.consumerCrashBeforeAck = false;
    state.consumerFailRepeatedly = false;
    logger.info('[SIMULATION] All failure scenarios reset');
  },

  shouldSkipMarkAfterPublish(): boolean {
    if (!isSimEnabled || !state.publisherSkipMarkAfterPublish) return false;
    // One-shot: reset after triggering so it only affects one event
    state.publisherSkipMarkAfterPublish = false;
    return true;
  },

  shouldCrashBeforeAck(): boolean {
    if (!isSimEnabled || !state.consumerCrashBeforeAck) return false;
    // One-shot
    state.consumerCrashBeforeAck = false;
    return true;
  },

  shouldFailRepeatedly(): boolean {
    if (!isSimEnabled) return false;
    return state.consumerFailRepeatedly;
  },

  getState(): SimulationState {
    return { ...state };
  },
};
