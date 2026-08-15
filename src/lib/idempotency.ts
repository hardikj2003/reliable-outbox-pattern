import { processedEventRepository } from '../repositories/processedEvent.repository.js';
import { logger } from '../config/logger.js';
import { prisma } from '../config/database.js';

/**
 * Ensures an event is processed exactly once by checking the processed_events table.
 *
 * Flow:
 * 1. Check if eventId already exists in processed_events.
 * 2. If yes → return { isDuplicate: true }. The caller should ACK the message.
 * 3. If no → execute the handler, then insert the eventId into processed_events.
 * 4. If insert fails due to unique constraint (race condition) → treat as duplicate.
 *
 * This handles two sources of duplicates:
 * - Publisher crash after publish but before marking PUBLISHED (republish on restart)
 * - Consumer crash before ACK (RabbitMQ redelivers to another consumer)
 */
export async function withIdempotency<T>(
  eventId: string,
  eventType: string,
  handler: () => Promise<T>
): Promise<{ isDuplicate: boolean; result?: T }> {
  //
  // STEP 1: Fast check outside transaction.
  //
  // Most events are not duplicates. This avoids starting a transaction
  // for the common case, which is more efficient.
  //
  const existing = await processedEventRepository.findByEventId(eventId);

  if (existing) {
    logger.info({ eventId, eventType }, 'Duplicate event detected, skipping processing');
    return { isDuplicate: true };
  }

  //
  // STEP 2: Execute handler and record processing in one transaction.
  //
  // We wrap the handler and the processed_events insert in a transaction
  // so that if the handler succeeds but the insert fails, both are rolled back.
  // This prevents the case where the side effect happened but we didn't record it.
  //
  try {
    const result = await prisma.$transaction(async (tx) => {
      // Double-check inside transaction (prevents race condition between
      // the fast check above and this transaction starting)
      const raceCheck = await processedEventRepository.findByEventId(eventId, tx);
      if (raceCheck) {
        return { isDuplicate: true };
      }

      // Execute the business logic
      const handlerResult = await handler();

      // Record that we processed this event
      await processedEventRepository.create(
        { eventId, eventType },
        tx
      );

      return { isDuplicate: false, result: handlerResult };
    });

    if (result.isDuplicate) {
      logger.info({ eventId, eventType }, 'Duplicate event detected inside transaction, skipping');
      return { isDuplicate: true };
    }

    return { isDuplicate: false, result: result.result };
  } catch (err) {
    //
    // STEP 3: Handle unique constraint violation.
    //
    // If two consumers process the same event simultaneously, one will
    // succeed and insert into processed_events. The other will get a
    // unique constraint violation on the insert. We catch this and
    // treat it as a duplicate — safe to ACK.
    //
    if (isUniqueConstraintError(err)) {
      logger.info({ eventId, eventType, err }, 'Unique constraint violation on processed_events, treating as duplicate');
      return { isDuplicate: true };
    }

    // Re-throw other errors (handler failed, DB down, etc.)
    throw err;
  }
}

function isUniqueConstraintError(err: unknown): boolean {
  if (err instanceof Error) {
    // Prisma unique constraint violation: P2002
    return err.message.includes('P2002');
  }
  return false;
}
