import { prisma, OutboxStatus } from '@notifyx/database';
import { KafkaProducer } from '@notifyx/kafka';
import { logger } from '@notifyx/logger';
import { TOPICS } from '@notifyx/shared';
import {
  outboxEventsPendingGauge,
  outboxEventsPublishedCounter,
  outboxEventsFailedCounter,
} from '@notifyx/metrics';

export interface OutboxPublisherOptions {
  batchSize?: number;
  pollIntervalMs?: number;
  maxAttempts?: number;
}

export class OutboxPublisher {
  private producer: KafkaProducer;
  private running: boolean = false;
  private timer: NodeJS.Timeout | null = null;
  private isProcessing: boolean = false;
  private readonly batchSize: number;
  private readonly pollIntervalMs: number;
  private readonly maxAttempts: number;

  constructor(producer: KafkaProducer, options?: OutboxPublisherOptions) {
    this.producer = producer;
    this.batchSize = options?.batchSize ?? 20;
    this.pollIntervalMs = options?.pollIntervalMs ?? 1000;
    this.maxAttempts = options?.maxAttempts ?? 10;
  }

  /**
   * Calculates bounded exponential backoff date for publisher retries.
   * Attempt 1: 5s
   * Attempt 2: 15s
   * Attempt 3: 30s
   * Attempt 4+: 60s
   */
  static calculateNextRetry(attempts: number): Date {
    const backoffs = [5, 15, 30, 60]; // in seconds
    const delaySeconds = backoffs[Math.min(attempts, backoffs.length - 1)];
    return new Date(Date.now() + delaySeconds * 1000);
  }

  /**
   * Starts the polling loop.
   */
  async start(): Promise<void> {
    if (this.running) {
      return;
    }

    this.running = true;
    logger.info(
      { batchSize: this.batchSize, pollIntervalMs: this.pollIntervalMs },
      'Starting Outbox Publisher loop'
    );

    // Initial immediate poll
    this.poll();
  }

  /**
   * Graceful stop. Waits for active poll to finish and clears timer.
   */
  async stop(): Promise<void> {
    logger.info('Stopping Outbox Publisher');
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    // Wait if a poll is currently in-flight
    while (this.isProcessing) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    logger.info('Outbox Publisher stopped cleanly');
  }

  isRunning(): boolean {
    return this.running;
  }

  private scheduleNextPoll(delayMs = this.pollIntervalMs): void {
    if (!this.running) {
      return;
    }
    this.timer = setTimeout(() => this.poll(), delayMs);
  }

  /**
   * Main polling routine.
   */
  async poll(): Promise<number> {
    if (this.isProcessing || !this.running) {
      return 0;
    }

    this.isProcessing = true;
    let processedCount = 0;

    try {
      processedCount = await this.processBatch();
    } catch (error) {
      logger.error({ error }, 'Unexpected error in outbox publisher polling loop');
    } finally {
      this.isProcessing = false;
      // If there were events processed up to batchSize, poll again immediately to drain backlog
      const nextDelay = processedCount >= this.batchSize ? 50 : this.pollIntervalMs;
      this.scheduleNextPoll(nextDelay);
    }

    return processedCount;
  }

  /**
   * Safely claims and processes a batch of outbox events.
   *
   * SAFE CONCURRENCY CLAIMING:
   * Uses PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED` inside a short transaction
   * to ensure multiple publisher instances never simultaneously process the same rows.
   *
   * AT-LEAST-ONCE SEMANTICS:
   * If a publisher publishes to Kafka and crashes before updating PostgreSQL,
   * the event remains PENDING in the DB and will be published again on restart.
   * Downstream consumers MUST be idempotent using eventId & deliveryId.
   */
  async processBatch(): Promise<number> {
    // Step 1: Claim pending records using SELECT FOR UPDATE SKIP LOCKED
    const claimedEventIds = await prisma.$transaction(async (tx: any) => {
      const rows = await tx.$queryRaw<Array<{ id: string }>>`
        SELECT id FROM outbox_events
        WHERE status = 'PENDING'
          AND "availableAt" <= NOW()
        ORDER BY "createdAt" ASC
        LIMIT ${this.batchSize}
        FOR UPDATE SKIP LOCKED
      `;
      return rows.map((r: { id: string }) => r.id);
    });

    if (claimedEventIds.length === 0) {
      return 0;
    }

    logger.info(
      { count: claimedEventIds.length },
      'Outbox events claimed for publishing'
    );

    // Step 2: Fetch full models for the claimed IDs
    const events = await prisma.outboxEvent.findMany({
      where: { id: { in: claimedEventIds } },
      orderBy: { createdAt: 'asc' },
    });

    let successCount = 0;

    // Step 3: Publish each event individually so transient failures are isolated
    for (const outbox of events) {
      const payload = outbox.payload as Record<string, unknown>;
      const targetTopic = outbox.eventType || TOPICS.NOTIFICATION_DELIVERY_REQUESTED;

      try {
        logger.info(
          {
            eventId: outbox.id,
            deliveryId: outbox.deliveryId,
            notificationId: outbox.aggregateId,
            tenantId: outbox.tenantId,
            eventType: outbox.eventType,
            topic: targetTopic,
          },
          'Kafka publish started'
        );

        // Publish to corresponding Kafka topic (requested, retry, or dlq)
        await this.producer.publish(
          targetTopic,
          payload,
          outbox.deliveryId
        );

        // Mark as PUBLISHED in PostgreSQL
        await prisma.outboxEvent.update({
          where: { id: outbox.id },
          data: {
            status: OutboxStatus.PUBLISHED,
            publishedAt: new Date(),
            lastError: null,
          },
        });

        // Update parent notification status to QUEUED if this was an initial delivery request and all outbox events are published
        if (outbox.eventType === TOPICS.NOTIFICATION_DELIVERY_REQUESTED) {
          await this.updateNotificationStatusIfAllPublished(outbox.aggregateId);
        }

        logger.info(
          {
            eventId: outbox.id,
            deliveryId: outbox.deliveryId,
            notificationId: outbox.aggregateId,
          },
          'Kafka publish succeeded'
        );

        outboxEventsPublishedCounter.inc({ event_type: outbox.eventType || 'unknown' });
        successCount++;
      } catch (publishError: any) {
        outboxEventsFailedCounter.inc({ event_type: outbox.eventType || 'unknown' });
        const nextAttempts = outbox.attempts + 1;
        const errorMessage = publishError?.message || String(publishError);
        const isFailedPermanently = nextAttempts >= this.maxAttempts;
        const nextAvailableAt = OutboxPublisher.calculateNextRetry(nextAttempts);

        logger.error(
          {
            error: publishError,
            eventId: outbox.id,
            deliveryId: outbox.deliveryId,
            tenantId: outbox.tenantId,
            attempts: nextAttempts,
            nextAvailableAt: nextAvailableAt.toISOString(),
            status: isFailedPermanently ? OutboxStatus.FAILED : OutboxStatus.PENDING,
          },
          isFailedPermanently
            ? 'Outbox event permanently failed max retry attempts'
            : 'Kafka publish failed; outbox event retry scheduled'
        );

        await prisma.outboxEvent.update({
          where: { id: outbox.id },
          data: {
            status: isFailedPermanently ? OutboxStatus.FAILED : OutboxStatus.PENDING,
            attempts: nextAttempts,
            lastError: errorMessage,
            availableAt: nextAvailableAt,
          },
        });
      }
    }

    // Update pending backlog metric
    try {
      const pendingCount = await prisma.outboxEvent.count({
        where: { status: OutboxStatus.PENDING },
      });
      outboxEventsPendingGauge.set({ event_type: 'all' }, pendingCount);
    } catch {
      // Non-critical metric collection failure
    }

    return successCount;
  }

  /**
   * If all outbox events for this notification are PUBLISHED, mark Notification as QUEUED.
   * Semantic: QUEUED means the event is durably placed on Kafka waiting for workers.
   */
  private async updateNotificationStatusIfAllPublished(notificationId: string): Promise<void> {
    try {
      const remainingUnpublished = await prisma.outboxEvent.count({
        where: {
          aggregateId: notificationId,
          status: { not: OutboxStatus.PUBLISHED },
        },
      });

      if (remainingUnpublished === 0) {
        await prisma.notification.updateMany({
          where: {
            id: notificationId,
            status: 'PENDING',
          },
          data: {
            status: 'QUEUED',
          },
        });
      }
    } catch (err) {
      logger.warn(
        { error: err, notificationId },
        'Could not update Notification status to QUEUED'
      );
    }
  }
}
