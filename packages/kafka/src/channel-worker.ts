import { prisma, Prisma } from '@notifyx/database';
import { logger } from '@notifyx/logger';
import {
  type ConsumerMessageContext,
} from './consumer.js';
import {
  Channel,
  EVENT_TYPES,
  TOPICS,
  type NotificationDeliveryRequestedEvent,
  type NotificationDeliveryRetryEvent,
  type NotificationDeliveryDlqEvent,
} from '@notifyx/shared';
import { KafkaConsumer } from './consumer.js';
import { calculateRetryDelay } from './backoff.js';
import { Semaphore } from './semaphore.js';
import {
  deliveriesAttemptedCounter,
  deliveriesSucceededCounter,
  deliveriesFailedCounter,
  retriesScheduledCounter,
  dlqEventsCounter,
  processingLatencyHistogram,
  providerLatencyHistogram,
  retryDelayHistogram,
  normalizeErrorCode,
  normalizeReasonCategory,
  getMetrics,
  getContentType,
  workerInFlightGauge,
  workerConcurrencyLimitGauge,
  providerRateLimitCounter,
} from '@notifyx/metrics';
import http from 'node:http';
import crypto from 'node:crypto';

/**
 * Normalized channel delivery request passed to a provider adapter.
 */
export interface ProviderSendRequest<T = Record<string, unknown>> {
  idempotencyKey: string; // deliveryId
  tenantId: string;
  userId: string;
  recipient: string;
  payload: T;
  priority?: string;
  notificationId: string;
  deliveryId: string;
}

/**
 * Standard structured result returned by any channel provider adapter.
 */
export interface ProviderResult {
  success: boolean;
  providerMessageId?: string;
  error?: string;
  retryable?: boolean;
  metadata?: Record<string, unknown>;
}

/**
 * Generic provider adapter interface for notification channels.
 */
export interface ChannelProvider<T = Record<string, unknown>> {
  readonly name: string;
  send(request: ProviderSendRequest<T>): Promise<ProviderResult>;
}

/**
 * Result of worker message processing.
 */
export interface ChannelDeliveryResult {
  status: 'PROCESSED' | 'SKIPPED' | 'ALREADY_DELIVERED' | 'INVALID' | 'FAILED' | 'RETRY_SCHEDULED';
  providerMessageId?: string;
  reason?: string;
}

/**
 * Retry and backoff configuration for BaseChannelWorker.
 */
export interface RetryWorkerConfig {
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  jitterRatio?: number;
}

/**
 * Configuration options for a BaseChannelWorker instance.
 */
export interface BaseChannelWorkerOptions<TPayload = Record<string, unknown>> {
  channel: Channel;
  serviceName: string;
  consumerGroup: string;
  brokers: string[];
  clientId?: string;
  healthPort: number;
  provider: ChannelProvider<TPayload>;
  retry?: RetryWorkerConfig;
  /**
   * Maximum concurrent in-flight provider calls for this worker instance (default: 5).
   */
  concurrency?: number;
  /**
   * Resolves the primary recipient (e.g. email, phone, token) from user record or event payload.
   */
  resolveRecipient: (
    user: { id: string; email?: string | null; phone?: string | null },
    payload: Record<string, unknown>
  ) => { recipient?: string; error?: string };
  /**
   * Validates the channel-specific payload.
   */
  validatePayload: (payload: Record<string, unknown>) => {
    valid: boolean;
    data?: TPayload;
    error?: string;
  };
}

/**
 * Envelope validation for notification delivery requested and retry events.
 */
export function validateDeliveryRequestedEvent(
  event: any
): { valid: boolean; error?: string } {
  if (!event || typeof event !== 'object') {
    return { valid: false, error: 'Event payload is missing or not an object' };
  }
  if (!event.eventId || typeof event.eventId !== 'string') {
    return { valid: false, error: 'Missing or invalid eventId' };
  }
  if (
    event.eventType !== EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED &&
    event.eventType !== EVENT_TYPES.NOTIFICATION_DELIVERY_RETRY
  ) {
    return { valid: false, error: `Invalid eventType: ${event.eventType}` };
  }
  if (event.version !== 1) {
    return { valid: false, error: `Unsupported event version: ${event.version}` };
  }
  if (!event.tenantId || typeof event.tenantId !== 'string') {
    return { valid: false, error: 'Missing or invalid tenantId' };
  }
  if (!event.notificationId || typeof event.notificationId !== 'string') {
    return { valid: false, error: 'Missing or invalid notificationId' };
  }
  if (!event.deliveryId || typeof event.deliveryId !== 'string') {
    return { valid: false, error: 'Missing or invalid deliveryId' };
  }
  if (!event.userId || typeof event.userId !== 'string') {
    return { valid: false, error: 'Missing or invalid userId' };
  }
  if (!event.channel || typeof event.channel !== 'string') {
    return { valid: false, error: 'Missing or invalid channel' };
  }
  return { valid: true };
}

/**
 * Reusable BaseChannelWorker that manages the complete distributed lifecycle:
 * - Kafka consumer connection & partition management (topics: requested & retry)
 * - Event parsing & envelope validation
 * - Channel routing & independent offset commit
 * - Database delivery lookup & multi-tenant security assertion
 * - Durable idempotency pre-check
 * - Attempt invariant tracking (delivery.attemptCount = executed, event.attempt = about to execute)
 * - Channel-specific payload validation
 * - Provider adapter invocation with idempotency key
 * - Atomic database transaction: DELIVERED, RETRY_SCHEDULED + RetryRecord + OutboxEvent, or FAILED + DeadLetterEvent + DLQ OutboxEvent
 * - Parent notification status progression
 * - Explicit Kafka offset commit (autoCommit: false)
 * - Health and readiness HTTP server
 * - Graceful SIGINT/SIGTERM shutdown
 */
export class BaseChannelWorker<TPayload = Record<string, unknown>> {
  private consumer: KafkaConsumer;
  private healthServer: http.Server | null = null;
  private isProcessing = false;
  private semaphore: Semaphore;

  constructor(private readonly options: BaseChannelWorkerOptions<TPayload>) {
    const concurrency = options.concurrency ?? 5;
    this.semaphore = new Semaphore(concurrency);

    this.consumer = new KafkaConsumer({
      brokers: options.brokers,
      groupId: options.consumerGroup,
      clientId: options.clientId || `notifyx-${options.serviceName}`,
      consumerConfig: {
        maxWaitTimeInMs: 100,
      },
    });
  }

  get isReady(): boolean {
    return this.consumer.isConnected();
  }

  get concurrencyLimit(): number {
    return this.semaphore.maxConcurrency;
  }

  get inFlightCount(): number {
    return this.semaphore.activeCount;
  }

  /**
   * Process a single consumed Kafka delivery event (initial or retry).
   */
  async processEvent(
    context: ConsumerMessageContext<NotificationDeliveryRequestedEvent | NotificationDeliveryRetryEvent>
  ): Promise<ChannelDeliveryResult> {
    const event = context.parsedPayload as any;

    // Correlation resolution: event.correlationId ?? Kafka correlation-id header ?? "unknown"
    const correlationId =
      (typeof event?.correlationId === 'string' && event.correlationId) ||
      (context.headers && typeof context.headers['correlation-id'] === 'string' && context.headers['correlation-id']) ||
      'unknown';

    if (correlationId === 'unknown') {
      logger.warn(
        {
          deliveryId: event?.deliveryId,
          eventId: event?.eventId,
          channel: this.options.channel,
          service: this.options.serviceName,
        },
        'Missing correlationId on delivery event. Assigned fallback "unknown".'
      );
    }

    // 1. Validate envelope
    const envelope = validateDeliveryRequestedEvent(event);
    if (!envelope.valid) {
      logger.warn(
        {
          error: envelope.error,
          topic: context.topic,
          partition: context.partition,
          offset: context.offset,
          service: this.options.serviceName,
          correlationId,
        },
        'Received malformed Kafka event. Skipping message.'
      );
      return { status: 'INVALID', reason: envelope.error };
    }

    // 2. Channel Filtering: strictly process designated channel
    if (event.channel !== this.options.channel) {
      logger.debug(
        {
          targetChannel: this.options.channel,
          eventChannel: event.channel,
          deliveryId: event.deliveryId,
          service: this.options.serviceName,
          correlationId,
        },
        `Skipping non-${this.options.channel} event for ${this.options.consumerGroup} group`
      );
      return {
        status: 'SKIPPED',
        reason: `Channel mismatch: expected ${this.options.channel}, received ${event.channel}`,
      };
    }

    // 3. Database delivery lookup
    const delivery = await prisma.notificationDelivery.findUnique({
      where: { id: event.deliveryId },
      include: {
        notification: {
          include: {
            user: true,
          },
        },
      },
    });

    if (!delivery) {
      logger.error(
        {
          deliveryId: event.deliveryId,
          notificationId: event.notificationId,
          service: this.options.serviceName,
          correlationId,
        },
        'NotificationDelivery record not found in PostgreSQL. Cannot process delivery.'
      );
      return { status: 'INVALID', reason: 'Delivery record not found' };
    }

    // 4. Security Assertions: Tenant and User Isolation
    if (delivery.notification.tenantId !== event.tenantId) {
      logger.error(
        {
          eventTenantId: event.tenantId,
          dbTenantId: delivery.notification.tenantId,
          deliveryId: event.deliveryId,
          service: this.options.serviceName,
          correlationId,
        },
        'SECURITY ALERT: Cross-tenant mismatch detected! Event tenantId does not match database record.'
      );
      return { status: 'INVALID', reason: 'Tenant mismatch' };
    }

    if (delivery.notification.userId !== event.userId) {
      logger.error(
        {
          eventUserId: event.userId,
          dbUserId: delivery.notification.userId,
          deliveryId: event.deliveryId,
          service: this.options.serviceName,
          correlationId,
        },
        'User ID mismatch detected between event and notification record.'
      );
      return { status: 'INVALID', reason: 'User mismatch' };
    }

    // Attempt invariant calculation:
    // event.attempt: provider attempt about to execute (initial = 1, retries = 2, 3...)
    // delivery.attemptCount: provider attempts already executed
    const incomingAttempt: number =
      typeof event.attempt === 'number' && event.attempt > 0
        ? event.attempt
        : delivery.attemptCount + 1;

    // 5. Durable Idempotency Pre-Checks
    if (delivery.status === 'DELIVERED') {
      logger.info(
        {
          deliveryId: event.deliveryId,
          notificationId: event.notificationId,
          service: this.options.serviceName,
          correlationId,
        },
        'NotificationDelivery is already marked DELIVERED (idempotent replay).'
      );
      return { status: 'ALREADY_DELIVERED', reason: 'Already delivered' };
    }

    if (delivery.status === 'FAILED') {
      logger.info(
        {
          deliveryId: event.deliveryId,
          notificationId: event.notificationId,
          service: this.options.serviceName,
          correlationId,
        },
        'NotificationDelivery is already in terminal FAILED state (idempotent replay).'
      );
      return { status: 'ALREADY_DELIVERED', reason: 'Already marked FAILED' };
    }

    // If RETRY_SCHEDULED and attemptCount >= incomingAttempt,
    // this specific attempt has already executed and scheduled next retry.
    if (delivery.status === 'RETRY_SCHEDULED' && delivery.attemptCount >= incomingAttempt) {
      logger.info(
        {
          deliveryId: event.deliveryId,
          incomingAttempt,
          currentAttemptCount: delivery.attemptCount,
          service: this.options.serviceName,
          correlationId,
        },
        'NotificationDelivery attempt already executed and scheduled for retry (idempotent replay).'
      );
      return {
        status: 'SKIPPED',
        reason: `Attempt ${incomingAttempt} already executed and scheduled`,
      };
    }

    // 6. Channel Payload & Recipient Validation
    const rawPayload = (event.payload || {}) as Record<string, unknown>;
    const payloadValidation = this.options.validatePayload(rawPayload);
    if (!payloadValidation.valid || !payloadValidation.data) {
      const errorMsg = payloadValidation.error || 'Invalid channel payload';
      logger.warn(
        {
          error: errorMsg,
          deliveryId: event.deliveryId,
          service: this.options.serviceName,
          correlationId,
        },
        `Invalid ${this.options.channel} payload structure. Marking terminal FAILED.`
      );

      deliveriesFailedCounter.inc({
        service: this.options.serviceName,
        channel: this.options.channel,
        provider: this.options.provider.name,
        error_code: 'INVALID_PAYLOAD',
      });
      dlqEventsCounter.inc({
        service: this.options.serviceName,
        channel: this.options.channel,
        reason_category: 'VALIDATION_ERROR',
      });

      await this.handleTerminalFailure({
        event,
        delivery,
        reason: errorMsg,
        errorCode: 'INVALID_PAYLOAD',
        incomingAttempt,
        rawPayload,
        correlationId,
      });

      return { status: 'INVALID', reason: errorMsg };
    }

    const recipientRes = this.options.resolveRecipient(
      delivery.notification.user,
      rawPayload
    );
    if (!recipientRes.recipient) {
      const errorMsg = recipientRes.error || `Recipient destination missing for ${this.options.channel}`;
      logger.warn(
        {
          error: errorMsg,
          deliveryId: event.deliveryId,
          service: this.options.serviceName,
          correlationId,
        },
        `Cannot resolve destination for ${this.options.channel}. Marking terminal FAILED.`
      );

      deliveriesFailedCounter.inc({
        service: this.options.serviceName,
        channel: this.options.channel,
        provider: this.options.provider.name,
        error_code: 'INVALID_RECIPIENT',
      });
      dlqEventsCounter.inc({
        service: this.options.serviceName,
        channel: this.options.channel,
        reason_category: 'RECIPIENT_MISSING',
      });

      await this.handleTerminalFailure({
        event,
        delivery,
        reason: errorMsg,
        errorCode: 'INVALID_RECIPIENT',
        incomingAttempt,
        rawPayload,
        correlationId,
      });

      return { status: 'INVALID', reason: errorMsg };
    }

    logger.info(
      {
        eventId: event.eventId,
        deliveryId: event.deliveryId,
        notificationId: event.notificationId,
        tenantId: event.tenantId,
        userId: event.userId,
        channel: event.channel,
        incomingAttempt,
        partition: context.partition,
        offset: context.offset,
        service: this.options.serviceName,
        correlationId,
      },
      `Executing ${this.options.channel} notification delivery attempt ${incomingAttempt}`
    );

    // Track attempt metric and start timing
    deliveriesAttemptedCounter.inc({
      service: this.options.serviceName,
      channel: this.options.channel,
    });
    const processingStartTime = Date.now();

    // 7. Invoke Channel Provider Adapter with Idempotency Key
    let providerResult: ProviderResult;
    const providerStartTime = Date.now();
    try {
      providerResult = await this.options.provider.send({
        idempotencyKey: event.deliveryId,
        tenantId: event.tenantId,
        userId: event.userId,
        recipient: recipientRes.recipient,
        payload: payloadValidation.data,
        priority: event.priority,
        notificationId: event.notificationId,
        deliveryId: event.deliveryId,
      });
    } catch (err: any) {
      logger.error(
        {
          error: err.message,
          deliveryId: event.deliveryId,
          provider: this.options.provider.name,
          service: this.options.serviceName,
          correlationId,
        },
        `Provider ${this.options.provider.name} threw unhandled error.`
      );
      // Re-throw so Kafka offset is NOT committed (preserves at-least-once redelivery)
      throw err;
    } finally {
      providerLatencyHistogram.observe(
        {
          service: this.options.serviceName,
          provider: this.options.provider.name,
        },
        (Date.now() - providerStartTime) / 1000
      );
    }

    // 8. Handle Provider SUCCESS
    if (providerResult.success) {
      await prisma.$transaction(async (tx: Prisma.TransactionClient) => {
        await tx.notificationDelivery.update({
          where: { id: event.deliveryId },
          data: {
            status: 'DELIVERED',
            attemptCount: incomingAttempt,
            lastAttemptAt: new Date(),
            nextAttemptAt: null,
            providerMessageId: providerResult.providerMessageId || `mock_${event.deliveryId}`,
            deliveredAt: new Date(),
            error: null,
            lastErrorCode: null,
          },
        });

        // Update parent notification state
        const remainingDeliveries = await tx.notificationDelivery.count({
          where: {
            notificationId: event.notificationId,
            status: { not: 'DELIVERED' },
          },
        });

        if (remainingDeliveries === 0) {
          await tx.notification.update({
            where: { id: event.notificationId },
            data: { status: 'DELIVERED' },
          });
        } else {
          await tx.notification.update({
            where: { id: event.notificationId },
            data: { status: 'PROCESSING' },
          });
        }
      });

      deliveriesSucceededCounter.inc({
        service: this.options.serviceName,
        channel: this.options.channel,
        provider: this.options.provider.name,
      });
      processingLatencyHistogram.observe(
        {
          service: this.options.serviceName,
          channel: this.options.channel,
        },
        (Date.now() - processingStartTime) / 1000
      );

      logger.info(
        {
          deliveryId: event.deliveryId,
          notificationId: event.notificationId,
          providerMessageId: providerResult.providerMessageId,
          attempt: incomingAttempt,
          channel: this.options.channel,
          service: this.options.serviceName,
          correlationId,
        },
        `${this.options.channel} delivery successfully persisted and marked DELIVERED`
      );

      return {
        status: 'PROCESSED',
        providerMessageId: providerResult.providerMessageId,
      };
    }

    // 9. Handle Provider FAILURE: Retry vs Terminal DLQ
    // Phase 11: Classify downstream provider rate limits
    const isRateLimit =
      providerResult.metadata?.errorCode === 'RATE_LIMIT' ||
      (typeof providerResult.error === 'string' &&
        (providerResult.error.toUpperCase().includes('RATE_LIMIT') ||
          providerResult.error.toUpperCase().includes('429')));

    if (isRateLimit) {
      providerRateLimitCounter.inc({
        service: this.options.serviceName,
        channel: this.options.channel,
      });
      logger.warn(
        {
          service: this.options.serviceName,
          channel: this.options.channel,
          deliveryId: event.deliveryId,
          correlationId,
        },
        'Downstream provider rate limit (HTTP 429) encountered. Routing through retry engine.'
      );
    }

    const maxAttempts = this.options.retry?.maxAttempts ?? 5;
    const baseDelayMs = this.options.retry?.baseDelayMs ?? 5000;
    const maxDelayMs = this.options.retry?.maxDelayMs ?? 900000;
    const jitterRatio = this.options.retry?.jitterRatio ?? 0.5;

    const isRetryable = providerResult.retryable !== false;
    const hasRetriesRemaining = incomingAttempt < maxAttempts;

    if (isRetryable && hasRetriesRemaining) {
      // SCHEDULE RETRY
      const delayMs = calculateRetryDelay({
        attempt: incomingAttempt,
        baseDelayMs,
        maxDelayMs,
        jitterRatio,
      });
      const nextAttemptAt = new Date(Date.now() + delayMs);
      const nextAttempt = incomingAttempt + 1;

      logger.warn(
        {
          deliveryId: event.deliveryId,
          provider: this.options.provider.name,
          error: providerResult.error,
          incomingAttempt,
          nextAttempt,
          maxAttempts,
          delayMs,
          nextAttemptAt: nextAttemptAt.toISOString(),
          service: this.options.serviceName,
          correlationId,
        },
        `Scheduling retry for ${this.options.channel} delivery`
      );

      try {
        await prisma.$transaction(async (tx: Prisma.TransactionClient) => {
          // A. Update delivery to RETRY_SCHEDULED with attemptCount = incomingAttempt
          await tx.notificationDelivery.update({
            where: { id: event.deliveryId },
            data: {
              status: 'RETRY_SCHEDULED',
              attemptCount: incomingAttempt,
              lastAttemptAt: new Date(),
              nextAttemptAt,
              error: providerResult.error || 'Provider rejected request',
              lastErrorCode: (providerResult.metadata?.errorCode as string) || null,
            },
          });

          // B. Insert RetryRecord (idempotent on deliveryId_attempt)
          const existingRetry = await tx.retryRecord.findUnique({
            where: {
              deliveryId_attempt: {
                deliveryId: event.deliveryId,
                attempt: incomingAttempt,
              },
            },
          });

          if (!existingRetry) {
            await tx.retryRecord.create({
              data: {
                notificationId: event.notificationId,
                deliveryId: event.deliveryId,
                attempt: incomingAttempt,
                error: providerResult.error || 'Provider rejected request',
                nextRetryAt: nextAttemptAt,
              },
            });

            // C. Insert OutboxEvent for retry publication via existing Outbox Publisher
            const retryEventPayload: NotificationDeliveryRetryEvent = {
              eventId: crypto.randomUUID(),
              correlationId,
              eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_RETRY,
              version: 1,
              occurredAt: new Date().toISOString(),
              tenantId: event.tenantId,
              notificationId: event.notificationId,
              deliveryId: event.deliveryId,
              userId: event.userId,
              channel: this.options.channel,
              priority: event.priority,
              payload: rawPayload,
              attempt: nextAttempt,
              maxAttempts,
              scheduledAt: new Date().toISOString(),
              nextAttemptAt: nextAttemptAt.toISOString(),
              originalEventId: event.eventId,
            };

            await tx.outboxEvent.create({
              data: {
                tenantId: event.tenantId,
                aggregateType: 'notification_delivery',
                aggregateId: event.deliveryId,
                deliveryId: event.deliveryId,
                eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_RETRY,
                payload: retryEventPayload as any,
                status: 'PENDING',
                availableAt: nextAttemptAt,
              },
            });
          }

          // D. Parent notification status progression
          await tx.notification.update({
            where: { id: event.notificationId },
            data: { status: 'PROCESSING' },
          });
        });

        retriesScheduledCounter.inc({
          service: this.options.serviceName,
          channel: this.options.channel,
        });
        retryDelayHistogram.observe(
          {
            channel: this.options.channel,
          },
          delayMs / 1000
        );
        processingLatencyHistogram.observe(
          {
            service: this.options.serviceName,
            channel: this.options.channel,
          },
          (Date.now() - processingStartTime) / 1000
        );
      } catch (err: any) {
        if (err instanceof Prisma.PrismaClientKnownRequestError && err.code === 'P2002') {
          logger.info(
            { deliveryId: event.deliveryId, incomingAttempt, correlationId },
            'RetryRecord unique constraint collision (P2002). Idempotent replay handled.'
          );
        } else {
          logger.error(
            { err: err.message, deliveryId: event.deliveryId, correlationId },
            'Transaction failed while scheduling retry. Preserving at-least-once delivery.'
          );
          throw err;
        }
      }

      return {
        status: 'RETRY_SCHEDULED',
        reason: providerResult.error || 'Retry scheduled',
      };
    } else {
      // TERMINAL FAILURE -> DLQ
      const reason = !isRetryable
        ? providerResult.error || 'Non-retryable provider error'
        : `Exhausted maximum retry attempts (${incomingAttempt}/${maxAttempts}): ${providerResult.error || 'Unknown error'}`;

      const rawErrorCode =
        (providerResult.metadata?.errorCode as string) ||
        (isRetryable ? 'RETRIES_EXHAUSTED' : 'PERMANENT_FAILURE');
      const normalizedErr = normalizeErrorCode(rawErrorCode);
      const normalizedReason = normalizeReasonCategory(reason);

      deliveriesFailedCounter.inc({
        service: this.options.serviceName,
        channel: this.options.channel,
        provider: this.options.provider.name,
        error_code: normalizedErr,
      });
      dlqEventsCounter.inc({
        service: this.options.serviceName,
        channel: this.options.channel,
        reason_category: normalizedReason,
      });

      logger.error(
        {
          deliveryId: event.deliveryId,
          incomingAttempt,
          maxAttempts,
          isRetryable,
          reason,
          service: this.options.serviceName,
          correlationId,
        },
        `Terminal failure for ${this.options.channel} delivery. Routing to Dead-Letter Queue.`
      );

      await this.handleTerminalFailure({
        event,
        delivery,
        reason,
        errorCode: rawErrorCode,
        incomingAttempt,
        rawPayload,
        correlationId,
      });

      processingLatencyHistogram.observe(
        {
          service: this.options.serviceName,
          channel: this.options.channel,
        },
        (Date.now() - processingStartTime) / 1000
      );

      return {
        status: 'FAILED',
        reason,
      };
    }
  }

  /**
   * Records terminal delivery failure, updates delivery state to FAILED,
   * records DeadLetterEvent, and writes Dead-Letter OutboxEvent for Kafka publication.
   */
  private async handleTerminalFailure(params: {
    event: any;
    delivery: any;
    reason: string;
    errorCode?: string;
    incomingAttempt: number;
    rawPayload: Record<string, unknown>;
    correlationId: string;
  }): Promise<void> {
    const { event, reason, errorCode, incomingAttempt, rawPayload, correlationId } = params;

    try {
      await prisma.$transaction(async (tx: Prisma.TransactionClient) => {
        // 1. Update delivery to FAILED
        await tx.notificationDelivery.update({
          where: { id: event.deliveryId },
          data: {
            status: 'FAILED',
            attemptCount: incomingAttempt,
            lastAttemptAt: new Date(),
            nextAttemptAt: null,
            error: reason,
            lastErrorCode: errorCode || null,
          },
        });

        // 2. Record terminal attempt in RetryRecord if this was an executed attempt
        if (incomingAttempt > 0) {
          const existingRetry = await tx.retryRecord.findUnique({
            where: {
              deliveryId_attempt: {
                deliveryId: event.deliveryId,
                attempt: incomingAttempt,
              },
            },
          });
          if (!existingRetry) {
            await tx.retryRecord.create({
              data: {
                notificationId: event.notificationId,
                deliveryId: event.deliveryId,
                attempt: incomingAttempt,
                error: reason,
                nextRetryAt: null,
              },
            });
          }
        }

        // 3. Upsert / Dedup DeadLetterEvent and create DLQ OutboxEvent
        const existingDlq = await tx.deadLetterEvent.findUnique({
          where: { deliveryId: event.deliveryId },
        });

        if (!existingDlq) {
          const dlqEventId = crypto.randomUUID();
          await tx.deadLetterEvent.create({
            data: {
              id: crypto.randomUUID(),
              eventId: dlqEventId,
              tenantId: event.tenantId,
              notificationId: event.notificationId,
              deliveryId: event.deliveryId,
              channel: this.options.channel,
              userId: event.userId,
              attemptCount: incomingAttempt,
              reason,
              errorCode: errorCode || null,
              payload: rawPayload as any,
            },
          });

          const dlqEventPayload: NotificationDeliveryDlqEvent = {
            eventId: dlqEventId,
            correlationId,
            eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_DLQ,
            version: 1,
            occurredAt: new Date().toISOString(),
            tenantId: event.tenantId,
            notificationId: event.notificationId,
            deliveryId: event.deliveryId,
            userId: event.userId,
            channel: this.options.channel,
            priority: event.priority,
            payload: rawPayload,
            attemptCount: incomingAttempt,
            reason,
            errorCode,
            failedAt: new Date().toISOString(),
          };

          await tx.outboxEvent.create({
            data: {
              tenantId: event.tenantId,
              aggregateType: 'notification_delivery',
              aggregateId: event.deliveryId,
              deliveryId: event.deliveryId,
              eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_DLQ,
              payload: dlqEventPayload as any,
              status: 'PENDING',
              availableAt: new Date(),
            },
          });
        }

        // 4. Update parent notification status if all deliveries are terminal
        const pendingOrRetryDeliveries = await tx.notificationDelivery.count({
          where: {
            notificationId: event.notificationId,
            status: { in: ['PENDING', 'RETRY_SCHEDULED'] },
          },
        });

        if (pendingOrRetryDeliveries === 0) {
          const deliveredCount = await tx.notificationDelivery.count({
            where: {
              notificationId: event.notificationId,
              status: 'DELIVERED',
            },
          });

          await tx.notification.update({
            where: { id: event.notificationId },
            data: { status: deliveredCount > 0 ? 'DELIVERED' : 'FAILED' },
          });
        }
      });
    } catch (err: any) {
      if (err instanceof Prisma.PrismaClientKnownRequestError && err.code === 'P2002') {
        logger.info(
          { deliveryId: event.deliveryId },
          'Unique constraint collision during terminal failure handling. Handled idempotently.'
        );
      } else {
        logger.error(
          { err: err.message, deliveryId: event.deliveryId },
          'Failed transaction during terminal failure handling.'
        );
        throw err;
      }
    }
  }

  /**
   * Starts the Kafka consumer and the HTTP health server.
   */
  async start(): Promise<void> {
    logger.info(
      {
        service: this.options.serviceName,
        channel: this.options.channel,
        consumerGroup: this.options.consumerGroup,
      },
      `Starting ${this.options.serviceName}`
    );

    // 1. Start HTTP Health & Readiness Server
    this.startHealthServer();

    // 2. Initialize worker concurrency metric
    workerConcurrencyLimitGauge.set(
      { service: this.options.serviceName, channel: this.options.channel },
      this.concurrencyLimit
    );

    // 3. Connect and subscribe consumer to requested AND retry topics
    await this.consumer.connect();
    await this.consumer.subscribe([
      TOPICS.NOTIFICATION_DELIVERY_REQUESTED,
      TOPICS.NOTIFICATION_DELIVERY_RETRY,
    ]);

    // 4. Start Kafka Consumer Loop with Bounded Concurrency Semaphore
    await this.consumer.run<NotificationDeliveryRequestedEvent | NotificationDeliveryRetryEvent>(
      async (context) => {
        const release = await this.semaphore.acquire();
        this.isProcessing = true;
        workerInFlightGauge.inc({
          service: this.options.serviceName,
          channel: this.options.channel,
        });

        try {
          await this.processEvent(context);
        } finally {
          workerInFlightGauge.dec({
            service: this.options.serviceName,
            channel: this.options.channel,
          });
          this.isProcessing = false;
          release();
        }
      }
    );

    logger.info(
      { service: this.options.serviceName },
      `${this.options.serviceName} successfully running and consuming Kafka topics`
    );
  }

  /**
   * Graceful shutdown.
   */
  async stop(): Promise<void> {
    logger.info(
      { service: this.options.serviceName },
      `Stopping ${this.options.serviceName}...`
    );

    // Wait briefly if currently processing a message
    if (this.isProcessing) {
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    await this.consumer.disconnect();

    if (this.healthServer) {
      await new Promise<void>((resolve) => {
        this.healthServer?.close(() => resolve());
      });
    }

    logger.info(
      { service: this.options.serviceName },
      `${this.options.serviceName} gracefully stopped.`
    );
  }

  private startHealthServer(): void {
    const port = this.options.healthPort;
    this.healthServer = http.createServer(async (req, res) => {
      if (req.url === '/metrics') {
        try {
          const metrics = await getMetrics();
          res.writeHead(200, { 'Content-Type': getContentType() });
          res.end(metrics);
        } catch (err: any) {
          res.writeHead(500, { 'Content-Type': 'text/plain' });
          res.end(err?.message || 'Error collecting metrics');
        }
        return;
      }

      if (req.url === '/health' || req.url === '/live') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(
          JSON.stringify({
            status: 'ok',
            service: this.options.serviceName,
            channel: this.options.channel,
            uptime: process.uptime(),
          })
        );
        return;
      }

      if (req.url === '/ready' || req.url === '/readiness') {
        let postgresOk = false;
        try {
          await prisma.$queryRaw`SELECT 1`;
          postgresOk = true;
        } catch {
          postgresOk = false;
        }

        const kafkaOk = this.consumer.isConnected();
        const isReady = postgresOk && kafkaOk;
        const statusCode = isReady ? 200 : 503;

        res.writeHead(statusCode, { 'Content-Type': 'application/json' });
        res.end(
          JSON.stringify({
            status: isReady ? 'ok' : 'degraded',
            service: this.options.serviceName,
            channel: this.options.channel,
            provider: this.options.provider.name,
            consumerGroup: this.options.consumerGroup,
            postgres: postgresOk ? 'healthy' : 'unhealthy',
            kafkaConsumer: kafkaOk ? 'connected' : 'disconnected',
            uptime: process.uptime(),
          })
        );
        return;
      }

      res.writeHead(404);
      res.end();
    });

    this.healthServer.listen(port, () => {
      logger.info(
        { port, service: this.options.serviceName },
        `${this.options.serviceName} health server listening`
      );
    });
  }
}
