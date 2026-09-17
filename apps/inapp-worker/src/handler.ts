import { prisma, Prisma } from '@notifyx/database';
import { logger } from '@notifyx/logger';
import {
  type NotificationDeliveryRequestedEvent,
  type ConsumerMessageContext,
} from '@notifyx/kafka';
import { Channel, EVENT_TYPES } from '@notifyx/shared';
import {
  deliveriesAttemptedCounter,
  deliveriesSucceededCounter,
  deliveriesFailedCounter,
  processingLatencyHistogram,
  normalizeErrorCode,
} from '@notifyx/metrics';

export interface InAppDeliveryResult {
  status: 'PROCESSED' | 'SKIPPED' | 'ALREADY_DELIVERED' | 'INVALID';
  inAppNotificationId?: string;
  reason?: string;
}

/**
 * Validates the event envelope against contract requirements.
 */
export function validateInAppEvent(
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
 * Core In-App notification event handler.
 * Executed for each Kafka message consumed by the notifyx-inapp-workers group.
 *
 * IDEMPOTENCY & AT-LEAST-ONCE INVARIANTS:
 * - Checks delivery status before processing.
 * - Database unique constraint on InAppNotification(deliveryId) prevents race-condition duplicates.
 * - Entire persistence (InAppNotification + NotificationDelivery DELIVERED) is atomic in a PostgreSQL transaction.
 * - Throws on transient errors so Kafka offset is NOT committed.
 */
export async function handleInAppDelivery(
  context: ConsumerMessageContext<NotificationDeliveryRequestedEvent>
): Promise<InAppDeliveryResult> {
  const event = context.parsedPayload;

  // Correlation resolution: event.correlationId ?? Kafka correlation-id header ?? "unknown"
  const correlationId =
    (typeof (event as any)?.correlationId === 'string' && (event as any).correlationId) ||
    (context.headers && typeof context.headers['correlation-id'] === 'string' && context.headers['correlation-id']) ||
    'unknown';

  if (correlationId === 'unknown') {
    logger.warn(
      {
        deliveryId: event?.deliveryId,
        eventId: event?.eventId,
        channel: Channel.IN_APP,
        service: 'inapp-worker',
      },
      'Missing correlationId on delivery event. Assigned fallback "unknown".'
    );
  }

  // 1. Validate event structure
  const validation = validateInAppEvent(event);
  if (!validation.valid) {
    deliveriesFailedCounter.inc({
      service: 'inapp-worker',
      channel: Channel.IN_APP,
      provider: 'inapp-database',
      error_code: 'INVALID_PAYLOAD',
    });
    logger.warn(
      {
        error: validation.error,
        topic: context.topic,
        partition: context.partition,
        offset: context.offset,
        correlationId,
      },
      'Received malformed Kafka event in In-App consumer. Skipping message.'
    );
    return { status: 'INVALID', reason: validation.error };
  }

  // 2. Channel Routing (Filter: only process IN_APP)
  if (event.channel !== Channel.IN_APP) {
    logger.debug(
      {
        channel: event.channel,
        deliveryId: event.deliveryId,
        notificationId: event.notificationId,
        correlationId,
      },
      'Skipping non-IN_APP event for In-App consumer group'
    );
    return { status: 'SKIPPED', reason: `Not an IN_APP channel: ${event.channel}` };
  }

  deliveriesAttemptedCounter.inc({
    service: 'inapp-worker',
    channel: Channel.IN_APP,
  });
  const startTime = Date.now();

  logger.info(
    {
      eventId: event.eventId,
      deliveryId: event.deliveryId,
      notificationId: event.notificationId,
      tenantId: event.tenantId,
      userId: event.userId,
      partition: context.partition,
      offset: context.offset,
      correlationId,
    },
    'Processing In-App notification delivery'
  );

  // 3. Verify delivery record in database
  const delivery = await prisma.notificationDelivery.findUnique({
    where: { id: event.deliveryId },
    include: { notification: true },
  });

  if (!delivery) {
    logger.error(
      { deliveryId: event.deliveryId, notificationId: event.notificationId },
      'NotificationDelivery record not found in PostgreSQL. Cannot deliver in-app notification.'
    );
    return { status: 'INVALID', reason: 'Delivery record not found' };
  }

  // 4. Tenant & User Security Validation
  if (delivery.notification.tenantId !== event.tenantId) {
    logger.error(
      {
        eventTenantId: event.tenantId,
        dbTenantId: delivery.notification.tenantId,
        deliveryId: event.deliveryId,
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
      },
      'User ID mismatch detected between event and notification record.'
    );
    return { status: 'INVALID', reason: 'User mismatch' };
  }

  // 5. Idempotency Check (Fast Path)
  if (delivery.status === 'DELIVERED') {
    logger.info(
      { deliveryId: event.deliveryId, notificationId: event.notificationId },
      'NotificationDelivery is already marked DELIVERED (idempotent replay).'
    );
    return { status: 'ALREADY_DELIVERED', reason: 'Already delivered' };
  }

  // Check if InAppNotification already exists for this deliveryId
  const existingInApp = await prisma.inAppNotification.findUnique({
    where: { deliveryId: event.deliveryId },
  });
  if (existingInApp) {
    logger.info(
      { deliveryId: event.deliveryId, inAppId: existingInApp.id },
      'InAppNotification already exists for this deliveryId (idempotent replay).'
    );
    return {
      status: 'ALREADY_DELIVERED',
      inAppNotificationId: existingInApp.id,
      reason: 'InAppNotification already exists',
    };
  }

  // 6. Content Extraction from payload
  const rawPayload = (event.payload || {}) as Record<string, unknown>;
  const title =
    (rawPayload.title as string) ||
    (rawPayload.subject as string) ||
    (rawPayload.header as string) ||
    'Notification';
  const body =
    (rawPayload.body as string) ||
    (rawPayload.message as string) ||
    (rawPayload.text as string) ||
    '';

  // 7. Atomic Database Transaction: InAppNotification + Delivery State Update
  try {
    const result = await prisma.$transaction(async (tx) => {
      // Create durable in-app notification record
      const inApp = await tx.inAppNotification.create({
        data: {
          tenantId: event.tenantId,
          userId: event.userId,
          notificationId: event.notificationId,
          deliveryId: event.deliveryId,
          title,
          body,
          data: rawPayload as any,
        },
      });

      // Update delivery record to DELIVERED
      await tx.notificationDelivery.update({
        where: { id: event.deliveryId },
        data: {
          status: 'DELIVERED',
          attemptCount: (event as any).attempt ?? 1,
          deliveredAt: new Date(),
        },
      });

      // Update parent notification status if all deliveries are finished
      const nonDeliveredCount = await tx.notificationDelivery.count({
        where: {
          notificationId: event.notificationId,
          status: { not: 'DELIVERED' },
        },
      });

      if (nonDeliveredCount === 0) {
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

      return inApp;
    });

    deliveriesSucceededCounter.inc({
      service: 'inapp-worker',
      channel: Channel.IN_APP,
      provider: 'inapp-database',
    });
    processingLatencyHistogram.observe(
      {
        service: 'inapp-worker',
        channel: Channel.IN_APP,
      },
      (Date.now() - startTime) / 1000
    );

    logger.info(
      {
        inAppId: result.id,
        deliveryId: event.deliveryId,
        notificationId: event.notificationId,
        userId: event.userId,
        correlationId,
      },
      'In-App notification successfully persisted and delivery marked DELIVERED'
    );

    return { status: 'PROCESSED', inAppNotificationId: result.id };
  } catch (error: any) {
    // Check if error was a unique constraint collision on deliveryId (concurrent race)
    if (
      error instanceof Prisma.PrismaClientKnownRequestError &&
      error.code === 'P2002'
    ) {
      logger.info(
        { deliveryId: event.deliveryId, correlationId },
        'Concurrent delivery duplicate prevented by database unique constraint.'
      );
      return {
        status: 'ALREADY_DELIVERED',
        reason: 'Concurrent insertion caught by unique constraint',
      };
    }

    deliveriesFailedCounter.inc({
      service: 'inapp-worker',
      channel: Channel.IN_APP,
      provider: 'inapp-database',
      error_code: normalizeErrorCode(error?.message),
    });

    logger.error(
      { error, deliveryId: event.deliveryId, notificationId: event.notificationId, correlationId },
      'Failed atomic transaction for In-App delivery. Preserving at-least-once retry.'
    );
    throw error;
  }
}
