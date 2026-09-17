import { randomUUID } from 'node:crypto';
import { prisma, type Notification, type NotificationDelivery } from '@notifyx/database';
import {
  NotificationStatus,
  ScheduledStatus,
  OutboxStatus,
  EVENT_TYPES,
  type Channel,
  type NotificationCategory,
  type NotificationDeliveryRequestedEvent,
  validateTemplateVariables,
  renderTemplate,
} from '@notifyx/shared';
import {
  templateRenderFailuresCounter,
  scheduledNotificationsCreatedCounter,
  scheduledNotificationsCancelledCounter,
  notificationIngestionDurationHistogram,
} from '@notifyx/metrics';
import { notificationQuotaLimiter } from '@notifyx/rate-limit';
import { tenantRateLimitService } from '../tenant-rate-limits/tenant-rate-limit.service.js';
import {
  idempotencyService,
  computeRequestHash,
} from '../../services/idempotency.service.js';
import { templateService } from '../templates/template.service.js';
import { preferenceService } from '../preferences/preference.service.js';
import type {
  CreateNotificationInput,
  ListNotificationsQuery,
} from './notification.schema.js';

export interface CreateNotificationResult {
  id: string;
  status: string;
  correlationId?: string;
  scheduledAt?: string;
  idempotencyReplay?: boolean;
  idempotencyHit?: boolean;
  deliveriesCount?: number;
  deliveries?: any[];
}

export interface NotificationWithDeliveries extends Notification {
  deliveries: NotificationDelivery[];
}

export interface PaginatedNotifications {
  data: NotificationWithDeliveries[];
  pagination: {
    page: number;
    limit: number;
    total: number;
    totalPages: number;
  };
}

export class NotificationService {
  async createNotification(
    tenantId: string,
    input: CreateNotificationInput,
    idempotencyKeyHeader?: string,
    correlationId?: string
  ): Promise<CreateNotificationResult> {
    const effectiveCorrelationId = correlationId || `corr_${randomUUID().replace(/-/g, '')}`;
    const ingestionStartTime = Date.now();

    // 1. Request-Level Idempotency check (Strictly header-only)
    let requestHash: string | null = null;
    if (idempotencyKeyHeader) {
      requestHash = computeRequestHash('/v1/notifications', input);
      const idempotencyHit = await idempotencyService.checkIdempotency(
        tenantId,
        idempotencyKeyHeader,
        requestHash
      );

      if (idempotencyHit) {
        return {
          ...(idempotencyHit.payload as any),
          idempotencyReplay: true,
          idempotencyHit: true,
        };
      }
    }

    // 2. Verify recipient user belongs to authenticated tenant
    const user = await prisma.user.findFirst({
      where: {
        tenantId,
        OR: [{ id: input.userId }, { externalId: input.userId }],
      },
    });

    if (!user) {
      const error = new Error(`User "${input.userId}" does not exist in this tenant`);
      (error as any).statusCode = 404;
      throw error;
    }

    // 3. Template Resolution & Deterministic Rendering (BEFORE transaction)
    let resolvedChannels: Channel[] = input.channels || [];
    let finalPayload: Record<string, unknown> = (input.payload || {}) as Record<string, unknown>;
    let resolvedTemplateVersionId: string | null = null;

    if (input.templateId) {
      const activeVersion = await templateService.resolveActiveTemplate(
        tenantId,
        input.templateId
      );

      if (activeVersion) {
        resolvedTemplateVersionId = activeVersion.id;

        // If channels were not explicitly provided, use the template version's channel
        if (!resolvedChannels || resolvedChannels.length === 0) {
          resolvedChannels = [activeVersion.channel as Channel];
        }

        // Variable validation & interpolation
        const templateData = {
          ...finalPayload,
          ...(input.templateData || {}),
          user: {
            id: user.id,
            externalId: user.externalId,
            email: user.email,
            phone: user.phone,
            ...(finalPayload.user as any || {}),
            ...(input.templateData?.user as any || {}),
          },
        };

        const requiredVars = (activeVersion.variables || []) as string[];
        const validation = validateTemplateVariables(requiredVars, templateData);

        if (!validation.valid) {
          templateRenderFailuresCounter.inc({
            channel: activeVersion.channel,
            reason: 'MISSING_VARIABLES',
          });
          const error = new Error(
            `Template rendering validation failed: Missing required variables: [${validation.missing.join(', ')}]`
          );
          (error as any).statusCode = 400;
          throw error;
        }

        try {
          const renderedBody = renderTemplate(activeVersion.body, templateData);
          let renderedSubject = activeVersion.subject;
          if (renderedSubject) {
            renderedSubject = renderTemplate(renderedSubject, templateData);
          }

          finalPayload = {
            ...templateData,
            body: renderedBody,
            ...(renderedSubject ? { subject: renderedSubject } : {}),
          };
        } catch (renderErr: any) {
          templateRenderFailuresCounter.inc({
            channel: activeVersion.channel,
            reason: 'RENDER_EXCEPTION',
          });
          const error = new Error(`Template rendering error: ${renderErr.message}`);
          (error as any).statusCode = 400;
          throw error;
        }
      } else if (input.templateData) {
        // If templateData was supplied, caller expected a managed template
        const error = new Error(`Template "${input.templateId}" not found for this tenant`);
        (error as any).statusCode = 404;
        throw error;
      }
      // If activeVersion not found and no templateData, preserve templateId as unmanaged string for legacy backwards compatibility
    }

    if (resolvedChannels.length === 0) {
      const error = new Error('At least one notification channel must be specified');
      (error as any).statusCode = 400;
      throw error;
    }

    // 4. User Notification Preference Evaluation (Filter channels before transaction)
    const { allowed: allowedChannels } = await preferenceService.filterChannels(
      tenantId,
      user.id,
      input.category as NotificationCategory,
      resolvedChannels
    );

    // 5. Tenant Notification Ingestion Quota Evaluation (BEFORE database transaction)
    // Invariant: Quota rejection happens BEFORE any database record is created.
    // Idempotency replays were already returned at step 1 and never consume quota.
    // Scheduled notifications consume quota once upon acceptance here, not at scheduler dispatch.
    const tenantRateLimits = await tenantRateLimitService.getTenantRateLimits(tenantId);
    const quotaResult = await notificationQuotaLimiter.consume(
      tenantId,
      tenantRateLimits,
      tenantRateLimits.isCustom ? 'custom' : 'default'
    );

    if (!quotaResult.allowed) {
      const error = new Error(quotaResult.reason || 'Notification quota exceeded');
      (error as any).statusCode = 429;
      (error as any).retryAfterMs = quotaResult.retryAfterMs;
      (error as any).errorCode = 'RATE_LIMITED';
      throw error;
    }

    // 6. Check if notification is scheduled for future delivery
    const scheduleTimeStr = input.scheduleAt || input.scheduledFor;
    const isScheduled = Boolean(scheduleTimeStr);

    // 7. Execute atomic transaction to persist Notification, IdempotencyRecord, and Outbox/Scheduled state
    try {
      const result = await prisma.$transaction(async (tx) => {
        if (isScheduled) {
          // Scheduled Notification Path:
          // Persist Notification with status SCHEDULED.
          // Create ScheduledNotification row.
          // Invariant: Exactly one NotifyX Notification record and exactly one ScheduledNotification for the same tenant + idempotency key.
          const scheduledExecuteAt = new Date(scheduleTimeStr!);

          const notification = await tx.notification.create({
            data: {
              tenantId,
              userId: user.id,
              templateId: input.templateId,
              templateVersionId: resolvedTemplateVersionId,
              category: input.category as any,
              status: NotificationStatus.SCHEDULED,
              priority: input.priority,
              channels: allowedChannels as any,
              payload: finalPayload as any,
              idempotencyKey: idempotencyKeyHeader || null,
              correlationId: effectiveCorrelationId,
              scheduledAt: scheduledExecuteAt,
            },
          });

          await tx.scheduledNotification.create({
            data: {
              tenantId,
              notificationId: notification.id,
              executeAt: scheduledExecuteAt,
              status: ScheduledStatus.SCHEDULED,
            },
          });

          const responseData: CreateNotificationResult = {
            id: notification.id,
            status: NotificationStatus.SCHEDULED,
            correlationId: effectiveCorrelationId,
            scheduledAt: scheduleTimeStr,
            deliveriesCount: 0,
            deliveries: [],
          };

          if (idempotencyKeyHeader && requestHash) {
            await tx.idempotencyRecord.create({
              data: {
                tenantId,
                key: idempotencyKeyHeader,
                requestHash,
                notificationId: notification.id,
                statusCode: 202,
                responsePayload: responseData as any,
                expiresAt: new Date(Date.now() + 86400000), // 24 hours TTL
              },
            });
          }

          return responseData;
        }

        // Immediate Notification Path:
        if (allowedChannels.length === 0) {
          // All requested channels disabled by user preferences
          const notification = await tx.notification.create({
            data: {
              tenantId,
              userId: user.id,
              templateId: input.templateId,
              templateVersionId: resolvedTemplateVersionId,
              category: input.category as any,
              status: NotificationStatus.CANCELLED,
              priority: input.priority,
              channels: [] as any,
              payload: finalPayload as any,
              idempotencyKey: idempotencyKeyHeader || null,
              correlationId: effectiveCorrelationId,
            },
          });

          const responseData: CreateNotificationResult = {
            id: notification.id,
            status: NotificationStatus.CANCELLED,
            correlationId: effectiveCorrelationId,
            deliveriesCount: 0,
            deliveries: [],
          };

          if (idempotencyKeyHeader && requestHash) {
            await tx.idempotencyRecord.create({
              data: {
                tenantId,
                key: idempotencyKeyHeader,
                requestHash,
                notificationId: notification.id,
                statusCode: 202,
                responsePayload: responseData as any,
                expiresAt: new Date(Date.now() + 86400000),
              },
            });
          }

          return responseData;
        }

        // Normal Immediate Path with active deliveries
        const notification = await tx.notification.create({
          data: {
            tenantId,
            userId: user.id,
            templateId: input.templateId,
            templateVersionId: resolvedTemplateVersionId,
            category: input.category as any,
            status: NotificationStatus.PENDING,
            priority: input.priority,
            channels: allowedChannels as any,
            payload: finalPayload as any,
            idempotencyKey: idempotencyKeyHeader || null,
            correlationId: effectiveCorrelationId,
          },
        });

        const deliveryRecords = await tx.notificationDelivery.createManyAndReturn({
          data: allowedChannels.map((channel: Channel) => ({
            notificationId: notification.id,
            channel: channel as any,
            status: NotificationStatus.PENDING,
            attemptCount: 0,
          })),
        });

        // Transactional Outbox creation
        const now = new Date();
        const outboxEventsData = deliveryRecords.map((delivery) => {
          const eventId = `evt_${randomUUID().replace(/-/g, '')}`;
          const eventPayload: NotificationDeliveryRequestedEvent = {
            eventId,
            correlationId: effectiveCorrelationId,
            eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
            version: 1,
            occurredAt: now.toISOString(),
            tenantId,
            notificationId: notification.id,
            deliveryId: delivery.id,
            userId: user.id,
            channel: delivery.channel as any,
            priority: notification.priority as any,
            payload: finalPayload,
          };

          return {
            id: eventId,
            tenantId,
            aggregateType: 'NOTIFICATION',
            aggregateId: notification.id,
            deliveryId: delivery.id,
            eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
            payload: eventPayload as any,
            status: OutboxStatus.PENDING,
            attempts: 0,
            availableAt: now,
          };
        });

        await tx.outboxEvent.createMany({
          data: outboxEventsData,
        });

        const responseData: CreateNotificationResult = {
          id: notification.id,
          status: NotificationStatus.PENDING,
          correlationId: effectiveCorrelationId,
          deliveriesCount: deliveryRecords.length,
          deliveries: deliveryRecords,
        };

        if (idempotencyKeyHeader && requestHash) {
          await tx.idempotencyRecord.create({
            data: {
              tenantId,
              key: idempotencyKeyHeader,
              requestHash,
              notificationId: notification.id,
              statusCode: 202,
              responsePayload: responseData as any,
              expiresAt: new Date(Date.now() + 86400000),
            },
          });
        }

        return responseData;
      });

      if (isScheduled) {
        scheduledNotificationsCreatedCounter.inc();
      }

      notificationIngestionDurationHistogram.observe((Date.now() - ingestionStartTime) / 1000);
      return result;
    } catch (err: any) {
      // Concurrency protection: If another concurrent request inserted IdempotencyRecord with same key
      if (err.code === 'P2002' && idempotencyKeyHeader && requestHash) {
        const winningRecord = await prisma.idempotencyRecord.findUnique({
          where: {
            tenantId_key: {
              tenantId,
              key: idempotencyKeyHeader,
            },
          },
        });

        if (winningRecord) {
          if (winningRecord.requestHash === requestHash) {
            notificationIngestionDurationHistogram.observe((Date.now() - ingestionStartTime) / 1000);
            return {
              ...(winningRecord.responsePayload as any),
              idempotencyReplay: true,
            };
          } else {
            const conflictErr = new Error(
              `Idempotency-Key "${idempotencyKeyHeader}" was previously used with a different request payload`
            );
            (conflictErr as any).statusCode = 409;
            throw conflictErr;
          }
        }
      }

      throw err;
    }
  }

  /**
   * Cancels a scheduled notification before it is dispatched.
   * Concurrency-safe: Only transitions from SCHEDULED -> CANCELLED.
   */
  async cancelNotification(tenantId: string, notificationId: string) {
    return prisma.$transaction(async (tx) => {
      // Lock and verify scheduled notification record
      const scheduled = await tx.scheduledNotification.findFirst({
        where: {
          tenantId,
          notificationId,
        },
      });

      if (!scheduled) {
        const error = new Error(`Scheduled notification "${notificationId}" not found for this tenant`);
        (error as any).statusCode = 404;
        throw error;
      }

      if (scheduled.status === ScheduledStatus.CANCELLED) {
        const error = new Error(
          `Cannot cancel scheduled notification "${notificationId}": already CANCELLED`
        );
        (error as any).statusCode = 409;
        throw error;
      }

      if (scheduled.status === ScheduledStatus.DISPATCHED) {
        const error = new Error(
          `Cannot cancel scheduled notification "${notificationId}": already DISPATCHED`
        );
        (error as any).statusCode = 409;
        throw error;
      }

      // Transition to CANCELLED
      await tx.scheduledNotification.update({
        where: { id: scheduled.id },
        data: {
          status: ScheduledStatus.CANCELLED,
          cancelledAt: new Date(),
        },
      });

      await tx.notification.update({
        where: { id: notificationId },
        data: {
          status: NotificationStatus.CANCELLED,
        },
      });

      scheduledNotificationsCancelledCounter.inc();

      return {
        notificationId,
        status: 'CANCELLED',
        cancelledAt: new Date().toISOString(),
      };
    });
  }

  async getNotification(tenantId: string, id: string) {
    const notification = await prisma.notification.findFirst({
      where: { id, tenantId },
      include: {
        deliveries: {
          orderBy: { createdAt: 'asc' },
          select: {
            id: true,
            channel: true,
            status: true,
            attemptCount: true,
            providerMessageId: true,
            error: true,
            lastErrorCode: true,
            lastAttemptAt: true,
            nextAttemptAt: true,
            deliveredAt: true,
            createdAt: true,
            updatedAt: true,
          },
        },
        scheduledNotification: {
          select: {
            id: true,
            executeAt: true,
            status: true,
            dispatchedAt: true,
            cancelledAt: true,
          },
        },
      },
    });

    if (!notification) {
      const error = new Error(`Notification "${id}" not found`);
      (error as any).statusCode = 404;
      throw error;
    }

    return {
      id: notification.id,
      tenantId: notification.tenantId,
      userId: notification.userId,
      templateId: notification.templateId,
      templateVersionId: notification.templateVersionId,
      category: notification.category,
      status: notification.status,
      priority: notification.priority,
      channels: notification.channels,
      payload: notification.payload,
      correlationId: notification.correlationId,
      scheduledAt: notification.scheduledAt,
      createdAt: notification.createdAt,
      updatedAt: notification.updatedAt,
      scheduled: notification.scheduledNotification,
      deliveries: notification.deliveries.map((d) => ({
        deliveryId: d.id,
        channel: d.channel,
        status: d.status,
        attemptCount: d.attemptCount,
        providerMessageId: d.providerMessageId,
        lastErrorCode: d.lastErrorCode,
        error: d.error,
        lastAttemptAt: d.lastAttemptAt,
        nextAttemptAt: d.nextAttemptAt,
        deliveredAt: d.deliveredAt,
      })),
    };
  }

  async listNotifications(
    tenantId: string,
    query: ListNotificationsQuery
  ): Promise<PaginatedNotifications> {
    const page = query.page;
    const limit = query.limit;
    const skip = (page - 1) * limit;

    const where: any = { tenantId };

    if (query.userId) {
      where.userId = query.userId;
    }

    if (query.status) {
      where.status = query.status;
    }

    if (query.category) {
      where.category = query.category;
    }

    if (query.channel) {
      where.channels = {
        has: query.channel,
      };
    }

    if (query.startDate || query.endDate) {
      where.createdAt = {};
      if (query.startDate) where.createdAt.gte = new Date(query.startDate);
      if (query.endDate) where.createdAt.lte = new Date(query.endDate);
    }

    const [total, data] = await Promise.all([
      prisma.notification.count({ where }),
      prisma.notification.findMany({
        where,
        skip,
        take: limit,
        orderBy: { createdAt: 'desc' },
        include: {
          deliveries: true,
        },
      }),
    ]);

    return {
      data,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
      },
    };
  }
}

export const notificationService = new NotificationService();
