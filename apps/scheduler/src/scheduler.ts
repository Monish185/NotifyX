import { randomUUID } from 'node:crypto';
import { prisma } from '@notifyx/database';
import {
  EVENT_TYPES,
  OutboxStatus,
  type Channel,
  type NotificationDeliveryRequestedEvent,
} from '@notifyx/shared';
import {
  scheduledNotificationsClaimedCounter,
  scheduledNotificationsDispatchedCounter,
  schedulerDispatchDelayHistogram,
} from '@notifyx/metrics';
import { createLogger } from '@notifyx/logger';

const logger = createLogger({ name: 'scheduler' });

export interface SchedulerOptions {
  pollIntervalMs?: number;
  batchSize?: number;
}

export class NotificationScheduler {
  private pollIntervalMs: number;
  private batchSize: number;
  private isRunning: boolean = false;
  private timer: NodeJS.Timeout | null = null;
  private isPolling: boolean = false;

  constructor(options: SchedulerOptions = {}) {
    this.pollIntervalMs = options.pollIntervalMs || Number(process.env.SCHEDULER_POLL_INTERVAL_MS) || 1000;
    this.batchSize = options.batchSize || Number(process.env.SCHEDULER_BATCH_SIZE) || 50;
  }

  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    logger.info(
      { pollIntervalMs: this.pollIntervalMs, batchSize: this.batchSize },
      'Notification Scheduler poller started'
    );
    this.scheduleNextPoll();
  }

  async stop(): Promise<void> {
    this.isRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    // Wait if a poll is currently in-flight
    while (this.isPolling) {
      await new Promise((r) => setTimeout(r, 50));
    }

    logger.info('Notification Scheduler poller stopped cleanly');
  }

  private scheduleNextPoll(): void {
    if (!this.isRunning) return;
    this.timer = setTimeout(async () => {
      try {
        await this.pollOnce();
      } catch (err: any) {
        logger.error({ error: err.message }, 'Unexpected error during scheduler polling cycle');
      } finally {
        this.scheduleNextPoll();
      }
    }, this.pollIntervalMs);
  }

  /**
   * Performs a single atomic polling cycle to claim and dispatch due scheduled notifications.
   * Uses PostgreSQL row locking with `FOR UPDATE SKIP LOCKED` to guarantee concurrency safety
   * across multiple scheduler instances.
   *
   * SEMANTICS OF 'DISPATCHED':
   * "scheduler successfully converted the scheduled job into normal notification delivery work
   * and created outbox events." It does NOT mean provider delivery succeeded.
   */
  async pollOnce(): Promise<number> {
    this.isPolling = true;
    try {
      const dispatchedCount = await prisma.$transaction(async (tx: any) => {
        // 1. Concurrency-safe atomic claim of due items
        const dueJobs = await tx.$queryRaw<Array<{
          id: string;
          tenantId: string;
          notificationId: string;
          executeAt: Date;
        }>>`
          SELECT id, "tenantId", "notificationId", "executeAt"
          FROM scheduled_notifications
          WHERE status = 'SCHEDULED' AND "executeAt" <= NOW()
          ORDER BY "executeAt" ASC
          LIMIT ${this.batchSize}
          FOR UPDATE SKIP LOCKED
        `;

        if (!dueJobs || dueJobs.length === 0) {
          return 0;
        }

        const jobIds = dueJobs.map((j: { id: string }) => j.id);
        const notificationIds = dueJobs.map((j: { notificationId: string }) => j.notificationId);
        const now = new Date();

        // 2. Mark ScheduledNotification records as DISPATCHED
        await tx.scheduledNotification.updateMany({
          where: { id: { in: jobIds } },
          data: {
            status: 'DISPATCHED',
            dispatchedAt: now,
          },
        });

        // 3. Fetch corresponding Notification records
        const notifications: any[] = await tx.notification.findMany({
          where: { id: { in: notificationIds } },
        });

        const notifMap = new Map<string, any>(notifications.map((n: any) => [n.id, n]));

        for (const job of dueJobs) {
          const notif = notifMap.get(job.notificationId);
          if (!notif) {
            logger.warn({ notificationId: job.notificationId }, 'Scheduled notification target not found');
            continue;
          }

          // If the notification was cancelled or already completed, skip delivery creation
          if (notif.status === 'CANCELLED') {
            continue;
          }

          // 4. Transition Notification to PENDING
          await tx.notification.update({
            where: { id: notif.id },
            data: { status: 'PENDING' },
          });

          // 5. Create delivery records for each channel
          const channels = (notif.channels || []) as Channel[];
          if (channels.length === 0) {
            continue;
          }

          const deliveryRecords: any[] = await tx.notificationDelivery.createManyAndReturn({
            data: channels.map((ch: any) => ({
              notificationId: notif.id,
              channel: ch,
              status: 'PENDING',
              attemptCount: 0,
            })),
          });

          // 6. Create Transactional Outbox events
          // CRITICAL: Scheduler does NOT publish directly to Kafka.
          // It creates OutboxEvent records so the Outbox Publisher bridge handles Kafka publishing.
          const outboxEventsData = deliveryRecords.map((delivery: any) => {
            const eventId = `evt_${randomUUID().replace(/-/g, '')}`;
            const correlationId = notif.correlationId || `corr_${randomUUID().replace(/-/g, '')}`;

            const eventPayload: NotificationDeliveryRequestedEvent = {
              eventId,
              correlationId,
              eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
              version: 1,
              occurredAt: now.toISOString(),
              tenantId: notif.tenantId,
              notificationId: notif.id,
              deliveryId: delivery.id,
              userId: notif.userId,
              channel: delivery.channel as any,
              priority: notif.priority as any,
              payload: (notif.payload || {}) as Record<string, unknown>,
            };

            return {
              id: eventId,
              tenantId: notif.tenantId,
              aggregateType: 'NOTIFICATION',
              aggregateId: notif.id,
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

          // 7. Measure scheduling dispatch delay
          const delaySec = Math.max(0, (now.getTime() - new Date(job.executeAt).getTime()) / 1000);
          schedulerDispatchDelayHistogram.observe(delaySec);
          scheduledNotificationsDispatchedCounter.inc();

          logger.info(
            {
              scheduledId: job.id,
              notificationId: notif.id,
              delaySeconds: delaySec.toFixed(3),
              deliveriesCount: deliveryRecords.length,
            },
            'Scheduled job converted to outbox delivery events (DISPATCHED)'
          );
        }

        scheduledNotificationsClaimedCounter.inc(dueJobs.length);
        return dueJobs.length;
      });

      return dispatchedCount;
    } finally {
      this.isPolling = false;
    }
  }

  /**
   * Alias for pollOnce with customizable limit.
   */
  async pollAndProcessDueJobs(limit?: number): Promise<number> {
    if (limit) {
      this.batchSize = limit;
    }
    return this.pollOnce();
  }

  /**
   * Performs connectivity check against PostgreSQL.
   */
  async healthCheck(): Promise<{ healthy: boolean; timestamp: string }> {
    try {
      await prisma.$queryRaw`SELECT 1`;
      return { healthy: true, timestamp: new Date().toISOString() };
    } catch {
      return { healthy: false, timestamp: new Date().toISOString() };
    }
  }
}

export const scheduler = new NotificationScheduler();
export const schedulerService = scheduler;
