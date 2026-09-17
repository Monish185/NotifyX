import { prisma } from '@notifyx/database';
import http from 'node:http';

export interface DatabaseStateMetrics {
  totalNotifications: number;
  totalDeliveries: number;
  deliveriesByStatus: {
    PENDING: number;
    DELIVERED: number;
    RETRY_SCHEDULED: number;
    FAILED: number;
  };
  outboxByStatus: {
    PENDING: number;
    PUBLISHED: number;
    FAILED: number;
  };
  totalRetryRecords: number;
  totalDeadLetterEvents: number;
  lifecycleComplete: boolean;
  unresolvedCount: number;
}

export class MetricsCollector {
  /**
   * Directly audit PostgreSQL state for the given test tenant IDs.
   */
  async collectDatabaseState(tenantIds: string[]): Promise<DatabaseStateMetrics> {
    const [
      totalNotifications,
      totalDeliveries,
      pendingDeliveries,
      deliveredDeliveries,
      retryScheduledDeliveries,
      failedDeliveries,
      pendingOutbox,
      publishedOutbox,
      failedOutbox,
      totalRetryRecords,
      totalDeadLetterEvents,
    ] = await Promise.all([
      prisma.notification.count({
        where: { tenantId: { in: tenantIds } },
      }),
      prisma.notificationDelivery.count({
        where: { notification: { tenantId: { in: tenantIds } } },
      }),
      prisma.notificationDelivery.count({
        where: { notification: { tenantId: { in: tenantIds } }, status: 'PENDING' },
      }),
      prisma.notificationDelivery.count({
        where: { notification: { tenantId: { in: tenantIds } }, status: 'DELIVERED' },
      }),
      prisma.notificationDelivery.count({
        where: { notification: { tenantId: { in: tenantIds } }, status: 'RETRY_SCHEDULED' },
      }),
      prisma.notificationDelivery.count({
        where: { notification: { tenantId: { in: tenantIds } }, status: 'FAILED' },
      }),
      prisma.outboxEvent.count({
        where: { status: 'PENDING' },
      }),
      prisma.outboxEvent.count({
        where: { status: 'PUBLISHED' },
      }),
      prisma.outboxEvent.count({
        where: { status: 'FAILED' },
      }),
      prisma.retryRecord.count({
        where: { delivery: { notification: { tenantId: { in: tenantIds } } } },
      }),
      prisma.deadLetterEvent.count({
        where: { tenantId: { in: tenantIds } },
      }),
    ]);

    const unresolvedCount = pendingDeliveries + retryScheduledDeliveries;
    const lifecycleComplete = totalDeliveries > 0 && unresolvedCount === 0;

    return {
      totalNotifications,
      totalDeliveries,
      deliveriesByStatus: {
        PENDING: pendingDeliveries,
        DELIVERED: deliveredDeliveries,
        RETRY_SCHEDULED: retryScheduledDeliveries,
        FAILED: failedDeliveries,
      },
      outboxByStatus: {
        PENDING: pendingOutbox,
        PUBLISHED: publishedOutbox,
        FAILED: failedOutbox,
      },
      totalRetryRecords,
      totalDeadLetterEvents,
      lifecycleComplete,
      unresolvedCount,
    };
  }

  /**
   * Poll PostgreSQL until all in-flight deliveries reach terminal status (DELIVERED or FAILED) or timeout.
   */
  async waitForDrain(
    tenantIds: string[],
    maxWaitMs = 60000,
    intervalMs = 1000
  ): Promise<{ drained: boolean; elapsedMs: number; finalMetrics: DatabaseStateMetrics }> {
    const startTime = Date.now();

    while (Date.now() - startTime < maxWaitMs) {
      const state = await this.collectDatabaseState(tenantIds);
      if (state.totalDeliveries > 0 && state.unresolvedCount === 0 && state.outboxByStatus.PENDING === 0) {
        return {
          drained: true,
          elapsedMs: Date.now() - startTime,
          finalMetrics: state,
        };
      }
      await new Promise((r) => setTimeout(r, intervalMs));
    }

    const finalMetrics = await this.collectDatabaseState(tenantIds);
    return {
      drained: finalMetrics.unresolvedCount === 0 && finalMetrics.outboxByStatus.PENDING === 0,
      elapsedMs: Date.now() - startTime,
      finalMetrics,
    };
  }

  /**
   * Scrape Prometheus text format from an HTTP endpoint (e.g. API /metrics).
   */
  async scrapePrometheus(urlStr: string): Promise<string> {
    return new Promise((resolve, reject) => {
      const url = new URL(urlStr);
      http
        .get(url, (res) => {
          let body = '';
          res.on('data', (c) => (body += c));
          res.on('end', () => resolve(body));
        })
        .on('error', reject);
    });
  }
}
