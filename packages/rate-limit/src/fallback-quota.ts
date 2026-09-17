import { prisma } from '@notifyx/database';
import { logger } from '@notifyx/logger';
import { type QuotaResult } from './types.js';
import { notificationQuotaRejectionsCounter } from '@notifyx/metrics';

/**
 * Concurrency-safe PostgreSQL fallback for notification quota enforcement.
 *
 * Uses atomic row-level upsert and transaction rollback on limit exhaustion
 * to prevent race conditions without naive select-then-insert patterns.
 */
export async function checkPostgresQuotaFallback(
  tenantId: string,
  minuteLimit: number,
  dayLimit: number,
  tenantType: 'custom' | 'default' = 'default'
): Promise<QuotaResult> {
  const now = new Date();
  const minuteKey = `min:${now.toISOString().slice(0, 16)}`; // YYYY-MM-DDTHH:mm
  const dayKey = `day:${now.toISOString().slice(0, 10)}`; // YYYY-MM-DD

  try {
    const result = await prisma.$transaction(async (tx) => {
      // 1. Atomic minute quota increment with row lock
      const minRes = await tx.$queryRawUnsafe<Array<{ count: number }>>(
        `
        INSERT INTO usage_quota_counters ("id", "tenantId", "windowKey", "count", "createdAt", "updatedAt")
        VALUES (gen_random_uuid()::text, $1, $2, 1, NOW(), NOW())
        ON CONFLICT ("tenantId", "windowKey")
        DO UPDATE SET "count" = usage_quota_counters."count" + 1, "updatedAt" = NOW()
        RETURNING "count";
        `,
        tenantId,
        minuteKey
      );

      const minCount = minRes[0]?.count ?? 1;
      if (minCount > minuteLimit) {
        throw new Error(`QUOTA_EXCEEDED:minute:${minCount}`);
      }

      // 2. Atomic daily quota increment with row lock
      const dayRes = await tx.$queryRawUnsafe<Array<{ count: number }>>(
        `
        INSERT INTO usage_quota_counters ("id", "tenantId", "windowKey", "count", "createdAt", "updatedAt")
        VALUES (gen_random_uuid()::text, $1, $2, 1, NOW(), NOW())
        ON CONFLICT ("tenantId", "windowKey")
        DO UPDATE SET "count" = usage_quota_counters."count" + 1, "updatedAt" = NOW()
        RETURNING "count";
        `,
        tenantId,
        dayKey
      );

      const dayCount = dayRes[0]?.count ?? 1;
      if (dayCount > dayLimit) {
        throw new Error(`QUOTA_EXCEEDED:day:${dayCount}`);
      }

      return {
        minCount,
        dayCount,
      };
    });

    return {
      allowed: true,
      remaining: Math.max(0, minuteLimit - result.minCount),
      retryAfterMs: 0,
      limit: minuteLimit,
      quotaType: 'none',
    };
  } catch (err: any) {
    if (err.message?.startsWith('QUOTA_EXCEEDED:minute')) {
      const remainingSec = 60 - now.getSeconds();
      notificationQuotaRejectionsCounter.inc({ tenant_type: tenantType, quota_window: 'minute' });
      return {
        allowed: false,
        remaining: 0,
        retryAfterMs: remainingSec * 1000,
        limit: minuteLimit,
        quotaType: 'minute',
        reason: 'Tenant notification rate limit exceeded (per minute)',
      };
    }

    if (err.message?.startsWith('QUOTA_EXCEEDED:day')) {
      const remainingSec = 86400 - (now.getHours() * 3600 + now.getMinutes() * 60 + now.getSeconds());
      notificationQuotaRejectionsCounter.inc({ tenant_type: tenantType, quota_window: 'day' });
      return {
        allowed: false,
        remaining: 0,
        retryAfterMs: remainingSec * 1000,
        limit: dayLimit,
        quotaType: 'day',
        reason: 'Tenant daily notification quota exceeded',
      };
    }

    // On unexpected database error, fail closed safely to avoid unbounded traffic
    logger.error(
      { tenantId, error: err.message },
      'PostgreSQL fallback quota check encountered an unexpected error; failing closed'
    );
    notificationQuotaRejectionsCounter.inc({ tenant_type: tenantType, quota_window: 'minute' });
    return {
      allowed: false,
      remaining: 0,
      retryAfterMs: 1000,
      limit: minuteLimit,
      quotaType: 'minute',
      reason: 'Quota service unavailable; failed closed',
    };
  }
}
