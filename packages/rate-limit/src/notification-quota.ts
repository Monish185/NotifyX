import { Redis } from 'ioredis';
import { getRateLimitRedis } from './redis-client.js';
import { checkPostgresQuotaFallback } from './fallback-quota.js';
import {
  type TenantRateLimitSettings,
  type QuotaResult,
  DEFAULT_TENANT_RATE_LIMITS,
} from './types.js';
import { notificationQuotaRejectionsCounter } from '@notifyx/metrics';
import { logger } from '@notifyx/logger';

/**
 * Atomic Notification Quota Redis Lua Script.
 *
 * Evaluates both per-minute ingestion rate and per-day tenant volume in a single atomic transaction.
 *
 * KEYS[1]: quota:tenant:{tenantId}:min:{minuteWindow}
 * KEYS[2]: quota:tenant:{tenantId}:day:{dayWindow}
 * ARGV[1]: maxPerMinute (number)
 * ARGV[2]: maxPerDay (number)
 * ARGV[3]: requestedCount (number)
 * ARGV[4]: minuteTtlSec (number)
 * ARGV[5]: dayTtlSec (number)
 * ARGV[6]: remainingMinuteMs (number)
 * ARGV[7]: remainingDayMs (number)
 *
 * Returns: [ allowed (1 or 0), reasonString, retryAfterMs (integer), remainingInMinute (integer) ]
 */
const NOTIFICATION_QUOTA_LUA = `
local minKey = KEYS[1]
local dayKey = KEYS[2]
local maxPerMin = tonumber(ARGV[1])
local maxPerDay = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local minTtlSec = tonumber(ARGV[4])
local dayTtlSec = tonumber(ARGV[5])
local remainingMinMs = tonumber(ARGV[6])
local remainingDayMs = tonumber(ARGV[7])

local currMin = tonumber(redis.call('GET', minKey) or '0')
local currDay = tonumber(redis.call('GET', dayKey) or '0')

if (currMin + requested) > maxPerMin then
    local remaining = math.max(0, maxPerMin - currMin)
    return { 0, 'MINUTE_QUOTA_EXCEEDED', remainingMinMs, remaining }
end

if (currDay + requested) > maxPerDay then
    local remaining = math.max(0, maxPerDay - currDay)
    return { 0, 'DAY_QUOTA_EXCEEDED', remainingDayMs, remaining }
end

local newMin = redis.call('INCRBY', minKey, requested)
if newMin == requested then
    redis.call('EXPIRE', minKey, minTtlSec)
end

local newDay = redis.call('INCRBY', dayKey, requested)
if newDay == requested then
    redis.call('EXPIRE', dayKey, dayTtlSec)
end

local remainingInMin = math.max(0, maxPerMin - newMin)
return { 1, 'OK', 0, remainingInMin }
`;

export class NotificationQuotaLimiter {
  private customRedis: Redis | null;

  constructor(redisClient?: Redis) {
    this.customRedis = redisClient || null;
  }

  private getRedis(): Redis | null {
    return this.customRedis || getRateLimitRedis();
  }

  /**
   * Evaluates and accounts for notification volume against tenant quotas.
   *
   * @param tenantId The authenticated tenant ID.
   * @param settings Tenant quota settings (optional; falls back to defaults).
   * @param tenantType Classification ('custom' | 'default') for bounded metrics.
   */
  async consume(
    tenantId: string,
    settings?: Partial<TenantRateLimitSettings>,
    tenantType: 'custom' | 'default' = 'default'
  ): Promise<QuotaResult> {
    const minuteLimit = settings?.notificationsPerMinute ?? DEFAULT_TENANT_RATE_LIMITS.notificationsPerMinute;
    const dayLimit = settings?.notificationsPerDay ?? DEFAULT_TENANT_RATE_LIMITS.notificationsPerDay;
    const enabled = settings?.enabled ?? DEFAULT_TENANT_RATE_LIMITS.enabled;

    if (!enabled) {
      return {
        allowed: true,
        remaining: minuteLimit,
        retryAfterMs: 0,
        limit: minuteLimit,
        quotaType: 'none',
      };
    }

    const redis = this.getRedis();

    // If Redis is unavailable, use the concurrency-safe PostgreSQL fallback
    if (!redis) {
      logger.warn(
        { tenantId },
        'Redis unavailable for notification quota; invoking concurrency-safe PostgreSQL fallback'
      );
      return checkPostgresQuotaFallback(tenantId, minuteLimit, dayLimit, tenantType);
    }

    const now = new Date();
    const minuteWindow = Math.floor(now.getTime() / 60000);
    const dayWindow = now.toISOString().slice(0, 10);

    const minKey = `quota:tenant:${tenantId}:min:${minuteWindow}`;
    const dayKey = `quota:tenant:${tenantId}:day:${dayWindow}`;

    const requested = 1;
    const minTtlSec = 120;
    const dayTtlSec = 172800; // 2 days

    const remainingMinuteMs = 60000 - (now.getTime() % 60000);
    const remainingDayMs =
      (86400 - (now.getUTCHours() * 3600 + now.getUTCMinutes() * 60 + now.getUTCSeconds())) * 1000;

    try {
      const res = (await redis.eval(
        NOTIFICATION_QUOTA_LUA,
        2,
        minKey,
        dayKey,
        minuteLimit.toString(),
        dayLimit.toString(),
        requested.toString(),
        minTtlSec.toString(),
        dayTtlSec.toString(),
        remainingMinuteMs.toString(),
        remainingDayMs.toString()
      )) as [number, string, number, number];

      const allowed = res[0] === 1;
      const statusReason = res[1];
      const retryAfterMs = Number(res[2]);
      const remaining = Number(res[3]);

      if (!allowed) {
        const quotaWindow = statusReason === 'MINUTE_QUOTA_EXCEEDED' ? 'minute' : 'day';
        notificationQuotaRejectionsCounter.inc({
          tenant_type: tenantType,
          quota_window: quotaWindow,
        });

        return {
          allowed: false,
          remaining: 0,
          retryAfterMs,
          limit: quotaWindow === 'minute' ? minuteLimit : dayLimit,
          quotaType: quotaWindow,
          reason:
            quotaWindow === 'minute'
              ? 'Notification rate limit exceeded (per minute)'
              : 'Daily notification quota exceeded',
        };
      }

      return {
        allowed: true,
        remaining,
        retryAfterMs: 0,
        limit: minuteLimit,
        quotaType: 'none',
      };
    } catch (err: any) {
      logger.warn(
        { tenantId, error: err.message },
        'Redis notification quota evaluation failed; invoking PostgreSQL fallback'
      );
      return checkPostgresQuotaFallback(tenantId, minuteLimit, dayLimit, tenantType);
    }
  }

  /**
   * Resets notification quota keys for a tenant (useful for testing).
   */
  async reset(tenantId: string): Promise<void> {
    const redis = this.getRedis();
    if (redis) {
      const keys = await redis.keys(`quota:tenant:${tenantId}:*`);
      if (keys.length > 0) {
        await redis.del(...keys);
      }
    }
  }
}

export const notificationQuotaLimiter = new NotificationQuotaLimiter();
