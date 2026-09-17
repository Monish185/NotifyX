import { Redis } from 'ioredis';
import { getRateLimitRedis } from './redis-client.js';
import {
  type TenantRateLimitSettings,
  type RateLimitResult,
  DEFAULT_TENANT_RATE_LIMITS,
} from './types.js';
import {
  tenantRateLimitRejectionsCounter,
  rateLimitRemainingGauge,
} from '@notifyx/metrics';
import { getConfig } from '@notifyx/config';
import { logger } from '@notifyx/logger';

/**
 * Atomic Token Bucket Redis Lua Script.
 *
 * KEYS[1]: ratelimit:tenant:{tenantId}:api
 * ARGV[1]: burstCapacity (number)
 * ARGV[2]: refillRatePerSec (number)
 * ARGV[3]: nowMs (number)
 * ARGV[4]: requestedTokens (number)
 * ARGV[5]: ttlSeconds (number)
 *
 * Returns: [ allowed (1 or 0), remainingTokens (integer), retryAfterMs (integer) ]
 */
const TOKEN_BUCKET_LUA = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refillRate = tonumber(ARGV[2])
local nowMs = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttlSec = tonumber(ARGV[5])

local data = redis.call('HMGET', key, 'tokens', 'lastRefillMs')
local tokens = tonumber(data[1])
local lastRefillMs = tonumber(data[2])

if tokens == nil or lastRefillMs == nil then
    tokens = capacity
    lastRefillMs = nowMs
else
    local elapsedMs = math.max(0, nowMs - lastRefillMs)
    local tokensToAdd = (elapsedMs * refillRate) / 1000.0
    tokens = math.min(capacity, tokens + tokensToAdd)
    lastRefillMs = nowMs
end

local allowed = 0
local retryAfterMs = 0

if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
else
    local missing = requested - tokens
    if refillRate > 0 then
        retryAfterMs = math.ceil((missing * 1000.0) / refillRate)
    else
        retryAfterMs = 1000
    end
end

redis.call('HMSET', key, 'tokens', tokens, 'lastRefillMs', lastRefillMs)
redis.call('EXPIRE', key, ttlSec)

return { allowed, math.floor(tokens), retryAfterMs }
`;

export class TokenBucketLimiter {
  private customRedis: Redis | null;

  constructor(redisClient?: Redis) {
    this.customRedis = redisClient || null;
  }

  private getRedis(): Redis | null {
    return this.customRedis || getRateLimitRedis();
  }

  /**
   * Evaluates API request against tenant's token bucket.
   *
   * @param tenantId The authenticated tenant ID.
   * @param settings Tenant-specific rate-limit settings (optional; falls back to defaults).
   * @param tenantType Classification ('custom' | 'default') for bounded metric labeling.
   */
  async consume(
    tenantId: string,
    settings?: Partial<TenantRateLimitSettings>,
    tenantType: 'custom' | 'default' = 'default'
  ): Promise<RateLimitResult> {
    const burstCapacity = settings?.burstCapacity ?? DEFAULT_TENANT_RATE_LIMITS.burstCapacity;
    const refillRatePerSec = settings?.requestsPerSecond ?? DEFAULT_TENANT_RATE_LIMITS.requestsPerSecond;
    const enabled = settings?.enabled ?? DEFAULT_TENANT_RATE_LIMITS.enabled;

    if (!enabled) {
      return {
        allowed: true,
        remaining: burstCapacity,
        retryAfterMs: 0,
        limit: burstCapacity,
      };
    }

    const redis = this.getRedis();
    const config = getConfig();
    const failClosed = config.RATE_LIMIT_FAIL_CLOSED ?? true;

    if (!redis) {
      logger.warn(
        { tenantId, failClosed },
        'Redis unavailable for API rate limiting; applying configured failure policy'
      );
      if (failClosed) {
        tenantRateLimitRejectionsCounter.inc({ tenant_type: tenantType });
        return {
          allowed: false,
          remaining: 0,
          retryAfterMs: 1000,
          limit: burstCapacity,
        };
      } else {
        return {
          allowed: true,
          remaining: burstCapacity,
          retryAfterMs: 0,
          limit: burstCapacity,
        };
      }
    }

    const key = `ratelimit:tenant:${tenantId}:api`;
    const nowMs = Date.now();
    const requested = 1;
    // TTL: enough to refill from 0 to full capacity + buffer
    const ttlSeconds = Math.max(60, Math.ceil(burstCapacity / Math.max(1, refillRatePerSec)) * 2 + 60);

    try {
      const res = (await redis.eval(
        TOKEN_BUCKET_LUA,
        1,
        key,
        burstCapacity.toString(),
        refillRatePerSec.toString(),
        nowMs.toString(),
        requested.toString(),
        ttlSeconds.toString()
      )) as [number, number, number];

      const allowed = res[0] === 1;
      const remaining = Number(res[1]);
      const retryAfterMs = Number(res[2]);

      // System-level aggregate metric sample without tenantId label
      rateLimitRemainingGauge.set(remaining);

      if (!allowed) {
        tenantRateLimitRejectionsCounter.inc({ tenant_type: tenantType });
      }

      return {
        allowed,
        remaining,
        retryAfterMs,
        limit: burstCapacity,
      };
    } catch (err: any) {
      logger.warn(
        { tenantId, error: err.message, failClosed },
        'Redis error during rate-limit evaluation; applying failure policy'
      );

      if (failClosed) {
        tenantRateLimitRejectionsCounter.inc({ tenant_type: tenantType });
        return {
          allowed: false,
          remaining: 0,
          retryAfterMs: 1000,
          limit: burstCapacity,
        };
      }

      return {
        allowed: true,
        remaining: burstCapacity,
        retryAfterMs: 0,
        limit: burstCapacity,
      };
    }
  }

  /**
   * Resets the rate limiter key for a tenant (useful for testing).
   */
  async reset(tenantId: string): Promise<void> {
    const redis = this.getRedis();
    if (redis) {
      await redis.del(`ratelimit:tenant:${tenantId}:api`);
    }
  }
}

export const tokenBucketLimiter = new TokenBucketLimiter();
