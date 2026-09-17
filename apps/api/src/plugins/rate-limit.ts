import type { FastifyRequest, FastifyReply } from 'fastify';
import { tokenBucketLimiter } from '@notifyx/rate-limit';
import { tenantRateLimitService } from '../modules/tenant-rate-limits/tenant-rate-limit.service.js';

/**
 * Fastify preHandler hook applying tenant-level API rate limiting.
 *
 * Execution ordering:
 * 1. Request ID / correlation ID
 * 2. API key authentication & Tenant resolution (request.tenant)
 * 3. Tenant rate limiter (this hook)
 * 4. Route payload validation & business logic
 */
export async function rateLimitPreHandler(
  request: FastifyRequest,
  reply: FastifyReply
): Promise<void> {
  // If no authenticated tenant (e.g. unauthenticated public route), skip tenant rate limiting
  if (!request.tenant?.id) {
    return;
  }

  const tenantId = request.tenant.id;
  const settings = await tenantRateLimitService.getTenantRateLimits(tenantId);

  const result = await tokenBucketLimiter.consume(
    tenantId,
    settings,
    settings.isCustom ? 'custom' : 'default'
  );

  // Standard rate limit headers
  reply.header('x-ratelimit-limit', result.limit.toString());

  if (!result.allowed) {
    const retryAfterSec = Math.max(1, Math.ceil(result.retryAfterMs / 1000));
    reply.header('retry-after', retryAfterSec.toString());
    reply.header('x-ratelimit-remaining', '0');

    return reply.status(429).send({
      error: 'RATE_LIMITED',
      message: 'Rate limit exceeded',
      retryAfterMs: result.retryAfterMs,
    });
  }

  reply.header('x-ratelimit-remaining', result.remaining.toString());
}
