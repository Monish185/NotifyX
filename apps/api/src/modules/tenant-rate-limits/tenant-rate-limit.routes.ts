import type { FastifyPluginAsync } from 'fastify';
import { z } from 'zod';
import { authenticateApiKey } from '../../plugins/auth.js';
import { rateLimitPreHandler } from '../../plugins/rate-limit.js';
import { tenantRateLimitService } from './tenant-rate-limit.service.js';

export const updateRateLimitsSchema = z.object({
  requestsPerSecond: z.number().int().min(1).max(10000).optional(),
  burstCapacity: z.number().int().min(1).max(20000).optional(),
  notificationsPerMinute: z.number().int().min(1).max(100000).optional(),
  notificationsPerDay: z.number().int().min(1).max(10000000).optional(),
  enabled: z.boolean().optional(),
});

export const tenantRateLimitRoutes: FastifyPluginAsync = async (fastify) => {
  fastify.addHook('preHandler', authenticateApiKey);
  fastify.addHook('preHandler', rateLimitPreHandler);

  // GET /v1/tenant/rate-limits
  fastify.get('/rate-limits', async (request, reply) => {
    const limits = await tenantRateLimitService.getTenantRateLimits(request.tenant!.id);
    return reply.status(200).send(limits);
  });

  // PUT /v1/tenant/rate-limits
  fastify.put('/rate-limits', async (request, reply) => {
    const parsed = updateRateLimitsSchema.parse(request.body);
    const updated = await tenantRateLimitService.updateTenantRateLimits(
      request.tenant!.id,
      parsed
    );
    return reply.status(200).send(updated);
  });
};
