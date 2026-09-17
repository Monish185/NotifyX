import type { FastifyPluginAsync } from 'fastify';
import { tenantService } from './tenant.service.js';
import { createTenantSchema, getTenantParamsSchema } from './tenant.schema.js';

export const tenantRoutes: FastifyPluginAsync = async (fastify) => {
  fastify.post('/', async (request, reply) => {
    const parsed = createTenantSchema.parse(request.body);
    const tenant = await tenantService.createTenant(parsed);
    return reply.status(201).send(tenant);
  });

  fastify.get('/:id', async (request, reply) => {
    const params = getTenantParamsSchema.parse(request.params);
    const tenant = await tenantService.getTenantById(params.id);
    return reply.status(200).send(tenant);
  });
};
