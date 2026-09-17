import type { FastifyPluginAsync } from 'fastify';
import { apiKeyService } from './api-key.service.js';
import { createApiKeySchema, revokeApiKeyParamsSchema } from './api-key.schema.js';
import { authenticateApiKey } from '../../plugins/auth.js';

export const apiKeyRoutes: FastifyPluginAsync = async (fastify) => {
  fastify.post('/', async (request, reply) => {
    const parsed = createApiKeySchema.parse(request.body);

    // If Authorization header is provided, use authenticated tenant context
    if (request.headers.authorization) {
      await authenticateApiKey(request, reply);
      if (reply.sent) return;
    }

    const tenantId = request.tenant?.id || parsed.tenantId;

    if (!tenantId) {
      return reply.status(401).send({
        statusCode: 401,
        error: 'Unauthorized',
        message: 'Must authenticate with an existing API key or specify tenantId for bootstrap.',
      });
    }

    const result = await apiKeyService.createApiKey(tenantId, parsed);
    return reply.status(201).send(result);
  });

  fastify.get('/', { preHandler: [authenticateApiKey] }, async (request, reply) => {
    const tenantId = request.tenant!.id;
    const keys = await apiKeyService.listApiKeys(tenantId);
    return reply.status(200).send(keys);
  });

  fastify.delete('/:id', { preHandler: [authenticateApiKey] }, async (request, reply) => {
    const tenantId = request.tenant!.id;
    const params = revokeApiKeyParamsSchema.parse(request.params);
    const result = await apiKeyService.revokeApiKey(params.id, tenantId);
    return reply.status(200).send(result);
  });
};
