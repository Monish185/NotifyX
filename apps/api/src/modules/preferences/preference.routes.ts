import type { FastifyPluginAsync } from 'fastify';
import { authenticateApiKey } from '../../plugins/auth.js';
import { preferenceService } from './preference.service.js';
import {
  userPreferenceParamsSchema,
  updatePreferenceSchema,
  bulkUpdatePreferencesSchema,
} from './preference.schema.js';

export const preferenceRoutes: FastifyPluginAsync = async (fastify) => {
  fastify.addHook('preHandler', authenticateApiKey);

  // GET /v1/users/:userId/preferences
  fastify.get('/:userId/preferences', async (request, reply) => {
    const tenantId = request.tenant!.id;
    const params = userPreferenceParamsSchema.parse(request.params);
    const result = await preferenceService.getUserPreferences(tenantId, params.userId);
    return reply.status(200).send(result);
  });

  // PUT /v1/users/:userId/preferences
  fastify.put('/:userId/preferences', async (request, reply) => {
    const tenantId = request.tenant!.id;
    const params = userPreferenceParamsSchema.parse(request.params);
    const body = updatePreferenceSchema.parse(request.body);
    const updated = await preferenceService.updatePreference(
      tenantId,
      params.userId,
      body
    );
    return reply.status(200).send(updated);
  });

  // POST /v1/users/:userId/preferences/bulk
  fastify.post('/:userId/preferences/bulk', async (request, reply) => {
    const tenantId = request.tenant!.id;
    const params = userPreferenceParamsSchema.parse(request.params);
    const body = bulkUpdatePreferencesSchema.parse(request.body);
    const result = await preferenceService.bulkUpdatePreferences(
      tenantId,
      params.userId,
      body
    );
    return reply.status(200).send(result);
  });
};
