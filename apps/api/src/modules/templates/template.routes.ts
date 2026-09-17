import type { FastifyPluginAsync } from 'fastify';
import { authenticateApiKey } from '../../plugins/auth.js';
import { templateService } from './template.service.js';
import {
  createTemplateSchema,
  createTemplateVersionSchema,
  templateParamsSchema,
  templateVersionParamsSchema,
  listTemplatesQuerySchema,
} from './template.schema.js';

export const templateRoutes: FastifyPluginAsync = async (fastify) => {
  fastify.addHook('preHandler', authenticateApiKey);

  // POST /v1/templates
  fastify.post('/', async (request, reply) => {
    const tenantId = request.tenant!.id;
    const body = createTemplateSchema.parse(request.body);
    const template = await templateService.createTemplate(tenantId, body);
    return reply.status(201).send(template);
  });

  // GET /v1/templates
  fastify.get('/', async (request, reply) => {
    const tenantId = request.tenant!.id;
    const query = listTemplatesQuerySchema.parse(request.query);
    const result = await templateService.listTemplates(
      tenantId,
      query.page,
      query.limit,
      query.channel as any
    );
    return reply.status(200).send(result);
  });

  // GET /v1/templates/:idOrKey
  fastify.get('/:idOrKey', async (request, reply) => {
    const tenantId = request.tenant!.id;
    const params = templateParamsSchema.parse(request.params);
    const template = await templateService.getTemplate(tenantId, params.idOrKey);
    return reply.status(200).send(template);
  });

  // POST /v1/templates/:id/versions
  fastify.post('/:id/versions', async (request, reply) => {
    const tenantId = request.tenant!.id;
    const params = templateParamsSchema.parse({ idOrKey: (request.params as any).id });
    const body = createTemplateVersionSchema.parse(request.body);
    const version = await templateService.createVersion(tenantId, params.idOrKey, body);
    return reply.status(201).send(version);
  });

  // POST /v1/templates/:id/versions/:versionId/activate
  fastify.post('/:id/versions/:versionId/activate', async (request, reply) => {
    const tenantId = request.tenant!.id;
    const params = templateVersionParamsSchema.parse(request.params);
    const activated = await templateService.activateVersion(
      tenantId,
      params.id,
      params.versionId
    );
    return reply.status(200).send(activated);
  });

  // POST /v1/templates/:id/versions/:versionId/archive
  fastify.post('/:id/versions/:versionId/archive', async (request, reply) => {
    const tenantId = request.tenant!.id;
    const params = templateVersionParamsSchema.parse(request.params);
    const archived = await templateService.archiveVersion(
      tenantId,
      params.id,
      params.versionId
    );
    return reply.status(200).send(archived);
  });
};
