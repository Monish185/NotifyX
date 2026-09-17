import type { FastifyPluginAsync } from 'fastify';
import { userService } from './user.service.js';
import { createUserSchema, getUserParamsSchema } from './user.schema.js';
import { authenticateApiKey } from '../../plugins/auth.js';

export const userRoutes: FastifyPluginAsync = async (fastify) => {
  fastify.post('/', async (request, reply) => {
    const parsed = createUserSchema.parse(request.body);

    if (request.headers.authorization) {
      await authenticateApiKey(request, reply);
      if (reply.sent) return;
    }

    const tenantId = request.tenant?.id || parsed.tenantId;

    if (!tenantId) {
      return reply.status(401).send({
        statusCode: 401,
        error: 'Unauthorized',
        message: 'Must authenticate with an API key or provide tenantId.',
      });
    }

    const user = await userService.createUser(tenantId, parsed);
    return reply.status(201).send(user);
  });

  fastify.get('/:id', { preHandler: [authenticateApiKey] }, async (request, reply) => {
    const tenantId = request.tenant!.id;
    const params = getUserParamsSchema.parse(request.params);
    const user = await userService.getUserById(params.id, tenantId);
    return reply.status(200).send(user);
  });
};
