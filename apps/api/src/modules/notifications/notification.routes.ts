import type { FastifyPluginAsync } from 'fastify';
import { notificationService } from './notification.service.js';
import {
  createNotificationSchema,
  getNotificationParamsSchema,
  cancelNotificationParamsSchema,
  listNotificationsQuerySchema,
} from './notification.schema.js';
import { authenticateApiKey } from '../../plugins/auth.js';
import { rateLimitPreHandler } from '../../plugins/rate-limit.js';

export const notificationRoutes: FastifyPluginAsync = async (fastify) => {
  fastify.addHook('preHandler', authenticateApiKey);
  fastify.addHook('preHandler', rateLimitPreHandler);

  // POST /v1/notifications
  fastify.post('/', async (request, reply) => {
    const parsed = createNotificationSchema.parse(request.body);
    const idempotencyKey = (request.headers['idempotency-key'] as string | undefined)?.trim();

    const result = await notificationService.createNotification(
      request.tenant!.id,
      parsed,
      idempotencyKey,
      (request as any).correlationId
    );

    if (result.idempotencyReplay) {
      reply.header('x-idempotent-replay', 'true');
    }

    return reply.status(202).send(result);
  });

  // POST /v1/notifications/:id/cancel
  fastify.post('/:id/cancel', async (request, reply) => {
    const params = cancelNotificationParamsSchema.parse(request.params);
    const result = await notificationService.cancelNotification(
      request.tenant!.id,
      params.id
    );
    return reply.status(200).send(result);
  });

  // GET /v1/notifications/:id
  fastify.get('/:id', async (request, reply) => {
    const params = getNotificationParamsSchema.parse(request.params);
    const notification = await notificationService.getNotification(
      request.tenant!.id,
      params.id
    );
    return reply.status(200).send(notification);
  });

  // GET /v1/notifications
  fastify.get('/', async (request, reply) => {
    const query = listNotificationsQuerySchema.parse(request.query);
    const result = await notificationService.listNotifications(
      request.tenant!.id,
      query
    );
    return reply.status(200).send(result);
  });
};
