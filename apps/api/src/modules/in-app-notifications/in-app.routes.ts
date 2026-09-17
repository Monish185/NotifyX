import type { FastifyPluginAsync } from 'fastify';
import { authenticateApiKey } from '../../plugins/auth.js';
import { inAppNotificationService } from './in-app.service.js';
import {
  listInAppQuerySchema,
  inAppIdParamSchema,
  readAllBodySchema,
} from './in-app.schema.js';

export const inAppRoutes: FastifyPluginAsync = async (app) => {
  // 1. List user in-app notifications
  app.get(
    '/',
    { preHandler: [authenticateApiKey] },
    async (request, reply) => {
      const tenantId = request.tenant!.id;
      const query = listInAppQuerySchema.parse(request.query);
      const result = await inAppNotificationService.listInAppNotifications(tenantId, query);
      return reply.status(200).send(result);
    }
  );

  // 2. Get unread count for user
  app.get(
    '/unread-count',
    { preHandler: [authenticateApiKey] },
    async (request, reply) => {
      const tenantId = request.tenant!.id;
      const { userId } = request.query as { userId?: string };
      if (!userId) {
        return reply.status(400).send({
          statusCode: 400,
          error: 'Bad Request',
          message: 'userId query parameter is required',
        });
      }

      const result = await inAppNotificationService.getUnreadCount(tenantId, userId);
      return reply.status(200).send(result);
    }
  );

  // 3. Mark single notification as read
  app.post(
    '/:id/read',
    { preHandler: [authenticateApiKey] },
    async (request, reply) => {
      const tenantId = request.tenant!.id;
      const { id } = inAppIdParamSchema.parse(request.params);
      const result = await inAppNotificationService.markAsRead(tenantId, id);
      return reply.status(200).send(result);
    }
  );

  // 4. Mark all unread notifications as read for user
  app.post(
    '/read-all',
    { preHandler: [authenticateApiKey] },
    async (request, reply) => {
      const tenantId = request.tenant!.id;
      const { userId } = readAllBodySchema.parse(request.body);
      const result = await inAppNotificationService.markAllAsRead(tenantId, userId);
      return reply.status(200).send(result);
    }
  );
};
