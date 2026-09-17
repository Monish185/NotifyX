import Fastify, { type FastifyInstance } from 'fastify';
import { ZodError } from 'zod';
import { randomUUID } from 'node:crypto';
import { logger } from '@notifyx/logger';
import { httpRequestDurationHistogram } from '@notifyx/metrics';
import { registerPlugins } from './plugins/index.js';
import { healthRoutes } from './routes/health.js';
import { tenantRoutes } from './modules/tenants/tenant.routes.js';
import { userRoutes } from './modules/users/user.routes.js';
import { apiKeyRoutes } from './modules/api-keys/api-key.routes.js';
import { notificationRoutes } from './modules/notifications/notification.routes.js';
import { inAppRoutes } from './modules/in-app-notifications/in-app.routes.js';
import { templateRoutes } from './modules/templates/template.routes.js';
import { preferenceRoutes } from './modules/preferences/preference.routes.js';
import { tenantRateLimitRoutes } from './modules/tenant-rate-limits/tenant-rate-limit.routes.js';

export async function buildApp(): Promise<FastifyInstance> {
  const app = Fastify({
    loggerInstance: logger as any,
    disableRequestLogging: process.env.NODE_ENV === 'test',
  });

  // Correlation & Request tracking hook
  app.addHook('onRequest', async (request, reply) => {
    const rawCorrelation = request.headers['x-correlation-id'] || request.headers['x-request-id'];
    const correlationId =
      typeof rawCorrelation === 'string' && rawCorrelation.trim().length > 0
        ? rawCorrelation.trim()
        : `corr_${randomUUID().replace(/-/g, '')}`;

    (request as any).correlationId = correlationId;
    (request as any).startTime = Date.now();

    reply.header('x-correlation-id', correlationId);
    reply.header('x-request-id', request.id);
  });

  // Prometheus HTTP metrics hook
  app.addHook('onResponse', async (request, reply) => {
    const startTime = (request as any).startTime || Date.now();
    const durationSec = (Date.now() - startTime) / 1000;
    const route = request.routeOptions?.url || request.url.split('?')[0] || 'unknown';

    httpRequestDurationHistogram.observe(
      {
        method: request.method,
        route,
        status_code: String(reply.statusCode),
      },
      durationSec
    );
  });

  // Register core plugins (cors, helmet, sensible)
  await registerPlugins(app);

  // Global error handler
  app.setErrorHandler((error: any, request, reply) => {
    if (error instanceof ZodError) {
      return reply.status(400).send({
        statusCode: 400,
        error: 'Bad Request',
        message: 'Validation failed',
        details: error.errors.map((e) => ({
          path: e.path.join('.'),
          message: e.message,
        })),
      });
    }

    const statusCode = error.statusCode || 500;

    if (statusCode === 429) {
      const retryAfterMs = typeof error.retryAfterMs === 'number' ? error.retryAfterMs : 1000;
      const retryAfterSec = Math.max(1, Math.ceil(retryAfterMs / 1000));
      reply.header('retry-after', String(retryAfterSec));
      return reply.status(429).send({
        statusCode: 429,
        error: error.errorCode || 'RATE_LIMITED',
        message: error.message || 'Rate limit exceeded',
        retryAfterMs,
      });
    }

    if (statusCode >= 500) {
      request.log.error({ err: error }, 'Internal Server Error');
      return reply.status(500).send({
        statusCode: 500,
        error: 'Internal Server Error',
        message: 'An unexpected internal error occurred',
      });
    }

    return reply.status(statusCode).send({
      statusCode,
      error: error.name || 'Error',
      message: error.message,
    });
  });

  // Health route
  await app.register(healthRoutes);

  // Core domain routes under /v1
  await app.register(tenantRoutes, { prefix: '/v1/tenants' });
  await app.register(userRoutes, { prefix: '/v1/users' });
  await app.register(apiKeyRoutes, { prefix: '/v1/api-keys' });
  await app.register(notificationRoutes, { prefix: '/v1/notifications' });
  await app.register(inAppRoutes, { prefix: '/v1/in-app-notifications' });
  await app.register(templateRoutes, { prefix: '/v1/templates' });
  await app.register(preferenceRoutes, { prefix: '/v1/users' });
  await app.register(tenantRateLimitRoutes, { prefix: '/v1/tenant' });
  await app.register(tenantRateLimitRoutes, { prefix: '/v1/tenants' });

  // Fallback 404 handler
  app.setNotFoundHandler((request, reply) => {
    reply.status(404).send({
      statusCode: 404,
      error: 'Not Found',
      message: `Route ${request.method}:${request.url} not found`,
    });
  });

  return app;
}
