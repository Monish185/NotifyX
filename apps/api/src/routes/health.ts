import type { FastifyPluginAsync } from 'fastify';
import { prisma } from '@notifyx/database';
import { getMetrics, getContentType } from '@notifyx/metrics';

export const healthRoutes: FastifyPluginAsync = async (fastify) => {
  // Liveness check: confirms process is responding
  fastify.get('/health', async (_request, reply) => {
    return reply.status(200).send({
      status: 'ok',
    });
  });

  fastify.get('/live', async (_request, reply) => {
    return reply.status(200).send({
      status: 'ok',
      uptime: process.uptime(),
    });
  });

  // Readiness check: confirms critical dependencies (PostgreSQL) are operational
  fastify.get('/ready', async (_request, reply) => {
    try {
      await prisma.$queryRaw`SELECT 1`;
      return reply.status(200).send({
        status: 'ok',
        database: 'connected',
        uptime: process.uptime(),
      });
    } catch {
      return reply.status(503).send({
        status: 'degraded',
        database: 'disconnected',
      });
    }
  });

  fastify.get('/readiness', async (_request, reply) => {
    try {
      await prisma.$queryRaw`SELECT 1`;
      return reply.status(200).send({
        status: 'ok',
        database: 'connected',
        uptime: process.uptime(),
      });
    } catch {
      return reply.status(503).send({
        status: 'degraded',
        database: 'disconnected',
      });
    }
  });

  // Prometheus metrics exposition
  fastify.get('/metrics', async (_request, reply) => {
    try {
      const metrics = await getMetrics();
      reply.header('Content-Type', getContentType());
      return reply.status(200).send(metrics);
    } catch (err: any) {
      reply.header('Content-Type', 'text/plain');
      return reply.status(500).send(err?.message || 'Error collecting metrics');
    }
  });
};
