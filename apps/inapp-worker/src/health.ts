import http from 'node:http';
import { prisma } from '@notifyx/database';
import { logger } from '@notifyx/logger';
import { getMetrics, getContentType } from '@notifyx/metrics';
import { InAppWorker } from './worker.js';

export function createHealthServer(worker: InAppWorker, port = 3003): http.Server {
  const server = http.createServer(async (req, res) => {
    // Prometheus metrics exposition
    if (req.url === '/metrics') {
      try {
        const metrics = await getMetrics();
        res.writeHead(200, { 'Content-Type': getContentType() });
        res.end(metrics);
      } catch (err: any) {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end(err?.message || 'Error collecting metrics');
      }
      return;
    }

    // Liveness check: confirms process is responding
    if (req.url === '/health' || req.url === '/live') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          status: 'ok',
          service: 'inapp-worker',
          uptime: process.uptime(),
        })
      );
      return;
    }

    // Readiness check: confirms PostgreSQL and Kafka consumer are operational
    if (req.url === '/ready' || req.url === '/readiness') {
      let postgresOk = false;
      try {
        await prisma.$queryRaw`SELECT 1`;
        postgresOk = true;
      } catch {
        postgresOk = false;
      }

      const kafkaOk = worker.isReady();
      const isReady = postgresOk && kafkaOk;
      const statusCode = isReady ? 200 : 503;

      res.writeHead(statusCode, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          status: isReady ? 'ok' : 'degraded',
          service: 'inapp-worker',
          consumerGroup: 'notifyx-inapp-workers',
          postgres: postgresOk ? 'healthy' : 'unhealthy',
          kafkaConsumer: kafkaOk ? 'connected' : 'disconnected',
          uptime: process.uptime(),
        })
      );
      return;
    }

    res.writeHead(404);
    res.end();
  });

  server.listen(port, () => {
    logger.info({ port }, 'In-App Worker health server listening');
  });

  return server;
}
