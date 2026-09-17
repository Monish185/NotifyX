import http from 'node:http';
import { prisma } from '@notifyx/database';
import { getMetrics, getContentType } from '@notifyx/metrics';
import { createLogger } from '@notifyx/logger';
import { scheduler } from './scheduler.js';

const logger = createLogger({ name: 'scheduler-service' });

const PORT = Number(process.env.SCHEDULER_HEALTH_PORT) || 3007;

// Create lightweight HTTP server for /health, /ready, and /metrics
const server = http.createServer(async (req, res) => {
  const url = req.url?.split('?')[0] || '/';

  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', service: 'scheduler' }));
    return;
  }

  if (url === '/ready') {
    try {
      await prisma.$queryRaw`SELECT 1`;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', service: 'scheduler', database: 'connected' }));
    } catch (err: any) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'error', database: 'disconnected', message: err.message }));
    }
    return;
  }

  if (url === '/metrics') {
    try {
      const metrics = await getMetrics();
      res.writeHead(200, { 'Content-Type': getContentType() });
      res.end(metrics);
    } catch (err: any) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end(`Error generating metrics: ${err.message}`);
    }
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not Found' }));
});

server.listen(PORT, () => {
  logger.info({ port: PORT }, `Scheduler health & metrics server listening on port ${PORT}`);
});

// Start scheduler background polling loop
scheduler.start();

// Graceful shutdown handling
async function shutdown(signal: string) {
  logger.info({ signal }, 'Received shutdown signal; stopping scheduler');

  await scheduler.stop();

  server.close(() => {
    logger.info('Scheduler health HTTP server closed');
  });

  await prisma.$disconnect();
  logger.info('Prisma database disconnected; shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
