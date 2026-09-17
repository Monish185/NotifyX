import http from 'node:http';
import { prisma } from '@notifyx/database';
import { KafkaProducer } from '@notifyx/kafka';
import { logger } from '@notifyx/logger';
import { getConfig } from '@notifyx/config';
import { getMetrics, getContentType } from '@notifyx/metrics';
import { OutboxPublisher } from './publisher.js';

const HEALTH_PORT = process.env.PUBLISHER_HEALTH_PORT
  ? parseInt(process.env.PUBLISHER_HEALTH_PORT, 10)
  : 3002;

let publisher: OutboxPublisher | null = null;
let producer: KafkaProducer | null = null;
let isShuttingDown = false;

// HTTP Health, Readiness & Metrics Server
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

  // Liveness check: confirms process is alive
  if (req.url === '/health' || req.url === '/live') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(
      JSON.stringify({
        status: isShuttingDown ? 'shutting_down' : 'ok',
        service: 'outbox-publisher',
        uptime: process.uptime(),
      })
    );
    return;
  }

  // Readiness check: confirms database and kafka producer are connected
  if (req.url === '/ready' || req.url === '/readiness') {
    if (isShuttingDown) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'shutting_down' }));
      return;
    }

    const kafkaOk = producer ? producer.isConnected() : false;
    let postgresOk = false;

    try {
      await prisma.$queryRaw`SELECT 1`;
      postgresOk = true;
    } catch {
      postgresOk = false;
    }

    const isReady = kafkaOk && postgresOk;
    const statusCode = isReady ? 200 : 503;

    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.end(
      JSON.stringify({
        status: isReady ? 'ok' : 'degraded',
        service: 'outbox-publisher',
        postgres: postgresOk ? 'healthy' : 'unhealthy',
        kafka: kafkaOk ? 'connected' : 'disconnected',
        uptime: process.uptime(),
      })
    );
    return;
  }

  res.writeHead(404);
  res.end();
});

async function connectKafkaWithRetry(
  kafkaProducer: KafkaProducer,
  maxRetries = 10,
  delayMs = 3000
): Promise<void> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      logger.info({ attempt, maxRetries }, 'Attempting Kafka connection');
      await kafkaProducer.connect();
      return;
    } catch (err) {
      logger.warn(
        { attempt, maxRetries, error: err },
        `Kafka connection attempt ${attempt} failed. Retrying in ${delayMs}ms...`
      );
      if (attempt === maxRetries) {
        throw new Error(`Failed to connect to Kafka after ${maxRetries} attempts`);
      }
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

async function main(): Promise<void> {
  const config = getConfig();

  const brokers = config.KAFKA_BROKERS.split(',').map((b: string) => b.trim());
  const clientId = `${config.KAFKA_CLIENT_ID}-outbox-publisher`;

  logger.info({ brokers, clientId }, 'Initializing Outbox Publisher process');

  producer = new KafkaProducer({
    brokers,
    clientId,
  });

  // Resilient connection with retries for container boot sequence
  await connectKafkaWithRetry(producer);

  publisher = new OutboxPublisher(producer, {
    batchSize: 20,
    pollIntervalMs: 500, // 500ms polling for responsive processing
    maxAttempts: 10,
  });

  await publisher.start();

  server.listen(HEALTH_PORT, () => {
    logger.info({ port: HEALTH_PORT }, 'Outbox Publisher health server listening');
  });
}

// Graceful Shutdown Handler
async function gracefulShutdown(signal: string): Promise<void> {
  if (isShuttingDown) {
    return;
  }
  isShuttingDown = true;
  logger.info({ signal }, 'Received termination signal. Starting graceful shutdown...');

  try {
    server.close();

    if (publisher) {
      await publisher.stop();
    }

    if (producer) {
      await producer.disconnect();
    }

    await prisma.$disconnect();
    logger.info('Graceful shutdown completed successfully. Exiting.');
    process.exit(0);
  } catch (error) {
    logger.error({ error }, 'Error during graceful shutdown. Forcing exit.');
    process.exit(1);
  }
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

main().catch((err) => {
  logger.fatal({ error: err }, 'Outbox Publisher fatal startup error');
  process.exit(1);
});
