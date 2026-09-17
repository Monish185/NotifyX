import { prisma } from '@notifyx/database';
import { logger } from '@notifyx/logger';
import { getConfig } from '@notifyx/config';
import { InAppWorker } from './worker.js';
import { createHealthServer } from './health.js';

const HEALTH_PORT = process.env.WORKER_HEALTH_PORT
  ? parseInt(process.env.WORKER_HEALTH_PORT, 10)
  : 3003;

let worker: InAppWorker | null = null;
let healthServer: any = null;
let isShuttingDown = false;

async function startWorkerWithRetry(inAppWorker: InAppWorker, maxRetries = 10, delayMs = 3000): Promise<void> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      logger.info({ attempt, maxRetries }, 'Connecting In-App Worker to Kafka');
      await inAppWorker.start();
      return;
    } catch (err) {
      logger.warn(
        { attempt, maxRetries, error: err },
        `In-App Worker Kafka connection attempt ${attempt} failed. Retrying in ${delayMs}ms...`
      );
      if (attempt === maxRetries) {
        throw new Error(`Failed to start In-App Worker after ${maxRetries} attempts`);
      }
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

async function main(): Promise<void> {
  const config = getConfig();

  const brokers = config.KAFKA_BROKERS.split(',').map((b: string) => b.trim());
  const groupId = config.KAFKA_GROUP_ID || 'notifyx-inapp-workers';
  const clientId = `${config.KAFKA_CLIENT_ID}-inapp-worker`;

  logger.info({ brokers, groupId, clientId }, 'Initializing In-App Worker process');

  worker = new InAppWorker({
    brokers,
    groupId,
    clientId,
    concurrency: config.INAPP_WORKER_CONCURRENCY,
  });

  healthServer = createHealthServer(worker, HEALTH_PORT);

  await startWorkerWithRetry(worker);

  logger.info('In-App Notification Worker successfully running');
}

async function gracefulShutdown(signal: string): Promise<void> {
  if (isShuttingDown) {
    return;
  }
  isShuttingDown = true;
  logger.info({ signal }, 'Received termination signal. Gracefully shutting down In-App Worker...');

  try {
    if (healthServer) {
      healthServer.close();
    }

    if (worker) {
      await worker.stop();
    }

    await prisma.$disconnect();
    logger.info('In-App Worker shutdown cleanly completed. Exiting.');
    process.exit(0);
  } catch (error) {
    logger.error({ error }, 'Error during In-App Worker shutdown. Forcing exit.');
    process.exit(1);
  }
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

main().catch((err) => {
  logger.fatal({ error: err }, 'In-App Worker fatal startup error');
  process.exit(1);
});
