import { loadConfig } from '@notifyx/config';
import { logger } from '@notifyx/logger';
import { BaseChannelWorker } from '@notifyx/kafka';
import { Channel } from '@notifyx/shared';
import { validatePushPayload, resolvePushRecipient } from './schemas.js';
import { createPushProvider } from './provider-factory.js';

const config = loadConfig();

const brokers = config.KAFKA_BROKERS.split(',').map((b) => b.trim());
const healthPort = Number(process.env.PUSH_WORKER_PORT || config.PUSH_WORKER_PORT || 3005);
const consumerGroup = process.env.KAFKA_GROUP_ID || 'notifyx-push-workers';

// Initialize Provider via Factory
const provider = createPushProvider(config);

export const pushWorker = new BaseChannelWorker({
  channel: Channel.PUSH,
  serviceName: 'push-worker',
  consumerGroup,
  brokers,
  clientId: 'notifyx-push-worker',
  healthPort,
  provider,
  concurrency: config.PUSH_WORKER_CONCURRENCY,
  validatePayload: validatePushPayload,
  resolveRecipient: resolvePushRecipient,
  retry: {
    maxAttempts: config.NOTIFYX_RETRY_MAX_ATTEMPTS,
    baseDelayMs: config.NOTIFYX_RETRY_BASE_DELAY_MS,
    maxDelayMs: config.NOTIFYX_RETRY_MAX_DELAY_MS,
    jitterRatio: config.NOTIFYX_RETRY_JITTER_RATIO,
  },
});

async function main() {
  logger.info(
    {
      service: 'push-worker',
      brokers,
      consumerGroup,
      healthPort,
      provider: provider.name,
    },
    'Initializing Push Worker process'
  );

  let retries = 10;
  while (retries > 0) {
    try {
      await pushWorker.start();
      break;
    } catch (err: any) {
      retries--;
      logger.warn(
        { err: err.message, remainingRetries: retries },
        'Failed to connect Push Worker to Kafka. Retrying in 3s...'
      );
      if (retries === 0) {
        logger.fatal('Could not connect Push Worker to Kafka after maximum retries. Exiting.');
        process.exit(1);
      }
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
  }

  const shutdown = async (signal: string) => {
    logger.info({ signal }, 'Received shutdown signal for Push Worker');
    await pushWorker.stop();
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

if (process.env.NODE_ENV !== 'test') {
  main().catch((err) => {
    logger.fatal({ err }, 'Fatal error during Push Worker bootstrap');
    process.exit(1);
  });
}
