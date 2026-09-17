import { loadConfig } from '@notifyx/config';
import { logger } from '@notifyx/logger';
import {
  BaseChannelWorker,
} from '@notifyx/kafka';
import { Channel } from '@notifyx/shared';
import { validateEmailPayload, resolveEmailRecipient } from './schemas.js';
import { createEmailProvider } from './provider-factory.js';

const config = loadConfig();

const brokers = config.KAFKA_BROKERS.split(',').map((b) => b.trim());
const healthPort = Number(process.env.EMAIL_WORKER_PORT || config.EMAIL_WORKER_PORT || 3004);
const consumerGroup = process.env.KAFKA_GROUP_ID || 'notifyx-email-workers';

// Initialize Provider dynamically via factory
const provider = createEmailProvider(config);

export const emailWorker = new BaseChannelWorker({
  channel: Channel.EMAIL,
  serviceName: 'email-worker',
  consumerGroup,
  brokers,
  clientId: 'notifyx-email-worker',
  healthPort,
  provider,
  concurrency: config.EMAIL_WORKER_CONCURRENCY,
  validatePayload: validateEmailPayload,
  resolveRecipient: resolveEmailRecipient,
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
      service: 'email-worker',
      brokers,
      consumerGroup,
      healthPort,
      provider: provider.name,
    },
    'Initializing Email Worker process'
  );

  let retries = 10;
  while (retries > 0) {
    try {
      await emailWorker.start();
      break;
    } catch (err: any) {
      retries--;
      logger.warn(
        { err: err.message, remainingRetries: retries },
        'Failed to connect Email Worker to Kafka. Retrying in 3s...'
      );
      if (retries === 0) {
        logger.fatal('Could not connect Email Worker to Kafka after maximum retries. Exiting.');
        process.exit(1);
      }
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
  }

  const shutdown = async (signal: string) => {
    logger.info({ signal }, 'Received shutdown signal for Email Worker');
    await emailWorker.stop();
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

if (process.env.NODE_ENV !== 'test') {
  main().catch((err) => {
    logger.fatal({ err }, 'Fatal error during Email Worker bootstrap');
    process.exit(1);
  });
}
