import { loadConfig } from '@notifyx/config';
import { logger } from '@notifyx/logger';
import { BaseChannelWorker } from '@notifyx/kafka';
import { Channel } from '@notifyx/shared';
import { validateSmsPayload, resolveSmsRecipient } from './schemas.js';
import { createSmsProvider } from './provider-factory.js';

const config = loadConfig();

const brokers = config.KAFKA_BROKERS.split(',').map((b) => b.trim());
const healthPort = Number(process.env.SMS_WORKER_PORT || config.SMS_WORKER_PORT || 3006);
const consumerGroup = process.env.KAFKA_GROUP_ID || 'notifyx-sms-workers';

// Initialize Provider via Factory
const provider = createSmsProvider(config);

export const smsWorker = new BaseChannelWorker({
  channel: Channel.SMS,
  serviceName: 'sms-worker',
  consumerGroup,
  brokers,
  clientId: 'notifyx-sms-worker',
  healthPort,
  provider,
  concurrency: config.SMS_WORKER_CONCURRENCY,
  validatePayload: validateSmsPayload,
  resolveRecipient: resolveSmsRecipient,
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
      service: 'sms-worker',
      brokers,
      consumerGroup,
      healthPort,
      provider: provider.name,
    },
    'Initializing SMS Worker process'
  );

  let retries = 10;
  while (retries > 0) {
    try {
      await smsWorker.start();
      break;
    } catch (err: any) {
      retries--;
      logger.warn(
        { err: err.message, remainingRetries: retries },
        'Failed to connect SMS Worker to Kafka. Retrying in 3s...'
      );
      if (retries === 0) {
        logger.fatal('Could not connect SMS Worker to Kafka after maximum retries. Exiting.');
        process.exit(1);
      }
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
  }

  const shutdown = async (signal: string) => {
    logger.info({ signal }, 'Received shutdown signal for SMS Worker');
    await smsWorker.stop();
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

if (process.env.NODE_ENV !== 'test') {
  main().catch((err) => {
    logger.fatal({ err }, 'Fatal error during SMS Worker bootstrap');
    process.exit(1);
  });
}
