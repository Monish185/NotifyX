import { getConfig } from '@notifyx/config';
import { logger } from '@notifyx/logger';
import { buildApp } from './app.js';

async function start() {
  const config = getConfig();
  const app = await buildApp();

  const shutdown = async (signal: string) => {
    logger.info({ signal }, `Received ${signal}, closing server gracefully...`);
    try {
      await app.close();
      logger.info('Server successfully closed.');
      process.exit(0);
    } catch (err) {
      logger.error({ err }, 'Error occurred while closing server');
      process.exit(1);
    }
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  try {
    const address = await app.listen({
      port: config.API_PORT,
      host: '0.0.0.0',
    });
    logger.info(`🚀 NotifyX API Server running at ${address}`);
  } catch (err) {
    logger.error({ err }, 'Failed to start NotifyX API server');
    process.exit(1);
  }
}

void start();
