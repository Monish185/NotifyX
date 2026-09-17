import { Redis } from 'ioredis';
import { getConfig } from '@notifyx/config';
import { logger } from '@notifyx/logger';

let redisInstance: Redis | null = null;
let isConnected = false;

export function getRateLimitRedis(): Redis | null {
  if (redisInstance) {
    return redisInstance;
  }

  try {
    const config = getConfig();
    const redisUrl = config.REDIS_URL || process.env.REDIS_URL || 'redis://localhost:6379';

    redisInstance = new Redis(redisUrl, {
      maxRetriesPerRequest: 1,
      connectTimeout: 5000,
      enableOfflineQueue: true,
    });

    redisInstance.on('connect', () => {
      isConnected = true;
      logger.info('Rate limiter connected to Redis');
    });

    redisInstance.on('error', (err) => {
      isConnected = false;
      logger.warn({ error: err.message }, 'Rate limiter Redis connection error');
    });

    redisInstance.on('close', () => {
      isConnected = false;
    });

    return redisInstance;
  } catch (err: any) {
    logger.warn({ error: err.message }, 'Failed to initialize rate limiter Redis instance');
    return null;
  }
}

export function isRedisHealthy(): boolean {
  return isConnected && redisInstance !== null && redisInstance.status === 'ready';
}

export async function disconnectRateLimitRedis(): Promise<void> {
  if (redisInstance) {
    try {
      await redisInstance.quit();
    } catch {
      redisInstance.disconnect();
    }
    redisInstance = null;
    isConnected = false;
  }
}
