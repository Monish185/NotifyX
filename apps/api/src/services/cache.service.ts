import { Redis } from 'ioredis';
import { getConfig } from '@notifyx/config';
import { createLogger } from '@notifyx/logger';

const logger = createLogger({ name: 'cache-service' });

let redisClient: Redis | null = null;
let isConnected = false;

function getRedisClient(): Redis | null {
  if (redisClient) return redisClient;

  try {
    const config = getConfig();
    const redisUrl = config.REDIS_URL || process.env.REDIS_URL || 'redis://localhost:6379';

    redisClient = new Redis(redisUrl, {
      maxRetriesPerRequest: 1,
      connectTimeout: 2000,
      lazyConnect: true,
      enableOfflineQueue: false,
    });

    redisClient.on('connect', () => {
      isConnected = true;
      logger.info('Connected to Redis for non-authoritative caching');
    });

    redisClient.on('error', (err) => {
      isConnected = false;
      logger.warn({ error: err.message }, 'Redis caching connection warning; falling back to PostgreSQL');
    });

    redisClient.on('close', () => {
      isConnected = false;
    });

    // Initiate connection without blocking
    redisClient.connect().catch((err) => {
      logger.warn({ error: err.message }, 'Initial Redis connection failed; caching disabled, using direct PostgreSQL');
    });

    return redisClient;
  } catch (err: any) {
    logger.warn({ error: err.message }, 'Could not initialize Redis client; caching disabled');
    return null;
  }
}

const TEMPLATE_TTL_SECONDS = 300; // 5 minutes
const PREFERENCE_TTL_SECONDS = 300; // 5 minutes

export class CacheService {
  async getCachedTemplate(tenantId: string, key: string): Promise<any | null> {
    try {
      const client = getRedisClient();
      if (!client || !isConnected) return null;
      const cached = await client.get(`nx:tpl:${tenantId}:${key}`);
      return cached ? JSON.parse(cached) : null;
    } catch {
      return null;
    }
  }

  async setCachedTemplate(tenantId: string, key: string, value: any): Promise<void> {
    try {
      const client = getRedisClient();
      if (!client || !isConnected) return;
      await client.set(`nx:tpl:${tenantId}:${key}`, JSON.stringify(value), 'EX', TEMPLATE_TTL_SECONDS);
    } catch {
      // Non-authoritative: ignore caching errors
    }
  }

  async invalidateTemplate(tenantId: string, key: string): Promise<void> {
    try {
      const client = getRedisClient();
      if (!client || !isConnected) return;
      await client.del(`nx:tpl:${tenantId}:${key}`);
    } catch {
      // Non-authoritative: ignore cache invalidation errors
    }
  }

  async getCachedPreferences(tenantId: string, userId: string): Promise<any | null> {
    try {
      const client = getRedisClient();
      if (!client || !isConnected) return null;
      const cached = await client.get(`nx:pref:${tenantId}:${userId}`);
      return cached ? JSON.parse(cached) : null;
    } catch {
      return null;
    }
  }

  async setCachedPreferences(tenantId: string, userId: string, value: any): Promise<void> {
    try {
      const client = getRedisClient();
      if (!client || !isConnected) return;
      await client.set(`nx:pref:${tenantId}:${userId}`, JSON.stringify(value), 'EX', PREFERENCE_TTL_SECONDS);
    } catch {
      // Non-authoritative
    }
  }

  async invalidatePreferences(tenantId: string, userId: string): Promise<void> {
    try {
      const client = getRedisClient();
      if (!client || !isConnected) return;
      await client.del(`nx:pref:${tenantId}:${userId}`);
    } catch {
      // Non-authoritative
    }
  }

  async isHealthy(): Promise<boolean> {
    try {
      const client = getRedisClient();
      if (!client || !isConnected) return false;
      const ping = await client.ping();
      return ping === 'PONG';
    } catch {
      return false;
    }
  }
}

export const cacheService = new CacheService();
