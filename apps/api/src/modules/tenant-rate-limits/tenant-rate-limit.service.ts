import { prisma } from '@notifyx/database';
import {
  type TenantRateLimitSettings,
  DEFAULT_TENANT_RATE_LIMITS,
} from '@notifyx/rate-limit';

export interface TenantRateLimitInfo extends TenantRateLimitSettings {
  id?: string;
  tenantId: string;
  isCustom: boolean;
  createdAt?: Date;
  updatedAt?: Date;
}

// In-process cache with short TTL (30 seconds) to avoid querying DB on every single HTTP request
interface CacheEntry {
  data: TenantRateLimitInfo;
  expiresAt: number;
}
const localConfigCache = new Map<string, CacheEntry>();

export class TenantRateLimitService {
  async getTenantRateLimits(tenantId: string): Promise<TenantRateLimitInfo> {
    const now = Date.now();
    const cached = localConfigCache.get(tenantId);
    if (cached && cached.expiresAt > now) {
      return cached.data;
    }

    const record = await prisma.tenantRateLimit.findUnique({
      where: { tenantId },
    });

    let result: TenantRateLimitInfo;
    if (record) {
      result = {
        id: record.id,
        tenantId: record.tenantId,
        requestsPerSecond: record.requestsPerSecond,
        burstCapacity: record.burstCapacity,
        notificationsPerMinute: record.notificationsPerMinute,
        notificationsPerDay: record.notificationsPerDay,
        enabled: record.enabled,
        isCustom: true,
        createdAt: record.createdAt,
        updatedAt: record.updatedAt,
      };
    } else {
      result = {
        tenantId,
        requestsPerSecond: DEFAULT_TENANT_RATE_LIMITS.requestsPerSecond,
        burstCapacity: DEFAULT_TENANT_RATE_LIMITS.burstCapacity,
        notificationsPerMinute: DEFAULT_TENANT_RATE_LIMITS.notificationsPerMinute,
        notificationsPerDay: DEFAULT_TENANT_RATE_LIMITS.notificationsPerDay,
        enabled: DEFAULT_TENANT_RATE_LIMITS.enabled,
        isCustom: false,
      };
    }

    localConfigCache.set(tenantId, {
      data: result,
      expiresAt: now + 30000, // 30s TTL
    });

    return result;
  }

  async updateTenantRateLimits(
    tenantId: string,
    input: Partial<TenantRateLimitSettings>
  ): Promise<TenantRateLimitInfo> {
    const upserted = await prisma.tenantRateLimit.upsert({
      where: { tenantId },
      update: {
        ...(input.requestsPerSecond !== undefined ? { requestsPerSecond: input.requestsPerSecond } : {}),
        ...(input.burstCapacity !== undefined ? { burstCapacity: input.burstCapacity } : {}),
        ...(input.notificationsPerMinute !== undefined ? { notificationsPerMinute: input.notificationsPerMinute } : {}),
        ...(input.notificationsPerDay !== undefined ? { notificationsPerDay: input.notificationsPerDay } : {}),
        ...(input.enabled !== undefined ? { enabled: input.enabled } : {}),
      },
      create: {
        tenantId,
        requestsPerSecond: input.requestsPerSecond ?? DEFAULT_TENANT_RATE_LIMITS.requestsPerSecond,
        burstCapacity: input.burstCapacity ?? DEFAULT_TENANT_RATE_LIMITS.burstCapacity,
        notificationsPerMinute: input.notificationsPerMinute ?? DEFAULT_TENANT_RATE_LIMITS.notificationsPerMinute,
        notificationsPerDay: input.notificationsPerDay ?? DEFAULT_TENANT_RATE_LIMITS.notificationsPerDay,
        enabled: input.enabled ?? DEFAULT_TENANT_RATE_LIMITS.enabled,
      },
    });

    const result: TenantRateLimitInfo = {
      id: upserted.id,
      tenantId: upserted.tenantId,
      requestsPerSecond: upserted.requestsPerSecond,
      burstCapacity: upserted.burstCapacity,
      notificationsPerMinute: upserted.notificationsPerMinute,
      notificationsPerDay: upserted.notificationsPerDay,
      enabled: upserted.enabled,
      isCustom: true,
      createdAt: upserted.createdAt,
      updatedAt: upserted.updatedAt,
    };

    // Invalidate local cache
    localConfigCache.delete(tenantId);

    return result;
  }

  clearCache(tenantId?: string): void {
    if (tenantId) {
      localConfigCache.delete(tenantId);
    } else {
      localConfigCache.clear();
    }
  }
}

export const tenantRateLimitService = new TenantRateLimitService();
