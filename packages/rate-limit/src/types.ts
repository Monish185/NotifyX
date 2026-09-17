export interface TenantRateLimitSettings {
  requestsPerSecond: number;
  burstCapacity: number;
  notificationsPerMinute: number;
  notificationsPerDay: number;
  enabled: boolean;
}

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  retryAfterMs: number;
  limit: number;
}

export interface QuotaResult {
  allowed: boolean;
  remaining: number;
  retryAfterMs: number;
  limit: number;
  quotaType: 'minute' | 'day' | 'none';
  reason?: string;
}

export const DEFAULT_TENANT_RATE_LIMITS: TenantRateLimitSettings = {
  requestsPerSecond: 10,
  burstCapacity: 20,
  notificationsPerMinute: 100,
  notificationsPerDay: 10000,
  enabled: true,
};
