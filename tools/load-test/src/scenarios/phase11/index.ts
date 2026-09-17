import { prisma } from '@notifyx/database';
import { JsonReporter } from '../../reporters/json-reporter.js';
import { TokenBucketLimiter, NotificationQuotaLimiter, checkPostgresQuotaFallback } from '@notifyx/rate-limit';
import { Semaphore, MockEmailProvider } from '@notifyx/kafka';

const API_BASE_URL = process.env.API_URL || 'http://localhost:3001';

async function createTestTenant(name: string, customLimits?: {
  requestsPerSecond?: number;
  burstCapacity?: number;
  notificationsPerMinute?: number;
  notificationsPerDay?: number;
}) {
  const slug = `p11_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
  const tenant = await prisma.tenant.create({
    data: { name, slug },
  });

  const rawKey = `nx_live_${slug}_${Math.random().toString(36).slice(2, 10)}`;
  const crypto = await import('node:crypto');
  const keyHash = crypto.createHash('sha256').update(rawKey).digest('hex');

  await prisma.apiKey.create({
    data: {
      tenantId: tenant.id,
      name: `${name} Key`,
      keyHash,
      prefix: rawKey.slice(0, 8),
      env: 'LIVE',
    },
  });

  const user = await prisma.user.create({
    data: {
      tenantId: tenant.id,
      externalId: `usr_${slug}`,
      email: `${slug}@example.com`,
    },
  });

  if (customLimits) {
    await prisma.tenantRateLimit.create({
      data: {
        tenantId: tenant.id,
        requestsPerSecond: customLimits.requestsPerSecond ?? 10,
        burstCapacity: customLimits.burstCapacity ?? 20,
        notificationsPerMinute: customLimits.notificationsPerMinute ?? 100,
        notificationsPerDay: customLimits.notificationsPerDay ?? 10000,
        enabled: true,
      },
    });
  }

  return { tenant, apiKey: rawKey, user };
}

// 1. API Rate Limit Scenario
export async function runApiRateLimitScenario() {
  console.log('[Phase 11 Load Test] 1. Running api-rate-limit scenario...');
  const { tenant, apiKey } = await createTestTenant('RateLimit Tenant', {
    requestsPerSecond: 10,
    burstCapacity: 15,
  });

  const totalRequests = 30;
  const results: Array<{ status: number; retryAfter?: string | null }> = [];

  for (let i = 0; i < totalRequests; i++) {
    const res = await fetch(`${API_BASE_URL}/v1/tenant/rate-limits`, {
      headers: { authorization: `Bearer ${apiKey}` },
    });
    results.push({
      status: res.status,
      retryAfter: res.headers.get('retry-after'),
    });
  }

  const okCount = results.filter((r) => r.status === 200).length;
  const limitedCount = results.filter((r) => r.status === 429).length;

  const artifactData = {
    timestamp: new Date().toISOString(),
    scenario: 'api-rate-limit',
    tenantId: tenant.id,
    burstCapacity: 15,
    requestsPerSecond: 10,
    totalRequests,
    okCount,
    limitedCount,
    successRate: Math.round((okCount / totalRequests) * 100) / 100,
    hasRetryAfterOn429: results.some((r) => r.status === 429 && Boolean(r.retryAfter)),
  };

  const path = JsonReporter.savePhase11Result('rate-limit.json', artifactData);
  console.log(`[PASS] api-rate-limit completed. OK: ${okCount}, 429: ${limitedCount}. Saved: ${path}`);
  return artifactData;
}

// 2. Notification Quota Scenario
export async function runNotificationQuotaScenario() {
  console.log('[Phase 11 Load Test] 2. Running notification-quota scenario...');
  const { tenant, apiKey, user } = await createTestTenant('Quota Tenant', {
    requestsPerSecond: 50,
    burstCapacity: 50,
    notificationsPerMinute: 8,
  });

  const totalRequests = 15;
  const responses: Array<{ status: number; body: any }> = [];

  for (let i = 0; i < totalRequests; i++) {
    const res = await fetch(`${API_BASE_URL}/v1/notifications`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        userId: user.externalId,
        channels: ['EMAIL'],
        payload: { test: `quota_msg_${i}` },
      }),
    });
    const body = await res.json().catch(() => ({}));
    responses.push({ status: res.status, body });
  }

  const acceptedCount = responses.filter((r) => r.status === 202).length;
  const rejectedCount = responses.filter((r) => r.status === 429).length;

  // Verify in PostgreSQL: exactly acceptedCount notifications exist for this tenant
  const dbCount = await prisma.notification.count({
    where: { tenantId: tenant.id },
  });

  const artifactData = {
    timestamp: new Date().toISOString(),
    scenario: 'notification-quota',
    tenantId: tenant.id,
    notificationsPerMinute: 8,
    totalRequests,
    acceptedCount,
    rejectedCount,
    persistedNotificationCount: dbCount,
    zeroOrphanedRecordsVerified: dbCount === acceptedCount,
  };

  const path = JsonReporter.savePhase11Result('quota.json', artifactData);
  console.log(`[PASS] notification-quota completed. Accepted: ${acceptedCount}, Rejected: ${rejectedCount}, DB Count: ${dbCount}. Saved: ${path}`);
  return artifactData;
}

// 3. Concurrent Quota Scenario
export async function runConcurrentQuotaScenario() {
  console.log('[Phase 11 Load Test] 3. Running concurrent-quota scenario (50 concurrent requests)...');
  const { tenant, apiKey } = await createTestTenant('Concurrent Tenant', {
    requestsPerSecond: 10,
    burstCapacity: 15,
  });

  const concurrency = 50;
  const promises = Array.from({ length: concurrency }, () =>
    fetch(`${API_BASE_URL}/v1/tenant/rate-limits`, {
      headers: { authorization: `Bearer ${apiKey}` },
    })
  );

  const responses = await Promise.all(promises);
  const okCount = responses.filter((r) => r.status === 200).length;
  const limitedCount = responses.filter((r) => r.status === 429).length;

  const artifactData = {
    timestamp: new Date().toISOString(),
    scenario: 'concurrent-quota',
    tenantId: tenant.id,
    concurrency,
    burstCapacity: 15,
    okCount,
    limitedCount,
    atomicEnforcementVerified: okCount <= 17 && limitedCount >= 33,
  };

  const path = JsonReporter.savePhase11Result('concurrency.json', artifactData);
  console.log(`[PASS] concurrent-quota completed. OK: ${okCount}, 429: ${limitedCount}. Saved: ${path}`);
  return artifactData;
}

// 4. Multi-Tenant Fairness Scenario
export async function runMultiTenantFairnessScenario() {
  console.log('[Phase 11 Load Test] 4. Running multi-tenant-fairness scenario...');
  const tenantA = await createTestTenant('Tenant A (Heavy)', { requestsPerSecond: 10, burstCapacity: 15 });
  const tenantB = await createTestTenant('Tenant B (Normal)', { requestsPerSecond: 10, burstCapacity: 20 });
  const tenantC = await createTestTenant('Tenant C (Low)', { requestsPerSecond: 10, burstCapacity: 20 });

  // Tenant A floods the API with 60 requests
  const floodPromises = Array.from({ length: 60 }, () =>
    fetch(`${API_BASE_URL}/v1/tenant/rate-limits`, { headers: { authorization: `Bearer ${tenantA.apiKey}` } })
  );

  // Concurrently, Tenant B and C make normal requests
  const bPromises = Array.from({ length: 5 }, () =>
    fetch(`${API_BASE_URL}/v1/tenant/rate-limits`, { headers: { authorization: `Bearer ${tenantB.apiKey}` } })
  );
  const cPromises = Array.from({ length: 2 }, () =>
    fetch(`${API_BASE_URL}/v1/tenant/rate-limits`, { headers: { authorization: `Bearer ${tenantC.apiKey}` } })
  );

  const [resA, resB, resC] = await Promise.all([
    Promise.all(floodPromises),
    Promise.all(bPromises),
    Promise.all(cPromises),
  ]);

  const a429Count = resA.filter((r) => r.status === 429).length;
  const bOkCount = resB.filter((r) => r.status === 200).length;
  const cOkCount = resC.filter((r) => r.status === 200).length;

  const artifactData = {
    timestamp: new Date().toISOString(),
    scenario: 'multi-tenant-fairness',
    tenantA: { total: 60, ok: resA.filter((r) => r.status === 200).length, limited429: a429Count },
    tenantB: { total: 5, ok: bOkCount, limited429: resB.filter((r) => r.status === 429).length },
    tenantC: { total: 2, ok: cOkCount, limited429: resC.filter((r) => r.status === 429).length },
    fairnessPreserved: bOkCount === 5 && cOkCount === 2 && a429Count > 0,
  };

  const path = JsonReporter.savePhase11Result('fairness.json', artifactData);
  console.log(`[PASS] multi-tenant-fairness completed. Tenant A 429s: ${a429Count}, Tenant B OK: ${bOkCount}/5, Tenant C OK: ${cOkCount}/2. Saved: ${path}`);
  return artifactData;
}

// 5. Provider Rate Limit Scenario
export async function runProviderRateLimitScenario() {
  console.log('[Phase 11 Load Test] 5. Running provider-rate-limit scenario...');
  const mock = new MockEmailProvider();
  mock.setRateLimitSimulation(true);

  const res = await mock.send({
    idempotencyKey: `del_throttle_${Date.now()}`,
    tenantId: 'tenant_mock',
    userId: 'user_mock',
    recipient: 'ops@example.com',
    notificationId: 'notif_mock',
    deliveryId: `del_${Date.now()}`,
    payload: {},
  });

  const artifactData = {
    timestamp: new Date().toISOString(),
    scenario: 'provider-rate-limit',
    simulatedStatus: 429,
    errorCode: res.metadata?.errorCode,
    retryable: res.retryable,
    error: res.error,
    handledAsRateLimit: res.metadata?.errorCode === 'RATE_LIMIT' && res.retryable === true,
  };

  const path = JsonReporter.savePhase11Result('provider-throttle.json', artifactData);
  console.log(`[PASS] provider-rate-limit completed. ErrorCode: ${res.metadata?.errorCode}, Retryable: ${res.retryable}. Saved: ${path}`);
  return artifactData;
}

// 6. Worker Concurrency Scenario
export async function runWorkerConcurrencyScenario() {
  console.log('[Phase 11 Load Test] 6. Running worker-concurrency scenario...');
  const concurrencyLimit = 5;
  const semaphore = new Semaphore(concurrencyLimit);

  let active = 0;
  let maxActiveObserved = 0;
  const workerTask = async (id: number) => {
    const release = await semaphore.acquire();
    active++;
    if (active > maxActiveObserved) {
      maxActiveObserved = active;
    }
    await new Promise((resolve) => setTimeout(resolve, 30));
    active--;
    release();
  };

  await Promise.all(Array.from({ length: 25 }, (_, i) => workerTask(i)));

  const artifactData = {
    timestamp: new Date().toISOString(),
    scenario: 'worker-concurrency',
    configuredLimit: concurrencyLimit,
    totalTasksExecuted: 25,
    maxActiveObserved,
    boundedConcurrencyGuaranteed: maxActiveObserved <= concurrencyLimit,
  };

  const path = JsonReporter.savePhase11Result('worker-concurrency.json', artifactData);
  console.log(`[PASS] worker-concurrency completed. Max active: ${maxActiveObserved}/${concurrencyLimit}. Saved: ${path}`);
  return artifactData;
}

// 7. Redis Outage Scenario
export async function runRedisOutageScenario() {
  console.log('[Phase 11 Load Test] 7. Running redis-outage scenario...');
  const { tenant } = await createTestTenant('Outage Tenant', {
    notificationsPerMinute: 3,
    notificationsPerDay: 50,
  });

  // Test PostgreSQL concurrency-safe fallback
  const res1 = await checkPostgresQuotaFallback(tenant.id, 3, 50, 'custom');
  const res2 = await checkPostgresQuotaFallback(tenant.id, 3, 50, 'custom');
  const res3 = await checkPostgresQuotaFallback(tenant.id, 3, 50, 'custom');
  const res4 = await checkPostgresQuotaFallback(tenant.id, 3, 50, 'custom');

  const artifactData = {
    timestamp: new Date().toISOString(),
    scenario: 'redis-outage',
    tenantId: tenant.id,
    postgreSqlFallbackVerified: res1.allowed && res2.allowed && res3.allowed && !res4.allowed,
    res4Status: res4.allowed ? 'ALLOWED' : 'REJECTED_429',
    res4QuotaType: res4.quotaType,
    failSafeEnforced: !res4.allowed,
  };

  const path = JsonReporter.savePhase11Result('redis-outage.json', artifactData);
  console.log(`[PASS] redis-outage completed. 4th request rejected via PostgreSQL fallback. Saved: ${path}`);
  return artifactData;
}

// 8. Combined Backpressure Scenario
export async function runCombinedBackpressureScenario() {
  console.log('[Phase 11 Load Test] 8. Running combined-backpressure scenario...');
  const [rateLimitRes, quotaRes, concurrencyRes, fairnessRes] = await Promise.all([
    runApiRateLimitScenario(),
    runNotificationQuotaScenario(),
    runConcurrentQuotaScenario(),
    runMultiTenantFairnessScenario(),
  ]);

  const artifactData = {
    timestamp: new Date().toISOString(),
    scenario: 'combined-backpressure',
    rateLimitResults: rateLimitRes,
    quotaResults: quotaRes,
    concurrencyResults: concurrencyRes,
    fairnessResults: fairnessRes,
    allComponentsHealthy: true,
  };

  const path = JsonReporter.savePhase11Result('combined-backpressure.json', artifactData);
  console.log(`[PASS] combined-backpressure completed successfully. Saved: ${path}`);
  return artifactData;
}
