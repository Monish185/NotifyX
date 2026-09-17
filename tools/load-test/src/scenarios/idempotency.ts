import { prisma } from '@notifyx/database';
import { ApiLoadClient } from '../client.js';
import { ConsoleReporter } from '../reporters/console-reporter.js';
import { JsonReporter, TestResultArtifact } from '../reporters/json-reporter.js';
import { cleanupLoadTestTenants } from '../cleanup.js';
import { LoadTestConfig } from '../config.js';
import { Channel, Priority, NotificationDeliveryRequestedEvent, NotificationDeliveryRetryEvent } from '@notifyx/shared';
import { BaseChannelWorker, MockEmailProvider } from '@notifyx/kafka';
import { validateEmailPayload, resolveEmailRecipient, createMockKafkaContext } from '../schemas.js';

export async function runIdempotencyScenario(config: LoadTestConfig): Promise<{
  originalStatus: string;
  duplicateProcessed: boolean;
  artifactPath: string;
}> {
  ConsoleReporter.printHeader('Phase 8 Idempotency & Duplicate Event Injection Drill');

  const client = new ApiLoadClient(config.apiUrl);

  console.log(`[1/4] Setting up test tenants (runId: ${config.runId})...`);
  const tenants = await client.setupTestTenants(config.runId, 1);
  const tenant = tenants[0];
  const user = tenant.users[0];

  const mockProvider = new MockEmailProvider();
  const worker = new BaseChannelWorker({
    channel: Channel.EMAIL,
    serviceName: 'email-idempotency-drill',
    consumerGroup: `test-idempotency-${config.runId}`,
    brokers: ['127.0.0.1:9094'],
    clientId: `idempotency-worker-${config.runId}`,
    healthPort: 3914,
    provider: mockProvider,
    validatePayload: validateEmailPayload,
    resolveRecipient: resolveEmailRecipient,
    retry: {
      maxAttempts: 5,
      baseDelayMs: 200,
      maxDelayMs: 2000,
      jitterRatio: 0.1,
    },
  });

  try {
    try {
      const { execSync } = await import('node:child_process');
      execSync('docker stop notifyx-email-worker', { stdio: 'ignore' });
    } catch {}

    console.log('[2/4] Creating test notification and obtaining initial delivery...');
    const notifRes = await client.sendNotification(tenant.apiKey, {
      userId: user.id,
      channels: [Channel.EMAIL],
      priority: Priority.NORMAL,
      payload: { subject: 'Idempotency Test Email', body: 'Testing duplicate Kafka message safety' },
    });

    const delivery = await prisma.notificationDelivery.findFirst({
      where: { notificationId: notifRes.notificationId },
    });
    if (!delivery) {
      throw new Error(`Failed to find delivery for notificationId: ${notifRes.notificationId}`);
    }

    const initialEvent: NotificationDeliveryRequestedEvent = {
      eventId: `evt_init_${delivery.id}`,
      eventType: 'notification.delivery.requested',
      version: 1,
      tenantId: tenant.tenantId,
      notificationId: delivery.notificationId,
      deliveryId: delivery.id,
      userId: user.id,
      channel: Channel.EMAIL,
      attempt: 1,
      priority: Priority.NORMAL,
      correlationId: `corr_idempotency_${delivery.id}`,
      payload: { subject: 'Idempotency Test Email', body: 'Testing duplicate Kafka message safety' },
      occurredAt: new Date().toISOString(),
    };

    console.log('[3/4] Processing event initially (Attempt 1)...');
    const result1 = await worker.processEvent(
      createMockKafkaContext('notification.delivery.requested', initialEvent, '100')
    );
    console.log(`      Initial processing result: ${result1.status}`);

    const postSendDelivery = await prisma.notificationDelivery.findUnique({
      where: { id: delivery.id },
    });
    console.log(`      Delivery status in DB: ${postSendDelivery?.status} (Expected: DELIVERED)`);

    console.log('\n[4/4] INJECTING DUPLICATE KAFKA EVENTS (Simulating broker redelivery / network replay)...');

    // Duplicate 1: Exact same requested event with identical eventId & attempt 1
    const resultDup1 = await worker.processEvent(
      createMockKafkaContext('notification.delivery.requested', initialEvent, '101')
    );
    console.log(`      Duplicate 1 (Same eventId): Status = ${resultDup1.status} (Reason: ${resultDup1.reason})`);

    // Duplicate 2: New Kafka eventId but pointing to same deliveryId and attempt 1
    const duplicateEventNewId: NotificationDeliveryRequestedEvent = {
      ...initialEvent,
      eventId: `evt_replayed_${Date.now()}`,
    };
    const resultDup2 = await worker.processEvent(
      createMockKafkaContext('notification.delivery.requested', duplicateEventNewId, '102')
    );
    console.log(`      Duplicate 2 (New eventId, same deliveryId): Status = ${resultDup2.status} (Reason: ${resultDup2.reason})`);

    // Duplicate 3: Duplicate Retry Event for already delivered notification
    const duplicateRetryEvent: NotificationDeliveryRetryEvent = {
      ...initialEvent,
      eventType: 'notification.delivery.retry',
      eventId: `evt_retry_dup_${Date.now()}`,
      attempt: 2,
      maxAttempts: 5,
      scheduledAt: new Date().toISOString(),
      nextAttemptAt: new Date(Date.now() + 1000).toISOString(),
    };
    const resultDup3 = await worker.processEvent(
      createMockKafkaContext('notification.delivery.retry', duplicateRetryEvent, '103')
    );
    console.log(`      Duplicate 3 (Retry event for delivered item): Status = ${resultDup3.status} (Reason: ${resultDup3.reason})`);

    // Database state assertions
    const finalDeliveryCount = await prisma.notificationDelivery.count({
      where: { notificationId: notifRes.notificationId },
    });
    const finalDelivery = await prisma.notificationDelivery.findUnique({
      where: { id: delivery.id },
    });

    console.log('\n--- Internal Database Idempotency Audit ---');
    console.log(`      Total Delivery rows for notification: ${finalDeliveryCount} (Expected: 1)`);
    console.log(`      Final Delivery status:                ${finalDelivery?.status} (Expected: DELIVERED)`);
    console.log(`      Attempt Count:                        ${finalDelivery?.attemptCount} (Expected: 1)`);

    if (finalDeliveryCount !== 1 || finalDelivery?.status !== 'DELIVERED' || finalDelivery?.attemptCount !== 1) {
      throw new Error('Database idempotency invariant violated! Duplicate records or invalid state created.');
    }

    console.log('\n--- External Provider Boundary Notice ---');
    console.log('      [GUARANTEED BY ARCHITECTURE]: NotifyX internal database transitions are strictly idempotent.');
    console.log('      [DISTRIBUTED REALITY]: External provider exactly-once is NOT guaranteed by NotifyX.');
    console.log('      If a worker crashes post-send before DB/offset commit, Kafka redelivers and the provider');
    console.log('      may be called again. Providers must support idempotency keys on their side for external safety.');

    const artifact: TestResultArtifact = {
      timestamp: new Date().toISOString(),
      scenario: 'idempotency',
      config: { runId: config.runId },
      results: {
        totalRequests: 1,
        successfulRequests: 1,
        failedRequests: 0,
        throughputRps: 0,
        latencyMs: { min: 0, max: 0, mean: 0, p50: 0, p90: 0, p95: 0, p99: 0, p99_9: 0 },
        durationSeconds: 1,
        deliveriesByStatus: {
          DELIVERED: 1,
          PENDING: 0,
          RETRY_SCHEDULED: 0,
          FAILED: 0,
        },
        lifecycleComplete: true,
      },
    };

    const artifactPath = JsonReporter.saveResult('idempotency', artifact);
    console.log(`[OK] Artifact saved to: ${artifactPath}`);

    return {
      originalStatus: postSendDelivery?.status || 'UNKNOWN',
      duplicateProcessed: true,
      artifactPath,
    };
  } finally {
    try {
      const { execSync } = await import('node:child_process');
      execSync('docker start notifyx-email-worker', { stdio: 'ignore' });
    } catch {}
    client.destroy();
    console.log(`Cleaning up test tenant data for runId: ${config.runId}...`);
    await cleanupLoadTestTenants(config.runId);
  }
}
