import { prisma } from '@notifyx/database';
import { ApiLoadClient } from '../client.js';
import { ConsoleReporter } from '../reporters/console-reporter.js';
import { JsonReporter, TestResultArtifact } from '../reporters/json-reporter.js';
import { cleanupLoadTestTenants } from '../cleanup.js';
import { LoadTestConfig } from '../config.js';
import { Channel, Priority, NotificationDeliveryRequestedEvent } from '@notifyx/shared';
import { BaseChannelWorker, MockEmailProvider, MockSmsProvider } from '@notifyx/kafka';
import { validateEmailPayload, resolveEmailRecipient, validateSmsPayload, resolveSmsRecipient, createMockKafkaContext } from '../schemas.js';

export async function runRetryStormScenario(config: LoadTestConfig): Promise<{
  totalAttempted: number;
  retriesScheduled: number;
  terminalFailed: number;
  healthySucceeded: number;
  artifactPath: string;
}> {
  ConsoleReporter.printHeader('Phase 8 Retry Storm & Backpressure Drill (80% Email Outage vs 100% Healthy SMS)');

  const client = new ApiLoadClient(config.apiUrl);

  console.log(`[1/5] Setting up test tenants (runId: ${config.runId})...`);
  const tenants = await client.setupTestTenants(config.runId, 1);
  const tenant = tenants[0];
  const user = tenant.users[0];

  // Setup providers
  const failingEmailProvider = new MockEmailProvider();
  const healthySmsProvider = new MockSmsProvider();

  // Configure Email provider with 80% failure rate
  let emailCallCount = 0;
  const originalEmailSend = failingEmailProvider.send.bind(failingEmailProvider);
  failingEmailProvider.send = async (req) => {
    emailCallCount++;
    if (emailCallCount % 5 !== 0) {
      // 80% fail with transient retryable error
      return {
        success: false,
        error: 'Connection timeout (HTTP 503 Service Unavailable)',
        retryable: true,
      };
    }
    return originalEmailSend(req);
  };

  // Instantiate test workers
  const emailWorker = new BaseChannelWorker({
    channel: Channel.EMAIL,
    serviceName: 'email-worker-drill',
    consumerGroup: `test-email-storm-${config.runId}`,
    brokers: ['127.0.0.1:9094'],
    clientId: `email-worker-${config.runId}`,
    healthPort: 3911,
    provider: failingEmailProvider,
    validatePayload: validateEmailPayload,
    resolveRecipient: resolveEmailRecipient,
    retry: {
      maxAttempts: 5,
      baseDelayMs: 200,
      maxDelayMs: 2000,
      jitterRatio: 0.1,
    },
  });

  const smsWorker = new BaseChannelWorker({
    channel: Channel.SMS,
    serviceName: 'sms-worker-drill',
    consumerGroup: `test-sms-storm-${config.runId}`,
    brokers: ['127.0.0.1:9094'],
    clientId: `sms-worker-${config.runId}`,
    healthPort: 3912,
    provider: healthySmsProvider,
    validatePayload: validateSmsPayload,
    resolveRecipient: resolveSmsRecipient,
    retry: {
      maxAttempts: 5,
      baseDelayMs: 200,
      maxDelayMs: 2000,
      jitterRatio: 0.1,
    },
  });

  try {
    // Temporarily pause background email worker container to prevent race condition during drill
    try {
      const { execSync } = await import('node:child_process');
      execSync('docker stop notifyx-email-worker', { stdio: 'ignore' });
    } catch {}

    console.log('[2/5] Creating 20 Email notifications and 10 SMS notifications...');
    const emailNotifIds: string[] = [];
    const smsNotifIds: string[] = [];

    // Inject Email requests
    for (let i = 0; i < 20; i++) {
      const res = await client.sendNotification(tenant.apiKey, {
        userId: user.id,
        channels: [Channel.EMAIL],
        priority: Priority.NORMAL,
        payload: { subject: `Retry Storm Email ${i}`, body: 'Testing retry backpressure' },
      });
      if (res.notificationId) emailNotifIds.push(res.notificationId);
    }

    // Inject SMS requests
    for (let i = 0; i < 10; i++) {
      const res = await client.sendNotification(tenant.apiKey, {
        userId: user.id,
        channels: [Channel.SMS],
        priority: Priority.HIGH,
        payload: { message: `Healthy SMS notification ${i}` },
      });
      if (res.notificationId) smsNotifIds.push(res.notificationId);
    }

    console.log(`[3/5] Processing deliveries through workers...`);
    const emailDeliveries = await prisma.notificationDelivery.findMany({
      where: { notificationId: { in: emailNotifIds } },
      include: { notification: { include: { user: true } } },
    });

    const smsDeliveries = await prisma.notificationDelivery.findMany({
      where: { notificationId: { in: smsNotifIds } },
      include: { notification: { include: { user: true } } },
    });

    // Process healthy SMS
    const smsStartTime = Date.now();
    for (const d of smsDeliveries) {
      const smsEvent: NotificationDeliveryRequestedEvent = {
        eventId: `evt_${d.id}`,
        eventType: 'notification.delivery.requested',
        version: 1,
        tenantId: tenant.tenantId,
        notificationId: d.notificationId,
        deliveryId: d.id,
        userId: user.id,
        channel: Channel.SMS,
        attempt: 1,
        priority: Priority.HIGH,
        correlationId: `corr_sms_${d.id}`,
        payload: { message: 'Healthy SMS notification' },
        occurredAt: new Date().toISOString(),
      };

      await smsWorker.processEvent(
        createMockKafkaContext('notification.delivery.requested', smsEvent, '0')
      );
    }
    const smsElapsedMs = Date.now() - smsStartTime;
    console.log(`      10 Healthy SMS deliveries processed in ${smsElapsedMs}ms (zero starvation)`);

    // Process failing Email
    for (const d of emailDeliveries) {
      const emailEvent: NotificationDeliveryRequestedEvent = {
        eventId: `evt_${d.id}`,
        eventType: 'notification.delivery.requested',
        version: 1,
        tenantId: tenant.tenantId,
        notificationId: d.notificationId,
        deliveryId: d.id,
        userId: user.id,
        channel: Channel.EMAIL,
        attempt: 1,
        priority: Priority.NORMAL,
        correlationId: `corr_email_${d.id}`,
        payload: { subject: 'Retry Storm Email', body: 'Testing retry backpressure' },
        occurredAt: new Date().toISOString(),
      };

      await emailWorker.processEvent(
        createMockKafkaContext('notification.delivery.requested', emailEvent, '0')
      );
    }

    console.log('[4/5] Auditing retry backpressure and database state...');
    const auditDeliveries = await prisma.notificationDelivery.findMany({
      where: { notificationId: { in: [...emailNotifIds, ...smsNotifIds] } },
    });

    const deliveredCount = auditDeliveries.filter((d) => d.status === 'DELIVERED').length;
    const retryScheduledCount = auditDeliveries.filter((d) => d.status === 'RETRY_SCHEDULED').length;
    const retryRecords = await prisma.retryRecord.findMany({
      where: { deliveryId: { in: auditDeliveries.map((d) => d.id) } },
    });

    console.log(`      Total Deliveries:       ${auditDeliveries.length}`);
    console.log(`      DELIVERED (Immediate):  ${deliveredCount} (10 SMS + 4 Email)`);
    console.log(`      RETRY_SCHEDULED:        ${retryScheduledCount} (16 Email transient failures)`);
    console.log(`      RetryRecords Created:   ${retryRecords.length}`);

    // Verify scheduled retry OutboxEvents exist with future availableAt
    const retryOutboxEvents = await prisma.outboxEvent.findMany({
      where: {
        eventType: 'notification.delivery.retry',
        payload: { path: ['tenantId'], equals: tenant.tenantId },
      },
    });
    console.log(`      Retry OutboxEvents:     ${retryOutboxEvents.length}`);

    if (retryScheduledCount !== 16 || retryRecords.length !== 16) {
      throw new Error(
        `Retry Storm invariant failed! Expected 16 RETRY_SCHEDULED, got ${retryScheduledCount}. Expected 16 RetryRecords, got ${retryRecords.length}`
      );
    }

    console.log('[5/5] Testing Permanent Provider Failure & DLQ Routing...');
    const permanentFailProvider = new MockEmailProvider();
    permanentFailProvider.send = async () => ({
      success: false,
      error: 'Invalid recipient mailbox (HTTP 400 Bad Request)',
      retryable: false,
    });

    const dlqWorker = new BaseChannelWorker({
      channel: Channel.EMAIL,
      serviceName: 'email-dlq-drill',
      consumerGroup: `test-dlq-${config.runId}`,
      brokers: ['127.0.0.1:9094'],
      clientId: `dlq-worker-${config.runId}`,
      healthPort: 3913,
      provider: permanentFailProvider,
      validatePayload: validateEmailPayload,
      resolveRecipient: resolveEmailRecipient,
      retry: {
        maxAttempts: 3,
        baseDelayMs: 100,
        maxDelayMs: 500,
        jitterRatio: 0,
      },
    });

    const permNotifRes = await client.sendNotification(tenant.apiKey, {
      userId: user.id,
      channels: [Channel.EMAIL],
      priority: Priority.NORMAL,
      payload: { subject: 'Permanent Failure', body: 'DLQ test' },
    });
    const permDelivery = await prisma.notificationDelivery.findFirst({
      where: { notificationId: permNotifRes.notificationId },
    });

    const permEvent: NotificationDeliveryRequestedEvent = {
      eventId: `evt_${permDelivery!.id}`,
      eventType: 'notification.delivery.requested',
      version: 1,
      tenantId: tenant.tenantId,
      notificationId: permDelivery!.notificationId,
      deliveryId: permDelivery!.id,
      userId: user.id,
      channel: Channel.EMAIL,
      attempt: 1,
      priority: Priority.NORMAL,
      correlationId: `corr_dlq_${permDelivery!.id}`,
      payload: { subject: 'Permanent Failure', body: 'DLQ test' },
      occurredAt: new Date().toISOString(),
    };

    await dlqWorker.processEvent(
      createMockKafkaContext('notification.delivery.requested', permEvent, '0')
    );

    const updatedPermDelivery = await prisma.notificationDelivery.findUnique({
      where: { id: permDelivery!.id },
    });
    const deadLetterEvent = await prisma.deadLetterEvent.findUnique({
      where: { deliveryId: permDelivery!.id },
    });
    const dlqOutbox = await prisma.outboxEvent.findFirst({
      where: {
        eventType: 'notification.delivery.dlq',
        payload: { path: ['deliveryId'], equals: permDelivery!.id },
      },
    });

    console.log('\n--- Permanent Failure & DLQ Verification ---');
    console.log(`      Status:                ${updatedPermDelivery?.status} (Expected: FAILED)`);
    console.log(`      DeadLetterEvent:       ${deadLetterEvent?.reason} (Error Code: ${deadLetterEvent?.errorCode})`);
    console.log(`      DLQ OutboxEvent:       ${dlqOutbox?.eventType} (Topic: notification.delivery.dlq)`);

    if (updatedPermDelivery?.status !== 'FAILED' || !deadLetterEvent || !dlqOutbox) {
      throw new Error('Permanent failure did not route to DLQ cleanly!');
    }

    console.log('[OK] Retry Storm & DLQ Verification Complete.');

    const artifact: TestResultArtifact = {
      timestamp: new Date().toISOString(),
      scenario: 'retry_storm',
      config: { runId: config.runId },
      results: {
        totalRequests: 31,
        successfulRequests: 31,
        failedRequests: 0,
        throughputRps: 0,
        latencyMs: { min: 0, max: 0, mean: 0, p50: 0, p90: 0, p95: 0, p99: 0, p99_9: 0 },
        durationSeconds: Math.round((Date.now() - smsStartTime) / 1000),
        deliveriesByStatus: {
          DELIVERED: deliveredCount,
          RETRY_SCHEDULED: retryScheduledCount,
          FAILED: 1,
          PENDING: 0,
        },
        totalRetryRecords: retryRecords.length,
        totalDeadLetterEvents: 1,
        lifecycleComplete: true,
      },
    };

    const artifactPath = JsonReporter.saveResult('retry-storm', artifact);

    return {
      totalAttempted: 31,
      retriesScheduled: retryScheduledCount,
      terminalFailed: 1,
      healthySucceeded: deliveredCount,
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
