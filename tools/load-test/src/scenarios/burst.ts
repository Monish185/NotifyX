import { ApiLoadClient } from '../client.js';
import { LoadGenerator, LoadRunStats } from '../generator.js';
import { MetricsCollector, DatabaseStateMetrics } from '../metrics-collector.js';
import { ConsoleReporter } from '../reporters/console-reporter.js';
import { JsonReporter, TestResultArtifact } from '../reporters/json-reporter.js';
import { cleanupLoadTestTenants } from '../cleanup.js';
import { LoadTestConfig } from '../config.js';

export async function runBurstScenario(baseConfig: LoadTestConfig): Promise<{
  warmupStats: LoadRunStats;
  burstStats: LoadRunStats;
  cooldownStats: LoadRunStats;
  dbMetrics: DatabaseStateMetrics;
  artifactPath: string;
}> {
  ConsoleReporter.printHeader('Phase 8 Burst Load Test (Warmup -> Burst Spike -> Cooldown)');

  const client = new ApiLoadClient(baseConfig.apiUrl);
  const metricsCollector = new MetricsCollector();

  console.log(`[1/5] Setting up test tenants (runId: ${baseConfig.runId})...`);
  const tenants = await client.setupTestTenants(baseConfig.runId, baseConfig.tenantCount);
  const tenantIds = tenants.map((t) => t.tenantId);

  try {
    // Stage 1: Warmup (e.g. 10 RPS for 10s)
    console.log('\n[2/5] STAGE 1: Warmup (10 RPS, 10s)...');
    const warmupConfig = { ...baseConfig, targetRps: 10, durationSeconds: 10 };
    const warmupGen = new LoadGenerator(warmupConfig, tenants, client);
    const warmupStats = await warmupGen.run();
    console.log(`      Warmup completed: ${warmupStats.successfulRequests} accepted.`);

    // Stage 2: Burst Spike (e.g. 150 RPS or baseConfig.targetRps for 15s)
    const burstRps = Math.max(baseConfig.targetRps, 150);
    console.log(`\n[3/5] STAGE 2: BURST SPIKE (${burstRps} RPS for 15s)...`);
    const burstConfig = { ...baseConfig, targetRps: burstRps, durationSeconds: 15, concurrency: 40 };
    const burstGen = new LoadGenerator(burstConfig, tenants, client);
    const burstStats = await burstGen.run((tick) => {
      process.stdout.write(
        `\r      Burst Ingress: ${tick.count} | 202 OK: ${tick.successes} | Current Rate: ${tick.currentRps} req/s`
      );
    });
    console.log(`\n      Burst spike completed: ${burstStats.successfulRequests} accepted.`);

    // Check peak outbox backlog right after burst
    const peakMetrics = await metricsCollector.collectDatabaseState(tenantIds);
    console.log(`      Peak Pending Outbox Backlog: ${peakMetrics.outboxByStatus.PENDING} events`);

    // Stage 3: Cooldown (e.g. 5 RPS for 20s)
    console.log('\n[4/5] STAGE 3: Cooldown & Queue Drain (5 RPS, 20s)...');
    const cooldownConfig = { ...baseConfig, targetRps: 5, durationSeconds: 20 };
    const cooldownGen = new LoadGenerator(cooldownConfig, tenants, client);
    const cooldownStats = await cooldownGen.run();

    console.log('\n[5/5] Waiting for workers to completely drain remaining backlog...');
    const drainResult = await metricsCollector.waitForDrain(tenantIds, 90000);
    const drainDurationSec = Math.round((drainResult.elapsedMs / 1000) * 10) / 10;

    // Verify Lifecycle Invariant
    const totalAccepted = warmupStats.successfulRequests + burstStats.successfulRequests + cooldownStats.successfulRequests;
    const finalMetrics = drainResult.finalMetrics;

    console.log('\n--- Lifecycle Invariant Audit ---');
    console.log(`Total Accepted (202):      ${totalAccepted}`);
    console.log(`Total DB Deliveries:       ${finalMetrics.totalDeliveries}`);
    console.log(`Delivered Count:           ${finalMetrics.deliveriesByStatus.DELIVERED}`);
    console.log(`Failed Count:              ${finalMetrics.deliveriesByStatus.FAILED}`);
    console.log(`Unresolved In-Flight:      ${finalMetrics.unresolvedCount}`);
    console.log(`Pending Outbox:            ${finalMetrics.outboxByStatus.PENDING}`);

    if (finalMetrics.unresolvedCount > 0 || finalMetrics.outboxByStatus.PENDING > 0) {
      throw new Error(
        `Lifecycle Invariant Violated! After cooldown, ${finalMetrics.unresolvedCount} deliveries remain unresolved and ${finalMetrics.outboxByStatus.PENDING} outbox events remain pending.`
      );
    }

    console.log('[OK] Lifecycle Invariant Confirmed: Every accepted notification reached a terminal state (DELIVERED or FAILED).');

    const combinedStats: LoadRunStats = {
      totalRequests: warmupStats.totalRequests + burstStats.totalRequests + cooldownStats.totalRequests,
      successfulRequests: warmupStats.successfulRequests + burstStats.successfulRequests + cooldownStats.successfulRequests,
      failedRequests: warmupStats.failedRequests + burstStats.failedRequests + cooldownStats.failedRequests,
      statusCodeCounts: { '202': totalAccepted },
      throughputRps: burstStats.throughputRps,
      latencyMs: burstStats.latencyMs,
      durationSeconds: warmupStats.durationSeconds + burstStats.durationSeconds + cooldownStats.durationSeconds,
      generatedNotificationIds: [],
    };

    ConsoleReporter.printSummary('burst', combinedStats, finalMetrics, drainDurationSec);

    const artifact: TestResultArtifact = {
      timestamp: new Date().toISOString(),
      scenario: 'burst',
      config: {
        warmupRps: warmupConfig.targetRps,
        burstRps: burstConfig.targetRps,
        cooldownRps: cooldownConfig.targetRps,
        runId: baseConfig.runId,
      },
      results: {
        totalRequests: combinedStats.totalRequests,
        successfulRequests: combinedStats.successfulRequests,
        failedRequests: combinedStats.failedRequests,
        throughputRps: burstStats.throughputRps,
        latencyMs: burstStats.latencyMs,
        durationSeconds: combinedStats.durationSeconds,
        deliveriesByStatus: finalMetrics.deliveriesByStatus,
        outboxByStatus: finalMetrics.outboxByStatus,
        totalRetryRecords: finalMetrics.totalRetryRecords,
        totalDeadLetterEvents: finalMetrics.totalDeadLetterEvents,
        lifecycleComplete: finalMetrics.lifecycleComplete,
        drainDurationSec,
      },
    };

    const artifactPath = JsonReporter.saveResult('burst', artifact);
    console.log(`[OK] Artifact saved to: ${artifactPath}`);

    return {
      warmupStats,
      burstStats,
      cooldownStats,
      dbMetrics: finalMetrics,
      artifactPath,
    };
  } finally {
    client.destroy();
    console.log(`Cleaning up test tenant data for runId: ${baseConfig.runId}...`);
    await cleanupLoadTestTenants(baseConfig.runId);
  }
}
