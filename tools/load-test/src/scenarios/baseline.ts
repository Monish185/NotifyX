import { ApiLoadClient } from '../client.js';
import { LoadGenerator, LoadRunStats } from '../generator.js';
import { MetricsCollector, DatabaseStateMetrics } from '../metrics-collector.js';
import { ConsoleReporter } from '../reporters/console-reporter.js';
import { JsonReporter, TestResultArtifact } from '../reporters/json-reporter.js';
import { cleanupLoadTestTenants } from '../cleanup.js';
import { LoadTestConfig } from '../config.js';

export async function runBaselineScenario(config: LoadTestConfig): Promise<{
  stats: LoadRunStats;
  dbMetrics: DatabaseStateMetrics;
  artifactPath: string;
}> {
  ConsoleReporter.printHeader(`Phase 8 Baseline Load Test (${config.targetRps} RPS, ${config.durationSeconds}s)`);

  const client = new ApiLoadClient(config.apiUrl);
  const metricsCollector = new MetricsCollector();

  console.log(`[1/4] Setting up ${config.tenantCount} dedicated test tenants (runId: ${config.runId})...`);
  const tenants = await client.setupTestTenants(config.runId, config.tenantCount);
  const tenantIds = tenants.map((t) => t.tenantId);

  try {
    console.log(`[2/4] Executing load generator across channels: ${config.channels.join(', ')}...`);
    const generator = new LoadGenerator(config, tenants, client);

    const stats = await generator.run((tick) => {
      process.stdout.write(
        `\r      Requests: ${tick.count} | 202 OK: ${tick.successes} | Errors: ${tick.failures} | Rate: ${tick.currentRps} req/s`
      );
    });
    console.log('\n[2/4] Ingress generation completed.');

    console.log('[3/4] Waiting for Outbox Publisher and Workers to drain backlog...');
    const drainResult = await metricsCollector.waitForDrain(tenantIds, 60000);
    const drainDurationSec = Math.round((drainResult.elapsedMs / 1000) * 10) / 10;

    ConsoleReporter.printSummary('baseline', stats, drainResult.finalMetrics, drainDurationSec);

    console.log('[4/4] Persisting test artifact to disk...');
    const artifact: TestResultArtifact = {
      timestamp: new Date().toISOString(),
      scenario: 'baseline',
      config: {
        targetRps: config.targetRps,
        durationSeconds: config.durationSeconds,
        concurrency: config.concurrency,
        channels: config.channels,
        tenantCount: config.tenantCount,
        runId: config.runId,
      },
      results: {
        totalRequests: stats.totalRequests,
        successfulRequests: stats.successfulRequests,
        failedRequests: stats.failedRequests,
        throughputRps: stats.throughputRps,
        latencyMs: stats.latencyMs,
        durationSeconds: stats.durationSeconds,
        deliveriesByStatus: drainResult.finalMetrics.deliveriesByStatus,
        outboxByStatus: drainResult.finalMetrics.outboxByStatus,
        totalRetryRecords: drainResult.finalMetrics.totalRetryRecords,
        totalDeadLetterEvents: drainResult.finalMetrics.totalDeadLetterEvents,
        lifecycleComplete: drainResult.finalMetrics.lifecycleComplete,
        drainDurationSec,
      },
    };

    const artifactPath = JsonReporter.saveResult('baseline', artifact);
    console.log(`[OK] Artifact saved to: ${artifactPath}`);

    return {
      stats,
      dbMetrics: drainResult.finalMetrics,
      artifactPath,
    };
  } finally {
    client.destroy();
    console.log(`Cleaning up test tenant data for runId: ${config.runId}...`);
    await cleanupLoadTestTenants(config.runId);
  }
}
