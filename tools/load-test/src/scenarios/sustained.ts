import { ApiLoadClient } from '../client.js';
import { LoadGenerator, LoadRunStats } from '../generator.js';
import { MetricsCollector, DatabaseStateMetrics } from '../metrics-collector.js';
import { ConsoleReporter } from '../reporters/console-reporter.js';
import { JsonReporter, TestResultArtifact } from '../reporters/json-reporter.js';
import { cleanupLoadTestTenants } from '../cleanup.js';
import { LoadTestConfig } from '../config.js';

export async function runSustainedScenario(config: LoadTestConfig): Promise<{
  stats: LoadRunStats;
  dbMetrics: DatabaseStateMetrics;
  artifactPath: string;
}> {
  ConsoleReporter.printHeader(`Phase 8 Sustained Load Test (${config.targetRps} RPS, ${config.durationSeconds}s)`);

  const client = new ApiLoadClient(config.apiUrl);
  const metricsCollector = new MetricsCollector();

  console.log(`[1/4] Setting up test tenants (runId: ${config.runId})...`);
  const tenants = await client.setupTestTenants(config.runId, config.tenantCount);
  const tenantIds = tenants.map((t) => t.tenantId);

  const initialMemoryMb = Math.round(process.memoryUsage().rss / 1024 / 1024);

  try {
    console.log(`[2/4] Generating sustained load at target ${config.targetRps} RPS...`);
    const generator = new LoadGenerator(config, tenants, client);

    const stats = await generator.run((tick) => {
      process.stdout.write(
        `\r      Requests: ${tick.count} | 202 OK: ${tick.successes} | Errors: ${tick.failures} | Rate: ${tick.currentRps} req/s`
      );
    });
    console.log('\n[2/4] Sustained generation finished.');

    const peakMemoryMb = Math.round(process.memoryUsage().rss / 1024 / 1024);

    console.log('[3/4] Waiting for queue drain and verifying outbox consumption...');
    const drainResult = await metricsCollector.waitForDrain(tenantIds, 120000);
    const drainDurationSec = Math.round((drainResult.elapsedMs / 1000) * 10) / 10;

    ConsoleReporter.printSummary('sustained', stats, drainResult.finalMetrics, drainDurationSec);

    console.log('[4/4] Persisting sustained load test artifact...');
    const artifact: TestResultArtifact = {
      timestamp: new Date().toISOString(),
      scenario: 'sustained',
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

    const artifactPath = JsonReporter.saveResult('sustained', artifact);
    console.log(`[OK] Artifact saved to: ${artifactPath}`);
    console.log(`Memory Footprint: Initial ${initialMemoryMb}MB -> Peak ${peakMemoryMb}MB`);

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
