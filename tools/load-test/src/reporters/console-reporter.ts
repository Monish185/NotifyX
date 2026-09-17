import { LoadRunStats } from '../generator.js';
import { DatabaseStateMetrics } from '../metrics-collector.js';

export class ConsoleReporter {
  static printHeader(title: string): void {
    console.log('\n================================================================');
    console.log(`  ${title.toUpperCase()}`);
    console.log('================================================================');
  }

  static printSummary(
    scenario: string,
    stats: LoadRunStats,
    dbMetrics?: DatabaseStateMetrics,
    drainTimeSec?: number
  ): void {
    console.log(`\n--- [RESULTS] Scenario: ${scenario} ---`);
    console.log(`Duration:            ${stats.durationSeconds}s`);
    console.log(`Total Requests:      ${stats.totalRequests}`);
    console.log(`Success (202):       ${stats.successfulRequests}`);
    console.log(`Failures:            ${stats.failedRequests}`);
    console.log(`Throughput:          ${stats.throughputRps} req/s`);
    console.log('\n--- Latency Percentiles (HTTP Ingress) ---');
    console.log(`  Min:               ${stats.latencyMs.min} ms`);
    console.log(`  Mean:              ${stats.latencyMs.mean} ms`);
    console.log(`  p50 (Median):      ${stats.latencyMs.p50} ms`);
    console.log(`  p90:               ${stats.latencyMs.p90} ms`);
    console.log(`  p95:               ${stats.latencyMs.p95} ms`);
    console.log(`  p99:               ${stats.latencyMs.p99} ms`);
    console.log(`  p99.9:             ${stats.latencyMs.p99_9} ms`);
    console.log(`  Max:               ${stats.latencyMs.max} ms`);

    if (dbMetrics) {
      console.log('\n--- Delivery Lifecycle Verification ---');
      console.log(`Total Notifications: ${dbMetrics.totalNotifications}`);
      console.log(`Total Deliveries:    ${dbMetrics.totalDeliveries}`);
      console.log(`  DELIVERED:         ${dbMetrics.deliveriesByStatus.DELIVERED}`);
      console.log(`  FAILED:            ${dbMetrics.deliveriesByStatus.FAILED}`);
      console.log(`  RETRY_SCHEDULED:   ${dbMetrics.deliveriesByStatus.RETRY_SCHEDULED}`);
      console.log(`  PENDING:           ${dbMetrics.deliveriesByStatus.PENDING}`);
      console.log(`Retry Records:       ${dbMetrics.totalRetryRecords}`);
      console.log(`Dead-Letter Events:  ${dbMetrics.totalDeadLetterEvents}`);
      console.log(`Pending Outbox:      ${dbMetrics.outboxByStatus.PENDING}`);
      console.log(`Lifecycle Complete:  ${dbMetrics.lifecycleComplete ? 'YES (All reached terminal state)' : 'NO'}`);
      if (drainTimeSec !== undefined) {
        console.log(`Drain Duration:      ${drainTimeSec}s`);
      }
    }
    console.log('================================================================\n');
  }
}
