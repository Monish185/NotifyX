import { parseConfig } from './config.js';
import { runBaselineScenario } from './scenarios/baseline.js';
import { runSustainedScenario } from './scenarios/sustained.js';
import { runBurstScenario } from './scenarios/burst.js';
import { runRetryStormScenario } from './scenarios/retry-storm.js';
import { runIdempotencyScenario } from './scenarios/idempotency.js';
import { cleanupLoadTestTenants } from './cleanup.js';
import {
  runApiRateLimitScenario,
  runNotificationQuotaScenario,
  runConcurrentQuotaScenario,
  runMultiTenantFairnessScenario,
  runProviderRateLimitScenario,
  runWorkerConcurrencyScenario,
  runRedisOutageScenario,
  runCombinedBackpressureScenario,
} from './scenarios/phase11/index.js';

async function main() {
  const args = process.argv.slice(2);
  const scenarioArg = args.find((a) => a.startsWith('--scenario='))?.split('=')[1] || 'baseline';
  const rpsArg = args.find((a) => a.startsWith('--rps='))?.split('=')[1];
  const durationArg = args.find((a) => a.startsWith('--duration='))?.split('=')[1];
  const concurrencyArg = args.find((a) => a.startsWith('--concurrency='))?.split('=')[1];
  const cleanupOnly = args.includes('--cleanup-only');

  if (cleanupOnly) {
    console.log('Cleaning up all test tenant data matching prefix tenant_load_test_...');
    const result = await cleanupLoadTestTenants();
    console.log('Cleanup result:', result);
    process.exit(0);
  }

  const config = parseConfig({
    targetRps: rpsArg ? Number(rpsArg) : undefined,
    durationSeconds: durationArg ? Number(durationArg) : undefined,
    concurrency: concurrencyArg ? Number(concurrencyArg) : undefined,
  });

  console.log(`\n================================================================`);
  console.log(`  NotifyX Phase 8 Load Testing Harness                          `);
  console.log(`  Scenario:     ${scenarioArg}                                  `);
  console.log(`  Target RPS:   ${config.targetRps}                             `);
  console.log(`  Duration:     ${config.durationSeconds}s                      `);
  console.log(`  Concurrency:  ${config.concurrency}                          `);
  console.log(`  Run ID:       ${config.runId}                                 `);
  console.log(`================================================================\n`);

  switch (scenarioArg.toLowerCase()) {
    case 'baseline':
      await runBaselineScenario(config);
      break;
    case 'sustained':
      await runSustainedScenario(config);
      break;
    case 'burst':
      await runBurstScenario(config);
      break;
    case 'retry-storm':
      await runRetryStormScenario(config);
      break;
    case 'idempotency':
      await runIdempotencyScenario(config);
      break;
    case 'all':
      console.log('>>> [1/5] RUNNING BASELINE...');
      await runBaselineScenario({ ...config, durationSeconds: 20 });
      console.log('\n>>> [2/5] RUNNING SUSTAINED...');
      await runSustainedScenario({ ...config, targetRps: 50, durationSeconds: 30 });
      console.log('\n>>> [3/5] RUNNING BURST...');
      await runBurstScenario(config);
      console.log('\n>>> [4/5] RUNNING RETRY STORM...');
      await runRetryStormScenario(config);
      console.log('\n>>> [5/5] RUNNING IDEMPOTENCY...');
      await runIdempotencyScenario(config);
      break;
    case 'api-rate-limit':
      await runApiRateLimitScenario();
      break;
    case 'notification-quota':
      await runNotificationQuotaScenario();
      break;
    case 'concurrent-quota':
      await runConcurrentQuotaScenario();
      break;
    case 'multi-tenant-fairness':
      await runMultiTenantFairnessScenario();
      break;
    case 'provider-rate-limit':
      await runProviderRateLimitScenario();
      break;
    case 'worker-concurrency':
      await runWorkerConcurrencyScenario();
      break;
    case 'redis-outage':
      await runRedisOutageScenario();
      break;
    case 'combined-backpressure':
      await runCombinedBackpressureScenario();
      break;
    case 'phase11':
      await runApiRateLimitScenario();
      await runNotificationQuotaScenario();
      await runConcurrentQuotaScenario();
      await runMultiTenantFairnessScenario();
      await runProviderRateLimitScenario();
      await runWorkerConcurrencyScenario();
      await runRedisOutageScenario();
      await runCombinedBackpressureScenario();
      break;
    default:
      console.error(`Unknown scenario: '${scenarioArg}'. Supported: baseline, sustained, burst, retry-storm, idempotency, all, phase11, api-rate-limit, notification-quota, concurrent-quota, multi-tenant-fairness, provider-rate-limit, worker-concurrency, redis-outage, combined-backpressure`);
      process.exit(1);
  }

  console.log('\n[COMPLETE] Selected scenario execution finished cleanly.');
}

main().catch((err) => {
  console.error('\n[FATAL ERROR in Load Test Harness]:', err);
  process.exit(1);
});
