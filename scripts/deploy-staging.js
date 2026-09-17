#!/usr/bin/env node
/**
 * NotifyX — Staging Deployment Script
 *
 * SAFETY INVARIANTS (MANDATORY):
 * 1. STAGING ONLY — refuses to run if DEPLOY_ENV !== "staging"
 * 2. EXPLICIT CONFIRMATION — requires CONFIRM_DEPLOY=true
 * 3. NO PRODUCTION SUPPORT — hard-stops on any production env value
 * 4. NO SECRETS IN CLI ARGS — reads credentials from environment only
 * 5. PRISMA MIGRATION RUNS ONCE — not from application startup
 *
 * Usage:
 *   DEPLOY_ENV=staging CONFIRM_DEPLOY=true node scripts/deploy-staging.js
 *
 * Environment variables (all read from environment, never CLI args):
 *   DEPLOY_ENV        - Must be exactly "staging"
 *   CONFIRM_DEPLOY    - Must be exactly "true"
 *   DATABASE_URL      - Required for migration step
 *   API_BASE          - Override API base URL for health check (default: http://localhost:3001)
 */

import { execSync, spawnSync } from 'node:child_process';
import process from 'node:process';

const API_BASE = process.env.API_BASE || 'http://localhost:3001';
const DEPLOY_ENV = process.env.DEPLOY_ENV;
const CONFIRM_DEPLOY = process.env.CONFIRM_DEPLOY;

// ============================================================================
// SAFETY CHECKS — These must run before any deployment action
// ============================================================================
function enforceSafetyInvariants() {
  if (!DEPLOY_ENV) {
    console.error('❌ FATAL: DEPLOY_ENV is not set. Must be explicitly set to "staging".');
    console.error('   Usage: DEPLOY_ENV=staging CONFIRM_DEPLOY=true node scripts/deploy-staging.js');
    process.exit(1);
  }

  if (DEPLOY_ENV === 'production') {
    console.error('❌ FATAL: DEPLOY_ENV=production is not supported by this script.');
    console.error('   This script is strictly staging-only. Production deployment requires a manual procedure.');
    console.error('   See: docs/aws_deployment.md for the production deployment process.');
    process.exit(1);
  }

  if (DEPLOY_ENV !== 'staging') {
    console.error(`❌ FATAL: DEPLOY_ENV="${DEPLOY_ENV}" is not recognized. Must be exactly "staging".`);
    process.exit(1);
  }

  if (!CONFIRM_DEPLOY || CONFIRM_DEPLOY !== 'true') {
    console.error('❌ FATAL: Deployment requires explicit confirmation.');
    console.error('   Set CONFIRM_DEPLOY=true to proceed.');
    process.exit(1);
  }

  if (!process.env.DATABASE_URL) {
    console.error('❌ FATAL: DATABASE_URL is required but not set.');
    process.exit(1);
  }

  console.log('✅ Safety invariants verified: STAGING deployment with explicit confirmation.');
}

function runCmd(label, cmd, opts = {}) {
  console.log(`\n[${label}] ${cmd}`);
  const result = spawnSync(cmd, { shell: true, stdio: 'inherit', ...opts });
  if (result.status !== 0) {
    console.error(`❌ [${label}] Command failed with exit code ${result.status}`);
    process.exit(result.status ?? 1);
  }
  console.log(`✅ [${label}] Done.`);
}

async function httpGet(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(5000) });
  return { status: response.status, body: await response.json().catch(() => null) };
}

async function waitForHealth(label, url, maxRetries = 12, delayMs = 5000) {
  console.log(`\n[${label}] Waiting for health: ${url}`);
  for (let i = 1; i <= maxRetries; i++) {
    try {
      const { status } = await httpGet(url);
      if (status === 200) {
        console.log(`✅ [${label}] Service is healthy.`);
        return;
      }
      console.log(`  Attempt ${i}/${maxRetries}: HTTP ${status} — retrying in ${delayMs / 1000}s...`);
    } catch {
      console.log(`  Attempt ${i}/${maxRetries}: Connection refused — retrying in ${delayMs / 1000}s...`);
    }
    await new Promise((r) => setTimeout(r, delayMs));
  }
  console.error(`❌ [${label}] Service did not become healthy after ${maxRetries} retries.`);
  process.exit(1);
}

async function main() {
  console.log('================================================================');
  console.log('NotifyX — Staging Deployment Orchestrator');
  console.log('================================================================');
  console.log(`Timestamp     : ${new Date().toISOString()}`);
  console.log(`Environment   : ${DEPLOY_ENV}`);
  console.log(`API Base      : ${API_BASE}`);
  console.log('================================================================\n');

  // Step 1: Enforce safety invariants before anything
  enforceSafetyInvariants();

  // Step 2: Validate environment configuration
  console.log('\n--- Step 1/9: Validating environment configuration ---');
  runCmd('validate-env', 'node -e "import(\'./packages/config/dist/index.js\').then(m => { m.loadConfig(); console.log(\'Config valid.\'); }).catch(e => { console.error(e.message); process.exit(1); })"');

  // Step 3: Build/pull images
  console.log('\n--- Step 2/9: Build Docker images (or pull from registry) ---');
  const imageTag = process.env.IMAGE_TAG || 'latest';
  if (process.env.ECR_REGISTRY) {
    // Pull from ECR if registry is provided
    const registry = process.env.ECR_REGISTRY;
    const services = ['api', 'dashboard', 'outbox-publisher', 'inapp-worker', 'email-worker', 'push-worker', 'sms-worker'];
    for (const svc of services) {
      runCmd(`pull-${svc}`, `docker pull ${registry}/notifyx-${svc}:${imageTag}`);
    }
  } else {
    console.log('No ECR_REGISTRY set — building images locally from docker-compose.prod.yml...');
    runCmd('docker-build', `IMAGE_TAG=${imageTag} docker compose -f docker-compose.prod.yml build`);
  }

  // Step 4: Verify network/infrastructure dependencies
  console.log('\n--- Step 3/9: Verifying Docker network ---');
  runCmd('docker-network', 'docker network ls --filter name=notifyx-prod-network');

  // Step 5: Run Prisma migrations ONCE as a standalone step
  console.log('\n--- Step 4/9: Running Prisma database migrations (once, standalone) ---');
  console.log('IMPORTANT: Migrations run once as a standalone step before any service starts.');
  console.log('This prevents race conditions when multiple replicas start simultaneously.');
  runCmd('db-migrate', 'pnpm --filter @notifyx/database db:deploy');

  // Step 6: Deploy API & Outbox Publisher first (consumers of DB)
  console.log('\n--- Step 5/9: Deploying API and Outbox Publisher ---');
  runCmd('deploy-api', `IMAGE_TAG=${imageTag} docker compose -f docker-compose.prod.yml up -d api outbox-publisher`);

  // Step 7: Deploy workers
  console.log('\n--- Step 6/9: Deploying Channel Workers ---');
  runCmd('deploy-workers', `IMAGE_TAG=${imageTag} docker compose -f docker-compose.prod.yml up -d inapp-worker email-worker push-worker sms-worker`);

  // Step 8: Deploy dashboard
  console.log('\n--- Step 7/9: Deploying Dashboard ---');
  runCmd('deploy-dashboard', `IMAGE_TAG=${imageTag} docker compose -f docker-compose.prod.yml up -d dashboard`);

  // Step 9: Verify health endpoints
  console.log('\n--- Step 8/9: Health and Readiness Verification ---');
  await waitForHealth('api-health', `${API_BASE}/health`);
  await waitForHealth('api-readiness', `${API_BASE}/ready`);

  // Step 10: Run staging smoke test
  console.log('\n--- Step 9/9: Running Staging Smoke Test ---');
  runCmd('smoke-test', `SMOKE_ENV=staging node scripts/smoke-test.js`);

  console.log('\n================================================================');
  console.log('✅ Staging Deployment Complete');
  console.log('================================================================');
  console.log('');
  console.log('NOTE: This deployment does NOT claim zero-downtime.');
  console.log('      For a future zero-downtime strategy, consider ECS rolling');
  console.log('      deployments or a blue/green configuration as described in');
  console.log('      docs/aws_deployment.md.');
}

main().catch((err) => {
  console.error('\n❌ Fatal deployment error:', err.message);
  process.exit(1);
});
