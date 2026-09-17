#!/usr/bin/env node
/**
 * NotifyX — Amazon ECR Build & Push Orchestration Script
 * 
 * Builds all 7 NotifyX production Docker images with immutable Git SHA tags
 * and pushes them to Amazon Elastic Container Registry (ECR).
 * 
 * Usage:
 *   node scripts/build-and-push-ecr.js [options]
 * 
 * Options:
 *   --region=<aws-region>      AWS region (default: us-east-1)
 *   --registry=<ecr-registry>  ECR Registry URL (e.g. 123456789012.dkr.ecr.us-east-1.amazonaws.com)
 *   --service=<service-name>   Target specific service (default: all)
 *   --push                     Actually push to ECR (default: build and tag only)
 *   --tag-latest               Also tag as 'latest' in addition to Git SHA
 *   --dry-run                  Log actions without building or pushing
 */

import { execSync } from 'node:child_process';
import path from 'node:path';
import process from 'node:process';

const SERVICES = [
  { name: 'notifyx-api', dockerfile: 'apps/api/Dockerfile' },
  { name: 'notifyx-dashboard', dockerfile: 'apps/dashboard/Dockerfile' },
  { name: 'notifyx-outbox-publisher', dockerfile: 'apps/outbox-publisher/Dockerfile' },
  { name: 'notifyx-inapp-worker', dockerfile: 'apps/inapp-worker/Dockerfile' },
  { name: 'notifyx-email-worker', dockerfile: 'apps/email-worker/Dockerfile' },
  { name: 'notifyx-push-worker', dockerfile: 'apps/push-worker/Dockerfile' },
  { name: 'notifyx-sms-worker', dockerfile: 'apps/sms-worker/Dockerfile' },
  { name: 'notifyx-scheduler', dockerfile: 'apps/scheduler/Dockerfile' },
];

function parseArgs() {
  const args = process.argv.slice(2);
  const options = {
    region: process.env.AWS_REGION || 'us-east-1',
    registry: process.env.ECR_REGISTRY || '',
    service: 'all',
    push: false,
    tagLatest: true,
    dryRun: false,
  };

  for (const arg of args) {
    if (arg.startsWith('--region=')) {
      options.region = arg.split('=')[1];
    } else if (arg.startsWith('--registry=')) {
      options.registry = arg.split('=')[1];
    } else if (arg.startsWith('--service=')) {
      options.service = arg.split('=')[1];
    } else if (arg === '--push') {
      options.push = true;
    } else if (arg === '--no-tag-latest') {
      options.tagLatest = false;
    } else if (arg === '--dry-run') {
      options.dryRun = true;
    }
  }

  return options;
}

function getGitSha() {
  try {
    return execSync('git rev-parse --short=8 HEAD', { encoding: 'utf8' }).trim();
  } catch {
    console.warn('⚠️  Could not determine git SHA via git. Using fallback timestamp tag.');
    return `build-${Date.now()}`;
  }
}

function runCmd(cmd, dryRun = false) {
  console.log(`\n$ ${cmd}`);
  if (dryRun) {
    console.log('  [DRY-RUN] Command skipped.');
    return;
  }
  execSync(cmd, { stdio: 'inherit' });
}

async function main() {
  const options = parseArgs();
  const gitSha = getGitSha();

  console.log('================================================================');
  console.log('NotifyX — Amazon ECR Build & Tag Pipeline');
  console.log('================================================================');
  console.log(`Git Commit SHA : ${gitSha}`);
  console.log(`AWS Region     : ${options.region}`);
  console.log(`ECR Registry   : ${options.registry || '(Local Docker tagging only - no registry specified)'}`);
  console.log(`Target Service : ${options.service}`);
  console.log(`Push to ECR    : ${options.push}`);
  console.log(`Tag Latest     : ${options.tagLatest}`);
  console.log(`Dry Run        : ${options.dryRun}`);
  console.log('================================================================\n');

  // Filter services if specified
  const targets = options.service === 'all'
    ? SERVICES
    : SERVICES.filter((s) => s.name === options.service);

  if (targets.length === 0) {
    console.error(`❌ Unknown service: ${options.service}. Available: ${SERVICES.map((s) => s.name).join(', ')}`);
    process.exit(1);
  }

  // Optional ECR Authentication
  if (options.push) {
    if (!options.registry) {
      console.error('❌ Cannot push: --registry or ECR_REGISTRY environment variable must be specified.');
      process.exit(1);
    }

    console.log('Authenticating with Amazon ECR...');
    try {
      const loginCmd = `aws ecr get-login-password --region ${options.region} | docker login --username AWS --password-stdin ${options.registry}`;
      runCmd(loginCmd, options.dryRun);
    } catch (err) {
      console.error('❌ ECR authentication failed. Ensure AWS CLI is installed and valid AWS credentials are configured.');
      process.exit(1);
    }
  }

  // Build and Tag Images
  for (const svc of targets) {
    console.log(`\n----------------------------------------------------------------`);
    console.log(`Building image: ${svc.name}`);
    console.log(`----------------------------------------------------------------`);

    const localShaTag = `${svc.name}:${gitSha}`;
    const buildCmd = `docker build -t ${localShaTag} -f ${svc.dockerfile} .`;
    runCmd(buildCmd, options.dryRun);

    if (options.registry) {
      const ecrShaTag = `${options.registry}/${svc.name}:${gitSha}`;
      runCmd(`docker tag ${localShaTag} ${ecrShaTag}`, options.dryRun);

      if (options.tagLatest) {
        const ecrLatestTag = `${options.registry}/${svc.name}:latest`;
        runCmd(`docker tag ${localShaTag} ${ecrLatestTag}`, options.dryRun);
      }

      if (options.push) {
        console.log(`Pushing ${ecrShaTag} to ECR...`);
        runCmd(`docker push ${ecrShaTag}`, options.dryRun);

        if (options.tagLatest) {
          const ecrLatestTag = `${options.registry}/${svc.name}:latest`;
          console.log(`Pushing ${ecrLatestTag} to ECR...`);
          runCmd(`docker push ${ecrLatestTag}`, options.dryRun);
        }
      }
    } else if (options.tagLatest) {
      runCmd(`docker tag ${localShaTag} ${svc.name}:latest`, options.dryRun);
    }
  }

  console.log('\n================================================================');
  console.log('✅ ECR Build & Tag Pipeline Completed Successfully');
  console.log('================================================================');
}

main().catch((err) => {
  console.error('\n❌ Fatal error in build pipeline:', err.message);
  process.exit(1);
});
