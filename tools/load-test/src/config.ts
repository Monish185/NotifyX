import { Channel, Priority } from '@notifyx/shared';
import crypto from 'node:crypto';

export interface LoadTestConfig {
  targetRps: number;
  concurrency: number;
  durationSeconds: number;
  tenantCount: number;
  apiUrl: string;
  channels: Channel[];
  priorities: Priority[];
  runId: string;
  payloadSize: 'small' | 'medium' | 'large';
  verbose?: boolean;
}

export function parseConfig(overrides: Partial<LoadTestConfig> = {}): LoadTestConfig {
  const runId = overrides.runId || `run_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;
  return {
    targetRps: overrides.targetRps ?? Number(process.env.TARGET_RPS || 20),
    concurrency: overrides.concurrency ?? Number(process.env.CONCURRENCY || 10),
    durationSeconds: overrides.durationSeconds ?? Number(process.env.DURATION_SECONDS || 60),
    tenantCount: overrides.tenantCount ?? Number(process.env.TENANT_COUNT || 2),
    apiUrl: overrides.apiUrl || process.env.API_URL || 'http://127.0.0.1:3001',
    channels: overrides.channels || [Channel.EMAIL, Channel.SMS, Channel.PUSH, Channel.IN_APP],
    priorities: overrides.priorities || [Priority.NORMAL, Priority.HIGH, Priority.LOW],
    runId,
    payloadSize: overrides.payloadSize || 'small',
    verbose: overrides.verbose ?? false,
  };
}
