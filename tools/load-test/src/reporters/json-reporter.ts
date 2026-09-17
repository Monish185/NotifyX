import fs from 'node:fs';
import path from 'node:path';
import { LoadRunStats } from '../generator.js';
import { DatabaseStateMetrics } from '../metrics-collector.js';

export interface TestResultArtifact {
  timestamp: string;
  scenario: string;
  config: Record<string, unknown>;
  results: {
    totalRequests: number;
    successfulRequests: number;
    failedRequests: number;
    throughputRps: number;
    latencyMs: {
      min: number;
      max: number;
      mean: number;
      p50: number;
      p90: number;
      p95: number;
      p99: number;
      p99_9: number;
    };
    durationSeconds: number;
    deliveriesByStatus?: DatabaseStateMetrics['deliveriesByStatus'];
    outboxByStatus?: DatabaseStateMetrics['outboxByStatus'];
    totalRetryRecords?: number;
    totalDeadLetterEvents?: number;
    lifecycleComplete?: boolean;
    drainDurationSec?: number;
  };
}

export class JsonReporter {
  private static readonly outputDir = path.resolve(process.cwd(), 'artifacts', 'phase8-results');

  static saveResult(scenario: string, artifact: TestResultArtifact): string {
    if (!fs.existsSync(this.outputDir)) {
      fs.mkdirSync(this.outputDir, { recursive: true });
    }

    const filename = `${scenario}-${Date.now()}.json`;
    const filePath = path.join(this.outputDir, filename);

    // Ensure no secrets or credentials leaked
    const sanitized = JSON.parse(JSON.stringify(artifact));
    fs.writeFileSync(filePath, JSON.stringify(sanitized, null, 2), 'utf8');

    return filePath;
  }

  static savePhase11Result(filename: string, data: Record<string, unknown>): string {
    const dir = path.resolve(process.cwd(), 'artifacts', 'phase11-results');
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    const cleanFilename = filename.endsWith('.json') ? filename : `${filename}.json`;
    const filePath = path.join(dir, cleanFilename);

    // Sanitize payload
    const sanitized = JSON.parse(JSON.stringify(data, (key, value) => {
      if (key.toLowerCase().includes('key') || key.toLowerCase().includes('secret') || key.toLowerCase().includes('auth') || key.toLowerCase().includes('password')) {
        return '[REDACTED]';
      }
      return value;
    }));

    fs.writeFileSync(filePath, JSON.stringify(sanitized, null, 2), 'utf8');
    return filePath;
  }
}

