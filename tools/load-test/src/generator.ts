import { ApiLoadClient, TestTenantContext } from './client.js';
import { LoadTestConfig } from './config.js';
import { Channel, Priority } from '@notifyx/shared';
import crypto from 'node:crypto';

export interface LoadRunStats {
  totalRequests: number;
  successfulRequests: number;
  failedRequests: number;
  statusCodeCounts: Record<string, number>;
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
  generatedNotificationIds: string[];
}

export class LoadGenerator {
  private readonly config: LoadTestConfig;
  private readonly client: ApiLoadClient;
  private readonly tenants: TestTenantContext[];
  private readonly latencies: number[] = [];
  private readonly statusCodes: Record<string, number> = {};
  private readonly notificationIds: string[] = [];

  private totalCount = 0;
  private successCount = 0;
  private failCount = 0;

  constructor(config: LoadTestConfig, tenants: TestTenantContext[], client?: ApiLoadClient) {
    this.config = config;
    this.tenants = tenants;
    this.client = client || new ApiLoadClient(config.apiUrl);
  }

  /**
   * Run the load generator with token-bucket paced rate limiting.
   */
  async run(
    onTick?: (stats: { count: number; successes: number; failures: number; currentRps: number }) => void
  ): Promise<LoadRunStats> {
    const { targetRps, durationSeconds, channels, priorities } = this.config;
    const totalTargetRequests = targetRps * durationSeconds;
    const intervalMs = 1000 / targetRps;

    const startTime = Date.now();
    let sentCount = 0;
    let inFlight = 0;
    const maxConcurrency = this.config.concurrency || 20;

    // Reporting timer
    let lastTickTime = Date.now();
    let lastTickCount = 0;
    const tickInterval = setInterval(() => {
      const now = Date.now();
      const elapsedSec = (now - lastTickTime) / 1000;
      const currentRps = (this.totalCount - lastTickCount) / (elapsedSec || 1);
      lastTickTime = now;
      lastTickCount = this.totalCount;

      if (onTick) {
        onTick({
          count: this.totalCount,
          successes: this.successCount,
          failures: this.failCount,
          currentRps: Math.round(currentRps * 10) / 10,
        });
      }
    }, 1000);

    const runSingleRequest = async () => {
      const tenant = this.tenants[sentCount % this.tenants.length];
      const user = tenant.users[sentCount % tenant.users.length];
      const channel = channels[sentCount % channels.length];
      const priority = priorities[sentCount % priorities.length];

      // Distribute payload size with channel-compliant fields
      let bodyData: Record<string, unknown> = {
        subject: `Load Test Subject #${sentCount}`,
        body: `Load Test Body Message #${sentCount}`,
        message: `Load Test Message #${sentCount}`,
        title: `Load Test Title #${sentCount}`,
        sampleKey: 'load-test-data',
        index: sentCount,
      };

      if (this.config.payloadSize === 'medium') {
        bodyData = {
          ...bodyData,
          items: Array.from({ length: 10 }, (_, idx) => ({ id: idx, desc: `Item description #${idx}` })),
        };
      } else if (this.config.payloadSize === 'large') {
        bodyData = {
          ...bodyData,
          text: 'X'.repeat(2048),
        };
      }

      const correlationId = `corr_load_${this.config.runId}_${sentCount}`;

      inFlight++;
      const res = await this.client.sendNotification(
        tenant.apiKey,
        {
          userId: user.id,
          channels: [channel],
          priority,
          templateId: `tpl_load_${channel.toLowerCase()}`,
          payload: bodyData,
        },
        correlationId
      );
      inFlight--;

      this.totalCount++;
      const codeKey = String(res.statusCode);
      this.statusCodes[codeKey] = (this.statusCodes[codeKey] || 0) + 1;

      // Bound latencies array (reservoir sample if over 50,000)
      if (this.latencies.length < 50000) {
        this.latencies.push(res.latencyMs);
      } else {
        const replaceIdx = Math.floor(Math.random() * this.totalCount);
        if (replaceIdx < 50000) {
          this.latencies[replaceIdx] = res.latencyMs;
        }
      }

      if (res.statusCode === 202) {
        this.successCount++;
        if (res.notificationId) {
          this.notificationIds.push(res.notificationId);
        }
      } else {
        this.failCount++;
      }
    };

    // Pacing loop
    while (sentCount < totalTargetRequests && (Date.now() - startTime) < (durationSeconds * 1000 + 5000)) {
      if (inFlight < maxConcurrency) {
        sentCount++;
        runSingleRequest();
      }

      await new Promise((r) => setTimeout(r, intervalMs));
    }

    // Wait for in-flight requests to complete
    const drainTimeout = Date.now() + 10000;
    while (inFlight > 0 && Date.now() < drainTimeout) {
      await new Promise((r) => setTimeout(r, 50));
    }

    clearInterval(tickInterval);
    const actualDurationSeconds = (Date.now() - startTime) / 1000;

    return this.calculateStats(actualDurationSeconds);
  }

  private calculateStats(actualDurationSeconds: number): LoadRunStats {
    const sorted = [...this.latencies].sort((a, b) => a - b);
    const len = sorted.length;

    const getPercentile = (p: number) => {
      if (len === 0) return 0;
      const idx = Math.min(Math.floor((p / 100) * len), len - 1);
      return Math.round(sorted[idx] * 100) / 100;
    };

    const sum = sorted.reduce((acc, val) => acc + val, 0);
    const mean = len > 0 ? Math.round((sum / len) * 100) / 100 : 0;
    const min = len > 0 ? Math.round(sorted[0] * 100) / 100 : 0;
    const max = len > 0 ? Math.round(sorted[len - 1] * 100) / 100 : 0;

    const throughputRps = actualDurationSeconds > 0
      ? Math.round((this.totalCount / actualDurationSeconds) * 10) / 10
      : 0;

    return {
      totalRequests: this.totalCount,
      successfulRequests: this.successCount,
      failedRequests: this.failCount,
      statusCodeCounts: this.statusCodes,
      throughputRps,
      latencyMs: {
        min,
        max,
        mean,
        p50: getPercentile(50),
        p90: getPercentile(90),
        p95: getPercentile(95),
        p99: getPercentile(99),
        p99_9: getPercentile(99.9),
      },
      durationSeconds: Math.round(actualDurationSeconds * 10) / 10,
      generatedNotificationIds: this.notificationIds,
    };
  }
}
