import {
  ChannelProvider,
  ProviderSendRequest,
  ProviderResult,
} from './channel-worker.js';
import { logger } from '@notifyx/logger';

/**
 * Base abstract mock provider offering side-effect deduplication by idempotencyKey.
 */
export abstract class BaseMockProvider<T = Record<string, unknown>>
  implements ChannelProvider<T>
{
  abstract readonly name: string;
  private readonly sentHistory = new Map<
    string,
    { count: number; result: ProviderResult; request: ProviderSendRequest<T> }
  >();
  private simulateFailure = false;
  private failureReason = 'Simulated provider outage';
  private failureRetryable = true;
  private simulateRateLimit = false;
  private latencyMs = 0;
  private failureRate = 0;

  /**
   * Set failure simulation for testing error handling and redelivery semantics.
   */
  setFailureSimulation(enabled: boolean, reason?: string, retryable = true): void {
    this.simulateFailure = enabled;
    if (reason) this.failureReason = reason;
    this.failureRetryable = retryable;
  }

  /**
   * Set downstream 429 rate-limit simulation.
   */
  setRateLimitSimulation(enabled: boolean): void {
    this.simulateRateLimit = enabled;
  }

  /**
   * Set simulated provider network/processing latency in milliseconds.
   */
  setLatencyMs(ms: number): void {
    this.latencyMs = ms;
  }

  /**
   * Set probabilistic failure rate between 0 and 1.
   */
  setFailureRate(rate: number): void {
    this.failureRate = rate;
  }

  /**
   * Reset provider state for clean test isolation.
   */
  reset(): void {
    this.sentHistory.clear();
    this.simulateFailure = false;
    this.simulateRateLimit = false;
    this.latencyMs = 0;
    this.failureRate = 0;
  }

  /**
   * Return the number of times this idempotency key was sent.
   */
  getSendCount(idempotencyKey: string): number {
    return this.sentHistory.get(idempotencyKey)?.count || 0;
  }

  /**
   * Inspect the historical record for an idempotencyKey.
   */
  getRecord(idempotencyKey: string) {
    return this.sentHistory.get(idempotencyKey);
  }

  async send(request: ProviderSendRequest<T>): Promise<ProviderResult> {
    const { idempotencyKey, recipient } = request;

    // Simulate latency if configured
    if (this.latencyMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.latencyMs));
    }

    // Simulate downstream rate limiting (HTTP 429) if configured
    if (this.simulateRateLimit) {
      logger.warn(
        { provider: this.name, idempotencyKey, recipient },
        `[MOCK PROVIDER] Simulating provider rate limit (HTTP 429) for ${this.name}`
      );
      return {
        success: false,
        error: 'Downstream provider rate limit exceeded (HTTP 429)',
        retryable: true,
        metadata: {
          errorCode: 'RATE_LIMIT',
          retryAfterMs: 5000,
        },
      };
    }

    // Simulate probabilistic failure if configured
    if (this.failureRate > 0 && Math.random() < this.failureRate) {
      logger.warn(
        { provider: this.name, idempotencyKey, recipient, failureRate: this.failureRate },
        `[MOCK PROVIDER] Simulating injected random failure for ${this.name}`
      );
      return {
        success: false,
        error: 'Simulated transient downstream failure',
        retryable: true,
        metadata: { errorCode: 'TRANSIENT_FAILURE' },
      };
    }

    // Simulate explicit failure if configured
    if (this.simulateFailure) {
      logger.warn(
        {
          provider: this.name,
          idempotencyKey,
          recipient,
          error: this.failureReason,
        },
        `[MOCK PROVIDER] Simulating failure for ${this.name}`
      );
      return {
        success: false,
        error: this.failureReason,
        retryable: this.failureRetryable,
      };
    }

    // Side-effect Idempotency Check:
    // If the provider has already seen this deliveryId / idempotency key,
    // it deduplicates the external call and returns the previous logical result.
    const existing = this.sentHistory.get(idempotencyKey);
    if (existing) {
      existing.count += 1;
      logger.info(
        {
          provider: this.name,
          idempotencyKey,
          recipient,
          previousResult: existing.result.providerMessageId,
          totalInvocations: existing.count,
        },
        `[MOCK PROVIDER] Duplicate idempotencyKey detected! Returning cached provider response without duplicate send side-effect.`
      );
      return existing.result;
    }

    // First time send
    const providerMessageId = `mock_${this.name.toLowerCase()}_${idempotencyKey}`;
    const result: ProviderResult = {
      success: true,
      providerMessageId,
      metadata: {
        sentAt: new Date().toISOString(),
        channel: this.name,
      },
    };

    this.sentHistory.set(idempotencyKey, {
      count: 1,
      result,
      request,
    });

    logger.info(
      {
        provider: this.name,
        idempotencyKey,
        recipient,
        providerMessageId,
      },
      `[MOCK PROVIDER] Message sent successfully via ${this.name}`
    );

    return result;
  }
}

/**
 * Mock Email Provider (e.g. SendGrid / AWS SES substitute)
 */
export class MockEmailProvider extends BaseMockProvider {
  readonly name = 'MockEmailProvider';
}

/**
 * Mock Push Provider (e.g. Firebase Cloud Messaging / APNs substitute)
 */
export class MockPushProvider extends BaseMockProvider {
  readonly name = 'MockPushProvider';
}

/**
 * Mock SMS Provider (e.g. Twilio substitute)
 */
export class MockSmsProvider extends BaseMockProvider {
  readonly name = 'MockSmsProvider';
}
