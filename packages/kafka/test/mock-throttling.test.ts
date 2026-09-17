import { describe, it, expect } from 'vitest';
import { MockEmailProvider } from '../src/mock-providers.js';

describe('BaseMockProvider Throttling & Simulation', () => {
  it('simulates downstream 429 rate limit when configured', async () => {
    const provider = new MockEmailProvider();
    provider.setRateLimitSimulation(true);

    const result = await provider.send({
      idempotencyKey: 'del_throttle_1',
      tenantId: 'tenant_1',
      userId: 'user_1',
      recipient: 'test@example.com',
      notificationId: 'notif_1',
      deliveryId: 'del_throttle_1',
      payload: {},
    });

    expect(result.success).toBe(false);
    expect(result.retryable).toBe(true);
    expect(result.metadata?.errorCode).toBe('RATE_LIMIT');
    expect(result.error).toContain('429');
  });

  it('simulates provider latency when configured', async () => {
    const provider = new MockEmailProvider();
    provider.setLatencyMs(100);

    const start = Date.now();
    const result = await provider.send({
      idempotencyKey: 'del_latency_1',
      tenantId: 'tenant_1',
      userId: 'user_1',
      recipient: 'test@example.com',
      notificationId: 'notif_2',
      deliveryId: 'del_latency_1',
      payload: {},
    });

    const elapsed = Date.now() - start;
    expect(result.success).toBe(true);
    expect(elapsed).toBeGreaterThanOrEqual(90);
  });
});
