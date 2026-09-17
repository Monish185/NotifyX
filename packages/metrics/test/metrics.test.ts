import { describe, it, expect, beforeEach } from 'vitest';
import {
  register,
  resetMetrics,
  getMetrics,
  getContentType,
  notificationsCreatedCounter,
  deliveriesAttemptedCounter,
  deliveriesSucceededCounter,
  deliveriesFailedCounter,
  retriesScheduledCounter,
  dlqEventsCounter,
  outboxEventsPendingGauge,
  normalizeErrorCode,
  normalizeReasonCategory,
  ErrorCodeCategory,
  ReasonCategory,
} from '../src/index.js';

describe('@notifyx/metrics', () => {
  beforeEach(() => {
    resetMetrics();
  });

  it('1. should register metrics with valid names and help text', () => {
    expect(register.getSingleMetric('notifications_created_total')).toBeDefined();
    expect(register.getSingleMetric('deliveries_attempted_total')).toBeDefined();
    expect(register.getSingleMetric('deliveries_succeeded_total')).toBeDefined();
    expect(register.getSingleMetric('deliveries_failed_total')).toBeDefined();
    expect(register.getSingleMetric('retries_scheduled_total')).toBeDefined();
    expect(register.getSingleMetric('dlq_events_total')).toBeDefined();
    expect(register.getSingleMetric('outbox_events_pending')).toBeDefined();
  });

  it('2. deliveries_attempted_total must only have service and channel labels (no attempt or high-cardinality)', () => {
    const metric = register.getSingleMetric('deliveries_attempted_total') as any;
    expect(metric.labelNames).toEqual(['service', 'channel']);
    expect(metric.labelNames).not.toContain('attempt');
    expect(metric.labelNames).not.toContain('tenantId');
    expect(metric.labelNames).not.toContain('deliveryId');
  });

  it('3. should increment metrics and expose valid Prometheus text format', async () => {
    deliveriesAttemptedCounter.inc({ service: 'email-worker', channel: 'EMAIL' });
    deliveriesSucceededCounter.inc({ service: 'email-worker', channel: 'EMAIL', provider: 'ses' });
    retriesScheduledCounter.inc({ service: 'sms-worker', channel: 'SMS' });
    dlqEventsCounter.inc({ service: 'push-worker', channel: 'PUSH', reason_category: 'RETRIES_EXHAUSTED' });
    outboxEventsPendingGauge.set({ event_type: 'notification.delivery.requested' }, 42);

    const output = await getMetrics();
    expect(output).toContain('deliveries_attempted_total{service="email-worker",channel="EMAIL"} 1');
    expect(output).toContain('deliveries_succeeded_total{service="email-worker",channel="EMAIL",provider="ses"} 1');
    expect(output).toContain('retries_scheduled_total{service="sms-worker",channel="SMS"} 1');
    expect(output).toContain('dlq_events_total{service="push-worker",channel="PUSH",reason_category="RETRIES_EXHAUSTED"} 1');
    expect(output).toContain('outbox_events_pending{event_type="notification.delivery.requested"} 42');

    expect(getContentType()).toContain('text/plain');
  });

  it('4. normalizeErrorCode should map diverse strings to bounded categories', () => {
    expect(normalizeErrorCode('ETIMEDOUT: connect timeout')).toBe(ErrorCodeCategory.TIMEOUT);
    expect(normalizeErrorCode('401 Unauthorized: Invalid API Key')).toBe(ErrorCodeCategory.AUTH_ERROR);
    expect(normalizeErrorCode('TooManyRequestsException: 429')).toBe(ErrorCodeCategory.RATE_LIMIT);
    expect(normalizeErrorCode('InvalidParameter: phone is bad')).toBe(ErrorCodeCategory.BAD_REQUEST);
    expect(normalizeErrorCode('503 Service Unavailable')).toBe(ErrorCodeCategory.SERVER_ERROR);
    expect(normalizeErrorCode('ECONNREFUSED: socket error')).toBe(ErrorCodeCategory.NETWORK_ERROR);
    expect(normalizeErrorCode('something totally unexpected')).toBe(ErrorCodeCategory.UNKNOWN);
    expect(normalizeErrorCode(null)).toBe(ErrorCodeCategory.UNKNOWN);
  });

  it('5. normalizeReasonCategory should map terminal failure reasons to bounded categories', () => {
    expect(normalizeReasonCategory('Exhausted maximum retry attempts (5/5)')).toBe(ReasonCategory.RETRIES_EXHAUSTED);
    expect(normalizeReasonCategory('Invalid channel payload structure')).toBe(ReasonCategory.INVALID_PAYLOAD);
    expect(normalizeReasonCategory('Cannot resolve recipient destination phone')).toBe(ReasonCategory.INVALID_RECIPIENT);
    expect(normalizeReasonCategory('Permanent non-retryable provider error')).toBe(ReasonCategory.PERMANENT_ERROR);
    expect(normalizeReasonCategory('Provider rejected request')).toBe(ReasonCategory.PROVIDER_REJECTED);
    expect(normalizeReasonCategory(null)).toBe(ReasonCategory.UNKNOWN);
  });
});
