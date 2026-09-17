/**
 * @notifyx/metrics
 * Prometheus-compatible metrics registry and collectors for NotifyX.
 *
 * All metric labels are strictly bounded to prevent high-cardinality memory leaks.
 * Excludes tenantId, userId, notificationId, deliveryId, eventId, and raw error strings.
 */

import client, {
  Registry,
  Counter,
  Histogram,
  Gauge,
  collectDefaultMetrics as promCollectDefaultMetrics,
} from 'prom-client';

export const register = new Registry();

// Collect Node.js process runtime metrics (event loop lag, memory, CPU)
promCollectDefaultMetrics({ register, prefix: 'notifyx_' });

// ============================================================================
// Normalized Enums & Helpers for Bounded Cardinality
// ============================================================================

export const ErrorCodeCategory = {
  TIMEOUT: 'TIMEOUT',
  AUTH_ERROR: 'AUTH_ERROR',
  RATE_LIMIT: 'RATE_LIMIT',
  BAD_REQUEST: 'BAD_REQUEST',
  SERVER_ERROR: 'SERVER_ERROR',
  NETWORK_ERROR: 'NETWORK_ERROR',
  UNKNOWN: 'UNKNOWN',
} as const;

export type ErrorCodeCategory =
  (typeof ErrorCodeCategory)[keyof typeof ErrorCodeCategory];

export const ReasonCategory = {
  RETRIES_EXHAUSTED: 'RETRIES_EXHAUSTED',
  PERMANENT_ERROR: 'PERMANENT_ERROR',
  INVALID_PAYLOAD: 'INVALID_PAYLOAD',
  INVALID_RECIPIENT: 'INVALID_RECIPIENT',
  PROVIDER_REJECTED: 'PROVIDER_REJECTED',
  UNKNOWN: 'UNKNOWN',
} as const;

export type ReasonCategory =
  (typeof ReasonCategory)[keyof typeof ReasonCategory];

/**
 * Normalizes arbitrary error codes / messages into a strictly bounded enum.
 */
export function normalizeErrorCode(raw?: string | null): ErrorCodeCategory {
  if (!raw) return ErrorCodeCategory.UNKNOWN;
  const upper = raw.toUpperCase();
  if (upper.includes('TIMEOUT') || upper.includes('ETIMEDOUT') || upper.includes('ECONNRESET')) {
    return ErrorCodeCategory.TIMEOUT;
  }
  if (upper.includes('AUTH') || upper.includes('UNAUTHORIZED') || upper.includes('FORBIDDEN') || upper.includes('KEY') || upper.includes('PERMISSION')) {
    return ErrorCodeCategory.AUTH_ERROR;
  }
  if (
    upper.includes('RATE') ||
    upper.includes('LIMIT') ||
    upper.includes('THROTTLED') ||
    upper.includes('TOO_MANY') ||
    upper.includes('TOOMANY') ||
    upper.includes('429')
  ) {
    return ErrorCodeCategory.RATE_LIMIT;
  }
  if (upper.includes('INVALID') || upper.includes('BAD') || upper.includes('PARAM') || upper.includes('NOT_REGISTERED') || upper.includes('400')) {
    return ErrorCodeCategory.BAD_REQUEST;
  }
  if (upper.includes('SERVER') || upper.includes('500') || upper.includes('502') || upper.includes('503') || upper.includes('504') || upper.includes('UNAVAILABLE')) {
    return ErrorCodeCategory.SERVER_ERROR;
  }
  if (upper.includes('NETWORK') || upper.includes('SOCKET') || upper.includes('ENOTFOUND') || upper.includes('ECONNREFUSED')) {
    return ErrorCodeCategory.NETWORK_ERROR;
  }
  return ErrorCodeCategory.UNKNOWN;
}

/**
 * Normalizes arbitrary terminal failure reasons into a bounded reason category.
 */
export function normalizeReasonCategory(raw?: string | null): ReasonCategory {
  if (!raw) return ReasonCategory.UNKNOWN;
  const upper = raw.toUpperCase();
  if (upper.includes('EXHAUST') || upper.includes('MAX ATTEMPTS')) {
    return ReasonCategory.RETRIES_EXHAUSTED;
  }
  if (upper.includes('PAYLOAD') || upper.includes('SCHEMA')) {
    return ReasonCategory.INVALID_PAYLOAD;
  }
  if (upper.includes('RECIPIENT') || upper.includes('DESTINATION') || upper.includes('PHONE') || upper.includes('EMAIL') || upper.includes('TOKEN')) {
    return ReasonCategory.INVALID_RECIPIENT;
  }
  if (upper.includes('PERMANENT') || upper.includes('NON-RETRYABLE')) {
    return ReasonCategory.PERMANENT_ERROR;
  }
  if (upper.includes('REJECT') || upper.includes('DECLINE')) {
    return ReasonCategory.PROVIDER_REJECTED;
  }
  return ReasonCategory.UNKNOWN;
}

// ============================================================================
// Core Prometheus Metrics Catalog
// ============================================================================

/**
 * Total notification requests accepted and persisted by the API.
 */
export const notificationsCreatedCounter = new Counter({
  name: 'notifications_created_total',
  help: 'Total notification aggregates created and persisted by the API',
  labelNames: ['priority', 'status'],
  registers: [register],
});

/**
 * Total delivery attempts started by channel workers.
 * Invariant: deliveries_attempted_total{service, channel}. Zero high-cardinality labels.
 */
export const deliveriesAttemptedCounter = new Counter({
  name: 'deliveries_attempted_total',
  help: 'Total delivery attempts initiated by channel workers',
  labelNames: ['service', 'channel'],
  registers: [register],
});

/**
 * Total successful deliveries confirmed by provider adapter.
 */
export const deliveriesSucceededCounter = new Counter({
  name: 'deliveries_succeeded_total',
  help: 'Total channel deliveries successfully sent and confirmed',
  labelNames: ['service', 'channel', 'provider'],
  registers: [register],
});

/**
 * Total delivery attempts that failed.
 */
export const deliveriesFailedCounter = new Counter({
  name: 'deliveries_failed_total',
  help: 'Total channel delivery attempts that failed',
  labelNames: ['service', 'channel', 'provider', 'error_code'],
  registers: [register],
});

/**
 * Total retry attempts scheduled with exponential backoff.
 */
export const retriesScheduledCounter = new Counter({
  name: 'retries_scheduled_total',
  help: 'Total deliveries scheduled for retry in transactional outbox',
  labelNames: ['service', 'channel'],
  registers: [register],
});

/**
 * Total dead-letter queue events routed on terminal failure.
 */
export const dlqEventsCounter = new Counter({
  name: 'dlq_events_total',
  help: 'Total terminal delivery failures routed to Dead-Letter Queue',
  labelNames: ['service', 'channel', 'reason_category'],
  registers: [register],
});

/**
 * Total requests dispatched to external provider adapters.
 */
export const providerRequestsCounter = new Counter({
  name: 'provider_requests_total',
  help: 'Total external provider adapter invocations',
  labelNames: ['service', 'provider', 'status'],
  registers: [register],
});

/**
 * Total provider adapter failures.
 */
export const providerFailuresCounter = new Counter({
  name: 'provider_failures_total',
  help: 'Total external provider adapter errors',
  labelNames: ['service', 'provider', 'error_type'],
  registers: [register],
});

/**
 * Total Kafka messages consumed.
 */
export const kafkaConsumedMessagesCounter = new Counter({
  name: 'kafka_consumed_messages_total',
  help: 'Total Kafka messages consumed across consumer groups',
  labelNames: ['service', 'topic', 'status'],
  registers: [register],
});

/**
 * Total Kafka consumer processing errors.
 */
export const kafkaProcessingErrorsCounter = new Counter({
  name: 'kafka_processing_errors_total',
  help: 'Total unhandled Kafka message processing errors',
  labelNames: ['service', 'topic'],
  registers: [register],
});

/**
 * Current backlog of pending outbox events.
 */
export const outboxEventsPendingGauge = new Gauge({
  name: 'outbox_events_pending',
  help: 'Current count of pending outbox events awaiting Kafka publication',
  labelNames: ['event_type'],
  registers: [register],
});

/**
 * Total outbox events successfully published to Kafka.
 */
export const outboxEventsPublishedCounter = new Counter({
  name: 'outbox_events_published_total',
  help: 'Total outbox events published to Kafka topics',
  labelNames: ['event_type'],
  registers: [register],
});

/**
 * Total outbox publishing failures.
 */
export const outboxEventsFailedCounter = new Counter({
  name: 'outbox_events_failed_total',
  help: 'Total failed attempts to publish outbox events to Kafka',
  labelNames: ['event_type'],
  registers: [register],
});

/**
 * Worker message processing duration in seconds.
 */
export const processingLatencyHistogram = new Histogram({
  name: 'processing_latency_seconds',
  help: 'End-to-end processing latency for a notification delivery message',
  labelNames: ['service', 'channel'],
  buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  registers: [register],
});

/**
 * Provider adapter network latency in seconds.
 */
export const providerLatencyHistogram = new Histogram({
  name: 'provider_latency_seconds',
  help: 'External provider adapter invocation latency',
  labelNames: ['service', 'provider'],
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30],
  registers: [register],
});

/**
 * Calculated retry delay backoff in seconds.
 */
export const retryDelayHistogram = new Histogram({
  name: 'retry_delay_seconds',
  help: 'Calculated exponential backoff delay assigned to retry events',
  labelNames: ['channel'],
  buckets: [1, 5, 15, 30, 60, 120, 300, 600, 900],
  registers: [register],
});

/**
 * HTTP request duration in seconds for API endpoints.
 */
export const httpRequestDurationHistogram = new Histogram({
  name: 'http_request_duration_seconds',
  help: 'Duration of HTTP requests through API server',
  labelNames: ['method', 'route', 'status_code'],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [register],
});

// ============================================================================
// Phase 10: Notification Product Layer Metrics
// ============================================================================

export const templatesCreatedCounter = new Counter({
  name: 'templates_created_total',
  help: 'Total notification templates created',
  labelNames: ['channel'],
  registers: [register],
});

export const templateRenderFailuresCounter = new Counter({
  name: 'template_render_failures_total',
  help: 'Total template rendering failures',
  labelNames: ['channel', 'reason'],
  registers: [register],
});

export const preferenceUpdatesCounter = new Counter({
  name: 'preference_updates_total',
  help: 'Total user notification preference updates',
  labelNames: ['category', 'channel', 'enabled'],
  registers: [register],
});

export const idempotencyHitsCounter = new Counter({
  name: 'idempotency_hits_total',
  help: 'Total idempotent request replays served',
  registers: [register],
});

export const idempotencyConflictsCounter = new Counter({
  name: 'idempotency_conflicts_total',
  help: 'Total requests rejected due to idempotency key payload mismatch',
  registers: [register],
});

export const scheduledNotificationsCreatedCounter = new Counter({
  name: 'scheduled_notifications_created_total',
  help: 'Total notifications scheduled for future delivery',
  registers: [register],
});

export const scheduledNotificationsClaimedCounter = new Counter({
  name: 'scheduled_notifications_claimed_total',
  help: 'Total scheduled notifications claimed by scheduler workers',
  registers: [register],
});

export const scheduledNotificationsDispatchedCounter = new Counter({
  name: 'scheduled_notifications_dispatched_total',
  help: 'Total scheduled notifications converted into outbox delivery events',
  registers: [register],
});

export const scheduledNotificationsCancelledCounter = new Counter({
  name: 'scheduled_notifications_cancelled_total',
  help: 'Total scheduled notifications cancelled prior to dispatch',
  registers: [register],
});

export const schedulerDispatchDelayHistogram = new Histogram({
  name: 'scheduler_dispatch_delay_seconds',
  help: 'Difference between scheduled time and actual dispatch time in seconds',
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30],
  registers: [register],
});

// ============================================================================
// Phase 11: Multi-Tenant Quotas, Rate Limiting & Backpressure Metrics
// (Strictly bounded labels; tenantId and entity IDs are NEVER used as labels)
// ============================================================================

/**
 * Total API requests rejected due to tenant token bucket rate-limit exhaustion.
 * tenant_type: 'custom' | 'default' (bounded cardinality)
 */
export const tenantRateLimitRejectionsCounter = new Counter({
  name: 'tenant_rate_limit_rejections_total',
  help: 'Total API requests rejected by tenant rate limiter',
  labelNames: ['tenant_type'],
  registers: [register],
});

/**
 * Total notification ingestion requests rejected due to notification quota exhaustion.
 * tenant_type: 'custom' | 'default', quota_window: 'minute' | 'day'
 */
export const notificationQuotaRejectionsCounter = new Counter({
  name: 'notification_quota_rejections_total',
  help: 'Total notification requests rejected by notification ingestion quota',
  labelNames: ['tenant_type', 'quota_window'],
  registers: [register],
});

/**
 * Aggregate gauge of current remaining token bucket allowance across active tenants.
 * (Aggregate / system-level only to prevent cardinality explosion)
 */
export const rateLimitRemainingGauge = new Gauge({
  name: 'rate_limit_remaining',
  help: 'System-level sample of remaining rate limit token allowance',
  registers: [register],
});

/**
 * Current number of in-flight provider calls being executed by a channel worker.
 */
export const workerInFlightGauge = new Gauge({
  name: 'worker_in_flight',
  help: 'Current active concurrent provider calls within channel worker semaphore',
  labelNames: ['service', 'channel'],
  registers: [register],
});

/**
 * Configured maximum concurrency limit for a channel worker instance.
 */
export const workerConcurrencyLimitGauge = new Gauge({
  name: 'worker_concurrency_limit',
  help: 'Configured maximum concurrency capacity for channel worker',
  labelNames: ['service', 'channel'],
  registers: [register],
});

/**
 * Total provider rate-limit rejections encountered during delivery attempts.
 */
export const providerRateLimitCounter = new Counter({
  name: 'provider_rate_limit_total',
  help: 'Total 429 rate limit responses received from downstream notification providers',
  labelNames: ['service', 'channel'],
  registers: [register],
});

/**
 * Kafka consumer lag approximation for channel workers.
 */
export const kafkaConsumerLagGauge = new Gauge({
  name: 'kafka_consumer_lag',
  help: 'Observed or estimated consumer lag for topic partitions',
  labelNames: ['service', 'topic'],
  registers: [register],
});

/**
 * Number of scheduled notifications currently overdue awaiting scheduler dispatch.
 */
export const schedulerDueBacklogGauge = new Gauge({
  name: 'scheduler_due_backlog',
  help: 'Number of due scheduled notifications currently in backlog',
  registers: [register],
});

/**
 * Duration of notification ingestion processing up to acceptance.
 */
export const notificationIngestionDurationHistogram = new Histogram({
  name: 'notification_ingestion_duration_seconds',
  help: 'Latency of notification ingestion before persistence acceptance',
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5],
  registers: [register],
});


// ============================================================================
// Helpers
// ============================================================================

/**
 * Returns Prometheus scrape-compatible text output.
 */
export async function getMetrics(): Promise<string> {
  return register.metrics();
}

/**
 * Returns Prometheus content-type header string.
 */
export function getContentType(): string {
  return register.contentType;
}

/**
 * Clears all metrics in the registry (useful for unit tests).
 */
export function resetMetrics(): void {
  register.resetMetrics();
}

export { client };
