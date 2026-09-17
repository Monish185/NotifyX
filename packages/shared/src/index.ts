/**
 * @notifyx/shared
 * Common types, enums, and constants for NotifyX.
 */

export const NotificationStatus = {
  PENDING: 'PENDING',
  QUEUED: 'QUEUED',
  PROCESSING: 'PROCESSING',
  SENT: 'SENT',
  DELIVERED: 'DELIVERED',
  RETRY_SCHEDULED: 'RETRY_SCHEDULED',
  FAILED: 'FAILED',
  SCHEDULED: 'SCHEDULED',
  CANCELLED: 'CANCELLED',
} as const;

export type NotificationStatus =
  (typeof NotificationStatus)[keyof typeof NotificationStatus];

export const NotificationCategory = {
  TRANSACTIONAL: 'TRANSACTIONAL',
  SECURITY: 'SECURITY',
  SYSTEM: 'SYSTEM',
  MARKETING: 'MARKETING',
} as const;

export type NotificationCategory =
  (typeof NotificationCategory)[keyof typeof NotificationCategory];

export const TemplateStatus = {
  DRAFT: 'DRAFT',
  ACTIVE: 'ACTIVE',
  ARCHIVED: 'ARCHIVED',
} as const;

export type TemplateStatus =
  (typeof TemplateStatus)[keyof typeof TemplateStatus];

export const ScheduledStatus = {
  SCHEDULED: 'SCHEDULED',
  PROCESSING: 'PROCESSING',
  DISPATCHED: 'DISPATCHED',
  COMPLETED: 'COMPLETED',
  CANCELLED: 'CANCELLED',
} as const;

export type ScheduledStatus =
  (typeof ScheduledStatus)[keyof typeof ScheduledStatus];

export * from './templates.js';

export const Channel = {
  EMAIL: 'EMAIL',
  PUSH: 'PUSH',
  SMS: 'SMS',
  IN_APP: 'IN_APP',
} as const;

export type Channel = (typeof Channel)[keyof typeof Channel];

export const Priority = {
  CRITICAL: 'CRITICAL',
  HIGH: 'HIGH',
  NORMAL: 'NORMAL',
  LOW: 'LOW',
} as const;

export type Priority = (typeof Priority)[keyof typeof Priority];

export const ApiKeyEnv = {
  LIVE: 'LIVE',
  TEST: 'TEST',
} as const;

export type ApiKeyEnv = (typeof ApiKeyEnv)[keyof typeof ApiKeyEnv];

export const OutboxStatus = {
  PENDING: 'PENDING',
  PUBLISHED: 'PUBLISHED',
  FAILED: 'FAILED',
} as const;

export type OutboxStatus = (typeof OutboxStatus)[keyof typeof OutboxStatus];

export const TOPICS = {
  NOTIFICATION_DELIVERY_REQUESTED: 'notification.delivery.requested',
  NOTIFICATION_DELIVERY_RETRY: 'notification.delivery.retry',
  NOTIFICATION_DELIVERY_DLQ: 'notification.delivery.dlq',
} as const;

export type TopicName = (typeof TOPICS)[keyof typeof TOPICS];

export const EVENT_TYPES = {
  NOTIFICATION_DELIVERY_REQUESTED: 'notification.delivery.requested',
  NOTIFICATION_DELIVERY_RETRY: 'notification.delivery.retry',
  NOTIFICATION_DELIVERY_DLQ: 'notification.delivery.dlq',
} as const;

export type EventType = (typeof EVENT_TYPES)[keyof typeof EVENT_TYPES];

/**
 * Event Contract for notification delivery requested.
 * Serialized to JSON when published to Kafka topic `notification.delivery.requested`.
 * Each Kafka message represents one specific channel delivery for a notification.
 */
export interface NotificationDeliveryRequestedEvent {
  eventId: string;
  eventType: typeof EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED;
  version: 1;
  occurredAt: string; // ISO 8601
  tenantId: string;
  notificationId: string;
  deliveryId: string;
  userId: string;
  channel: Channel;
  priority: Priority;
  payload: Record<string, unknown>;
  attempt?: number;
  correlationId?: string;
}

/**
 * Event Contract for scheduled retry notification delivery.
 * Serialized to JSON when published to Kafka topic `notification.delivery.retry`.
 */
export interface NotificationDeliveryRetryEvent {
  eventId: string;
  eventType: typeof EVENT_TYPES.NOTIFICATION_DELIVERY_RETRY;
  version: 1;
  occurredAt: string; // ISO 8601
  tenantId: string;
  notificationId: string;
  deliveryId: string;
  userId: string;
  channel: Channel;
  priority: Priority;
  payload: Record<string, unknown>;
  attempt: number;
  maxAttempts: number;
  scheduledAt: string;
  nextAttemptAt: string;
  originalEventId?: string;
  correlationId?: string;
}

/**
 * Event Contract for terminal Dead-Letter Queue (DLQ) notification delivery.
 * Serialized to JSON when published to Kafka topic `notification.delivery.dlq`.
 */
export interface NotificationDeliveryDlqEvent {
  eventId: string;
  eventType: typeof EVENT_TYPES.NOTIFICATION_DELIVERY_DLQ;
  version: 1;
  occurredAt: string; // ISO 8601
  tenantId: string;
  notificationId: string;
  deliveryId: string;
  userId: string;
  channel: Channel;
  priority: Priority;
  payload: Record<string, unknown>;
  attemptCount: number;
  reason: string;
  errorCode?: string;
  failedAt: string;
  correlationId?: string;
}
