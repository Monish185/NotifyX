import { describe, it, expect, beforeEach, vi } from 'vitest';
import { validateEmailPayload, resolveEmailRecipient } from '../src/schemas.js';
import { BaseChannelWorker, MockEmailProvider } from '@notifyx/kafka';
import { Channel, EVENT_TYPES } from '@notifyx/shared';
import { prisma } from '@notifyx/database';

describe('Email Worker Unit Tests', () => {
  const provider = new MockEmailProvider();

  beforeEach(() => {
    provider.reset();
  });

  describe('Payload & Recipient Validation', () => {
    it('should validate valid email payload with body', () => {
      const result = validateEmailPayload({
        subject: 'Weekly Digest',
        body: 'Here is your weekly summary...',
      });
      expect(result.valid).toBe(true);
      expect(result.data?.subject).toBe('Weekly Digest');
    });

    it('should validate valid email payload with html', () => {
      const result = validateEmailPayload({
        subject: 'Welcome!',
        html: '<h1>Welcome to our service</h1>',
      });
      expect(result.valid).toBe(true);
    });

    it('should reject email payload without body, text, or html', () => {
      const result = validateEmailPayload({
        subject: 'Empty Email',
      });
      expect(result.valid).toBe(false);
      expect(result.error).toContain('At least one of body, text, or html');
    });

    it('should reject email payload with invalid to address', () => {
      const result = validateEmailPayload({
        subject: 'Invalid Recipient',
        body: 'Test',
        to: 'not-an-email',
      });
      expect(result.valid).toBe(false);
    });

    it('should resolve recipient from payload.to if present', () => {
      const res = resolveEmailRecipient(
        { id: 'user_1', email: 'fallback@example.com' },
        { to: 'explicit@example.com' }
      );
      expect(res.recipient).toBe('explicit@example.com');
    });

    it('should resolve recipient from user.email if payload lacks to', () => {
      const res = resolveEmailRecipient(
        { id: 'user_1', email: 'user@example.com' },
        {}
      );
      expect(res.recipient).toBe('user@example.com');
    });

    it('should return error if no email is found anywhere', () => {
      const res = resolveEmailRecipient({ id: 'user_1', email: null }, {});
      expect(res.recipient).toBeUndefined();
      expect(res.error).toBeDefined();
    });
  });

  describe('MockEmailProvider Idempotency', () => {
    it('should return cached result and not duplicate send for the same idempotency key', async () => {
      const req = {
        idempotencyKey: 'del_email_test_1',
        tenantId: 't1',
        userId: 'u1',
        recipient: 'user@example.com',
        payload: { subject: 'Test', body: 'Hello' },
        notificationId: 'n1',
        deliveryId: 'del_email_test_1',
      };

      const res1 = await provider.send(req);
      expect(res1.success).toBe(true);
      expect(provider.getSendCount('del_email_test_1')).toBe(1);

      // Second call with same idempotency key
      const res2 = await provider.send(req);
      expect(res2.success).toBe(true);
      expect(res2.providerMessageId).toBe(res1.providerMessageId);
      // Send history count recorded duplicate call, but returns cached result
      expect(provider.getSendCount('del_email_test_1')).toBe(2);
    });

    it('should return failure when simulated failure is enabled', async () => {
      provider.setFailureSimulation(true, 'SMTP connection timeout', true);

      const res = await provider.send({
        idempotencyKey: 'del_email_fail',
        tenantId: 't1',
        userId: 'u1',
        recipient: 'user@example.com',
        payload: { subject: 'Test', body: 'Hello' },
        notificationId: 'n1',
        deliveryId: 'del_email_fail',
      });

      expect(res.success).toBe(false);
      expect(res.error).toBe('SMTP connection timeout');
      expect(res.retryable).toBe(true);
    });
  });

  describe('Channel Worker Routing & Processing', () => {
    const worker = new BaseChannelWorker({
      channel: Channel.EMAIL,
      serviceName: 'email-worker',
      consumerGroup: 'notifyx-email-workers',
      brokers: ['localhost:9092'],
      healthPort: 3904,
      provider,
      validatePayload: validateEmailPayload,
      resolveRecipient: resolveEmailRecipient,
    });

    it('should skip non-EMAIL events', async () => {
      const mockContext: any = {
        topic: 'notification.delivery.requested',
        partition: 0,
        offset: '1',
        parsedPayload: {
          eventId: 'evt_1',
          eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
          version: 1,
          occurredAt: new Date().toISOString(),
          tenantId: 't1',
          notificationId: 'n1',
          deliveryId: 'd1',
          userId: 'u1',
          channel: Channel.PUSH, // Not EMAIL
          priority: 'NORMAL',
          payload: { title: 'Push Title', body: 'Push Body' },
        },
      };

      const result = await worker.processEvent(mockContext);
      expect(result.status).toBe('SKIPPED');
    });

    it('should reject malformed event envelope', async () => {
      const mockContext: any = {
        topic: 'notification.delivery.requested',
        partition: 0,
        offset: '2',
        parsedPayload: {
          eventId: 'evt_2',
          // missing required fields
        },
      };

      const result = await worker.processEvent(mockContext);
      expect(result.status).toBe('INVALID');
    });
  });
});
