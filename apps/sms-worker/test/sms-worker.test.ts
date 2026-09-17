import { describe, it, expect, beforeEach } from 'vitest';
import { validateSmsPayload, resolveSmsRecipient } from '../src/schemas.js';
import { BaseChannelWorker, MockSmsProvider } from '@notifyx/kafka';
import { Channel, EVENT_TYPES } from '@notifyx/shared';

describe('SMS Worker Unit Tests', () => {
  const provider = new MockSmsProvider();

  beforeEach(() => {
    provider.reset();
  });

  describe('Payload & Recipient Validation', () => {
    it('should validate valid sms payload', () => {
      const result = validateSmsPayload({
        message: 'Your verification code is 123456',
        phoneNumber: '+15551234567',
      });
      expect(result.valid).toBe(true);
    });

    it('should reject sms payload without message, body or text', () => {
      const result = validateSmsPayload({
        phoneNumber: '+15551234567',
      });
      expect(result.valid).toBe(false);
      expect(result.error).toContain('At least one of message, body, or text');
    });

    it('should resolve recipient from payload.phoneNumber', () => {
      const res = resolveSmsRecipient(
        { id: 'user_1', phone: '+10000000000' },
        { phoneNumber: '+15559998888' }
      );
      expect(res.recipient).toBe('+15559998888');
    });

    it('should fallback to user.phone if payload has no recipient', () => {
      const res = resolveSmsRecipient(
        { id: 'user_1', phone: '+10000000000' },
        {}
      );
      expect(res.recipient).toBe('+10000000000');
    });

    it('should return error if no phone number is found', () => {
      const res = resolveSmsRecipient({ id: 'user_1', phone: null }, {});
      expect(res.recipient).toBeUndefined();
      expect(res.error).toBeDefined();
    });
  });

  describe('MockSmsProvider Idempotency', () => {
    it('should deduplicate side-effects on duplicate idempotencyKey', async () => {
      const req = {
        idempotencyKey: 'del_sms_test_1',
        tenantId: 't1',
        userId: 'u1',
        recipient: '+15551234567',
        payload: { message: 'OTP: 9999' },
        notificationId: 'n1',
        deliveryId: 'del_sms_test_1',
      };

      const res1 = await provider.send(req);
      expect(res1.success).toBe(true);

      const res2 = await provider.send(req);
      expect(res2.success).toBe(true);
      expect(res2.providerMessageId).toBe(res1.providerMessageId);
      expect(provider.getSendCount('del_sms_test_1')).toBe(2);
    });
  });

  describe('Channel Worker Routing', () => {
    const worker = new BaseChannelWorker({
      channel: Channel.SMS,
      serviceName: 'sms-worker',
      consumerGroup: 'notifyx-sms-workers',
      brokers: ['localhost:9092'],
      healthPort: 3906,
      provider,
      validatePayload: validateSmsPayload,
      resolveRecipient: resolveSmsRecipient,
    });

    it('should skip non-SMS events', async () => {
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
          channel: Channel.EMAIL, // Not SMS
          priority: 'NORMAL',
          payload: { subject: 'Hi', body: 'There' },
        },
      };

      const result = await worker.processEvent(mockContext);
      expect(result.status).toBe('SKIPPED');
    });
  });
});
