import { describe, it, expect, beforeEach } from 'vitest';
import { validatePushPayload, resolvePushRecipient } from '../src/schemas.js';
import { BaseChannelWorker, MockPushProvider } from '@notifyx/kafka';
import { Channel, EVENT_TYPES } from '@notifyx/shared';

describe('Push Worker Unit Tests', () => {
  const provider = new MockPushProvider();

  beforeEach(() => {
    provider.reset();
  });

  describe('Payload & Recipient Validation', () => {
    it('should validate valid push payload', () => {
      const result = validatePushPayload({
        title: 'New Order',
        body: 'You have received order #456',
        data: { orderId: '456' },
      });
      expect(result.valid).toBe(true);
      expect(result.data?.title).toBe('New Order');
    });

    it('should reject push payload missing body or title', () => {
      const result = validatePushPayload({
        title: 'Missing Body',
      });
      expect(result.valid).toBe(false);
      expect(result.error).toContain('Body is required');
    });

    it('should resolve recipient from explicit deviceToken', () => {
      const res = resolvePushRecipient(
        { id: 'user_1' },
        { deviceToken: 'fcm_token_123' }
      );
      expect(res.recipient).toBe('fcm_token_123');
    });

    it('should fallback to synthetic token based on user id if none in payload', () => {
      const res = resolvePushRecipient({ id: 'user_1' }, {});
      expect(res.recipient).toBe('device_token_user_user_1');
    });
  });

  describe('MockPushProvider Idempotency', () => {
    it('should deduplicate side-effect on duplicate idempotencyKey', async () => {
      const req = {
        idempotencyKey: 'del_push_test_1',
        tenantId: 't1',
        userId: 'u1',
        recipient: 'token_123',
        payload: { title: 'Order', body: 'Ready' },
        notificationId: 'n1',
        deliveryId: 'del_push_test_1',
      };

      const res1 = await provider.send(req);
      expect(res1.success).toBe(true);

      const res2 = await provider.send(req);
      expect(res2.success).toBe(true);
      expect(res2.providerMessageId).toBe(res1.providerMessageId);
      expect(provider.getSendCount('del_push_test_1')).toBe(2);
    });
  });

  describe('Channel Worker Routing', () => {
    const worker = new BaseChannelWorker({
      channel: Channel.PUSH,
      serviceName: 'push-worker',
      consumerGroup: 'notifyx-push-workers',
      brokers: ['localhost:9092'],
      healthPort: 3905,
      provider,
      validatePayload: validatePushPayload,
      resolveRecipient: resolvePushRecipient,
    });

    it('should skip non-PUSH events', async () => {
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
          channel: Channel.EMAIL, // Not PUSH
          priority: 'NORMAL',
          payload: { subject: 'Hi', body: 'There' },
        },
      };

      const result = await worker.processEvent(mockContext);
      expect(result.status).toBe('SKIPPED');
    });
  });
});
