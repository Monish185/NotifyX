import { describe, it, expect, vi } from 'vitest';
import { generateKeyPairSync } from 'crypto';
import {
  FcmPushProvider,
  classifyFcmError,
  redactFcmToken,
} from '../src/fcm-provider.js';
import { createPushProvider } from '../src/provider-factory.js';
import { MockPushProvider } from '@notifyx/kafka';
import type { EnvConfig } from '@notifyx/config';

// Generate a valid in-memory PKCS#8 RSA key for testing private key parsing
const { privateKey: testRsaKey } = generateKeyPairSync('rsa', {
  modulusLength: 2048,
  privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
});

describe('FcmPushProvider & Push Factory', () => {
  describe('createPushProvider factory', () => {
    it('returns MockPushProvider when PUSH_PROVIDER=mock', () => {
      const mockConfig = {
        PUSH_PROVIDER: 'mock',
      } as unknown as EnvConfig;

      const provider = createPushProvider(mockConfig);
      expect(provider).toBeInstanceOf(MockPushProvider);
      expect(provider.name).toBe('MockPushProvider');
    });

    it('returns FcmPushProvider when PUSH_PROVIDER=fcm with valid credentials and escaped newlines', () => {
      // Test key containing literal \n string representations as commonly passed from Docker/env
      const escapedKey = testRsaKey.replace(/\n/g, '\\n');

      const fcmConfig = {
        PUSH_PROVIDER: 'fcm',
        FIREBASE_PROJECT_ID: 'test-project',
        FIREBASE_CLIENT_EMAIL: 'test@test-project.iam.gserviceaccount.com',
        FIREBASE_PRIVATE_KEY: escapedKey,
      } as unknown as EnvConfig;

      const provider = createPushProvider(fcmConfig);
      expect(provider).toBeInstanceOf(FcmPushProvider);
      expect(provider.name).toBe('FcmPushProvider');
    });

    it('throws when PUSH_PROVIDER=fcm but credentials are missing', () => {
      const invalidConfig = {
        PUSH_PROVIDER: 'fcm',
        FIREBASE_PROJECT_ID: '',
        FIREBASE_CLIENT_EMAIL: '',
        FIREBASE_PRIVATE_KEY: '',
      } as unknown as EnvConfig;

      expect(() => createPushProvider(invalidConfig)).toThrow(
        /FIREBASE_PROJECT_ID, FIREBASE_CLIENT_EMAIL, and FIREBASE_PRIVATE_KEY are required/
      );
    });

    it('throws when PUSH_PROVIDER is an unknown value', () => {
      const invalidConfig = {
        PUSH_PROVIDER: 'apns-direct',
      } as unknown as EnvConfig;

      expect(() => createPushProvider(invalidConfig)).toThrow(
        /Unsupported PUSH_PROVIDER/
      );
    });
  });

  describe('Token Redaction', () => {
    it('redacts sensitive FCM registration tokens', () => {
      expect(redactFcmToken('bk3RNwTe3H0:CI2k_HHwgIpoDKCIZvvDMExUdFQ3P1')).toBe('bk3RNw...Q3P1');
      expect(redactFcmToken('short')).toBe('***');
      expect(redactFcmToken('')).toBe('***');
    });
  });

  describe('classifyFcmError', () => {
    it('classifies transient FCM errors as retryable', () => {
      expect(classifyFcmError({ code: 'messaging/server-unavailable' })).toMatchObject({
        retryable: true,
        code: 'messaging/server-unavailable',
      });

      expect(classifyFcmError({ code: 'messaging/quota-exceeded' })).toMatchObject({
        retryable: true,
        code: 'messaging/quota-exceeded',
      });

      expect(classifyFcmError({ code: 'messaging/internal-error' })).toMatchObject({
        retryable: true,
        code: 'messaging/internal-error',
      });

      expect(classifyFcmError({ code: 'messaging/device-message-rate-exceeded' })).toMatchObject({
        retryable: true,
        code: 'messaging/device-message-rate-exceeded',
      });

      expect(classifyFcmError({ code: 'ECONNRESET' })).toMatchObject({
        retryable: true,
        code: 'messaging/network-timeout',
      });

      expect(classifyFcmError({ code: 'ETIMEDOUT' })).toMatchObject({
        retryable: true,
        code: 'messaging/network-timeout',
      });

      expect(classifyFcmError({ status: 429 })).toMatchObject({
        retryable: true,
      });

      expect(classifyFcmError({ httpResponse: { statusCode: 503 } })).toMatchObject({
        retryable: true,
      });
    });

    it('classifies permanent FCM errors as non-retryable', () => {
      expect(classifyFcmError({ code: 'messaging/invalid-registration-token' })).toMatchObject({
        retryable: false,
        code: 'messaging/invalid-registration-token',
      });

      expect(classifyFcmError({ code: 'messaging/registration-token-not-registered' })).toMatchObject({
        retryable: false,
        code: 'messaging/registration-token-not-registered',
      });

      expect(classifyFcmError({ code: 'messaging/invalid-argument' })).toMatchObject({
        retryable: false,
        code: 'messaging/invalid-argument',
      });

      expect(classifyFcmError({ code: 'messaging/authentication-error' })).toMatchObject({
        retryable: false,
        code: 'messaging/authentication-error',
      });

      expect(classifyFcmError({ code: 'messaging/invalid-payload' })).toMatchObject({
        retryable: false,
        code: 'messaging/invalid-payload',
      });
    });
  });

  describe('FcmPushProvider.send', () => {
    it('successfully maps payload and dispatches push via messaging client', async () => {
      let sentMessage: any = null;
      const mockMessaging = {
        send: vi.fn().mockImplementation(async (msg) => {
          sentMessage = msg;
          return 'projects/test-project/messages/fcm-msg-id-12345';
        }),
      } as any;

      const provider = new FcmPushProvider({
        projectId: 'test-project',
        clientEmail: 'test@example.com',
        privateKey: 'test-key',
        messaging: mockMessaging,
      });

      const result = await provider.send({
        idempotencyKey: 'del-uuid-1',
        deliveryId: 'del-uuid-1',
        notificationId: 'notif-uuid-1',
        tenantId: 'tenant-1',
        userId: 'user-1',
        recipient: 'device_token_xyz_987654321',
        payload: {
          title: 'Order Shipped',
          body: 'Your package is on the way!',
          data: {
            orderId: 1042,
            trackingUrl: 'https://example.com/track',
            isActive: true,
          },
        },
      });

      expect(result.success).toBe(true);
      expect(result.providerMessageId).toBe('projects/test-project/messages/fcm-msg-id-12345');
      expect(sentMessage).toEqual({
        token: 'device_token_xyz_987654321',
        notification: {
          title: 'Order Shipped',
          body: 'Your package is on the way!',
        },
        data: {
          orderId: '1042',
          trackingUrl: 'https://example.com/track',
          isActive: 'true',
        },
      });
    });

    it('handles transient provider failures gracefully without crashing', async () => {
      const mockMessaging = {
        send: vi.fn().mockRejectedValue({
          code: 'messaging/server-unavailable',
          message: 'The Firebase server is temporarily unavailable',
        }),
      } as any;

      const provider = new FcmPushProvider({
        projectId: 'test-project',
        clientEmail: 'test@example.com',
        privateKey: 'test-key',
        messaging: mockMessaging,
      });

      const result = await provider.send({
        idempotencyKey: 'del-uuid-2',
        deliveryId: 'del-uuid-2',
        notificationId: 'notif-uuid-2',
        tenantId: 'tenant-1',
        userId: 'user-1',
        recipient: 'device_token_xyz_987654321',
        payload: {
          title: 'Alert',
          body: 'Server down',
        },
      });

      expect(result.success).toBe(false);
      expect(result.retryable).toBe(true);
      expect(result.error).toContain('FCM_messaging/server-unavailable');
    });

    it('handles permanent provider failures as non-retryable', async () => {
      const mockMessaging = {
        send: vi.fn().mockRejectedValue({
          code: 'messaging/registration-token-not-registered',
          message: 'Device registration token has expired',
        }),
      } as any;

      const provider = new FcmPushProvider({
        projectId: 'test-project',
        clientEmail: 'test@example.com',
        privateKey: 'test-key',
        messaging: mockMessaging,
      });

      const result = await provider.send({
        idempotencyKey: 'del-uuid-3',
        deliveryId: 'del-uuid-3',
        notificationId: 'notif-uuid-3',
        tenantId: 'tenant-1',
        userId: 'user-1',
        recipient: 'invalid_token_12345',
        payload: {
          title: 'Alert',
          body: 'Hello',
        },
      });

      expect(result.success).toBe(false);
      expect(result.retryable).toBe(false);
      expect(result.error).toContain('FCM_messaging/registration-token-not-registered');
    });
  });
});
