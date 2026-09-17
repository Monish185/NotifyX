import { describe, it, expect, vi } from 'vitest';
import {
  TwilioSmsProvider,
  classifyTwilioError,
  redactPhoneNumber,
} from '../src/twilio-provider.js';
import { createSmsProvider } from '../src/provider-factory.js';
import { MockSmsProvider } from '@notifyx/kafka';
import type { EnvConfig } from '@notifyx/config';

describe('TwilioSmsProvider & SMS Factory', () => {
  describe('createSmsProvider factory', () => {
    it('returns MockSmsProvider when SMS_PROVIDER=mock', () => {
      const mockConfig = {
        SMS_PROVIDER: 'mock',
      } as unknown as EnvConfig;

      const provider = createSmsProvider(mockConfig);
      expect(provider).toBeInstanceOf(MockSmsProvider);
      expect(provider.name).toBe('MockSmsProvider');
    });

    it('returns TwilioSmsProvider when SMS_PROVIDER=twilio with valid credentials', () => {
      const twilioConfig = {
        SMS_PROVIDER: 'twilio',
        TWILIO_ACCOUNT_SID: 'AC1234567890abcdef1234567890abcdef',
        TWILIO_AUTH_TOKEN: 'test-auth-token',
        TWILIO_FROM_NUMBER: '+15551234567',
      } as unknown as EnvConfig;

      const provider = createSmsProvider(twilioConfig);
      expect(provider).toBeInstanceOf(TwilioSmsProvider);
      expect(provider.name).toBe('TwilioSmsProvider');
    });

    it('throws when SMS_PROVIDER=twilio but credentials are missing', () => {
      const invalidConfig = {
        SMS_PROVIDER: 'twilio',
        TWILIO_ACCOUNT_SID: '',
        TWILIO_AUTH_TOKEN: '',
        TWILIO_FROM_NUMBER: '',
      } as unknown as EnvConfig;

      expect(() => createSmsProvider(invalidConfig)).toThrow(
        /TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, and TWILIO_FROM_NUMBER are required/
      );
    });

    it('throws when SMS_PROVIDER is an unknown value', () => {
      const invalidConfig = {
        SMS_PROVIDER: 'vonage',
      } as unknown as EnvConfig;

      expect(() => createSmsProvider(invalidConfig)).toThrow(
        /Unsupported SMS_PROVIDER/
      );
    });
  });

  describe('Phone Number Redaction', () => {
    it('masks phone numbers safely in log statements', () => {
      expect(redactPhoneNumber('+15551234567')).toBe('***-***-4567');
      expect(redactPhoneNumber('+919876543210')).toBe('***-***-3210');
      expect(redactPhoneNumber('123')).toBe('***');
      expect(redactPhoneNumber('')).toBe('***');
    });
  });

  describe('classifyTwilioError', () => {
    it('classifies transient Twilio errors as retryable', () => {
      expect(classifyTwilioError({ code: 20429 })).toMatchObject({
        retryable: true,
        code: '20429',
      });

      expect(classifyTwilioError({ code: '20500' })).toMatchObject({
        retryable: true,
        code: '20500',
      });

      expect(classifyTwilioError({ code: '20503' })).toMatchObject({
        retryable: true,
        code: '20503',
      });

      expect(classifyTwilioError({ code: 'ECONNRESET' })).toMatchObject({
        retryable: true,
        code: 'twilio/network-timeout',
      });

      expect(classifyTwilioError({ code: 'ETIMEDOUT' })).toMatchObject({
        retryable: true,
        code: 'twilio/network-timeout',
      });

      expect(classifyTwilioError({ status: 429 })).toMatchObject({
        retryable: true,
      });

      expect(classifyTwilioError({ status: 503 })).toMatchObject({
        retryable: true,
      });
    });

    it('classifies permanent Twilio errors as non-retryable', () => {
      expect(classifyTwilioError({ code: 21211 })).toMatchObject({
        retryable: false,
        code: '21211',
      });

      expect(classifyTwilioError({ code: 21614 })).toMatchObject({
        retryable: false,
        code: '21614',
      });

      expect(classifyTwilioError({ code: 21610 })).toMatchObject({
        retryable: false,
        code: '21610',
      });

      expect(classifyTwilioError({ code: 20003 })).toMatchObject({
        retryable: false,
        code: '20003',
      });

      expect(classifyTwilioError({ code: 20404 })).toMatchObject({
        retryable: false,
        code: '20404',
      });

      expect(classifyTwilioError({ code: 21602 })).toMatchObject({
        retryable: false,
        code: '21602',
      });

      expect(classifyTwilioError({ status: 400 })).toMatchObject({
        retryable: false,
      });
    });
  });

  describe('TwilioSmsProvider.send', () => {
    it('successfully maps payload and dispatches SMS via Twilio client', async () => {
      let createdPayload: any = null;
      const mockTwilioClient = {
        messages: {
          create: vi.fn().mockImplementation(async (params) => {
            createdPayload = params;
            return {
              sid: 'SM1234567890abcdef1234567890abcdef',
              status: 'queued',
              dateCreated: new Date().toISOString(),
            };
          }),
        },
      } as any;

      const provider = new TwilioSmsProvider({
        accountSid: 'AC123',
        authToken: 'auth123',
        fromNumber: '+15550000000',
        twilioClient: mockTwilioClient,
      });

      const result = await provider.send({
        idempotencyKey: 'del-sms-1',
        deliveryId: 'del-sms-1',
        notificationId: 'notif-sms-1',
        tenantId: 'tenant-1',
        userId: 'user-1',
        recipient: '+15551234567',
        payload: {
          message: 'Your verification code is 492018',
        },
      });

      expect(result.success).toBe(true);
      expect(result.providerMessageId).toBe('SM1234567890abcdef1234567890abcdef');
      expect(createdPayload).toEqual({
        to: '+15551234567',
        from: '+15550000000',
        body: 'Your verification code is 492018',
      });
    });

    it('rejects non-E.164 phone numbers as permanent failure without calling Twilio API', async () => {
      const mockTwilioClient = {
        messages: {
          create: vi.fn(),
        },
      } as any;

      const provider = new TwilioSmsProvider({
        accountSid: 'AC123',
        authToken: 'auth123',
        fromNumber: '+15550000000',
        twilioClient: mockTwilioClient,
      });

      const result = await provider.send({
        idempotencyKey: 'del-sms-2',
        deliveryId: 'del-sms-2',
        notificationId: 'notif-sms-2',
        tenantId: 'tenant-1',
        userId: 'user-1',
        recipient: '5551234567', // Missing leading '+'
        payload: {
          message: 'Hello',
        },
      });

      expect(result.success).toBe(false);
      expect(result.retryable).toBe(false);
      expect(result.error).toContain('TWILIO_INVALID_PHONE');
      expect(mockTwilioClient.messages.create).not.toHaveBeenCalled();
    });

    it('handles transient provider failures gracefully', async () => {
      const mockTwilioClient = {
        messages: {
          create: vi.fn().mockRejectedValue({
            code: 20429,
            message: 'Too many concurrent requests',
            status: 429,
          }),
        },
      } as any;

      const provider = new TwilioSmsProvider({
        accountSid: 'AC123',
        authToken: 'auth123',
        fromNumber: '+15550000000',
        twilioClient: mockTwilioClient,
      });

      const result = await provider.send({
        idempotencyKey: 'del-sms-3',
        deliveryId: 'del-sms-3',
        notificationId: 'notif-sms-3',
        tenantId: 'tenant-1',
        userId: 'user-1',
        recipient: '+15551234567',
        payload: {
          message: 'Important alert',
        },
      });

      expect(result.success).toBe(false);
      expect(result.retryable).toBe(true);
      expect(result.error).toContain('TWILIO_20429');
    });

    it('handles permanent provider failures gracefully', async () => {
      const mockTwilioClient = {
        messages: {
          create: vi.fn().mockRejectedValue({
            code: 21211,
            message: 'Invalid phone number',
            status: 400,
          }),
        },
      } as any;

      const provider = new TwilioSmsProvider({
        accountSid: 'AC123',
        authToken: 'auth123',
        fromNumber: '+15550000000',
        twilioClient: mockTwilioClient,
      });

      const result = await provider.send({
        idempotencyKey: 'del-sms-4',
        deliveryId: 'del-sms-4',
        notificationId: 'notif-sms-4',
        tenantId: 'tenant-1',
        userId: 'user-1',
        recipient: '+15559999999',
        payload: {
          message: 'Hello',
        },
      });

      expect(result.success).toBe(false);
      expect(result.retryable).toBe(false);
      expect(result.error).toContain('TWILIO_21211');
    });
  });
});
