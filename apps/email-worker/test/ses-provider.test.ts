import { describe, it, expect, vi } from 'vitest';
import { SesEmailProvider, classifySesError } from '../src/ses-provider.js';
import { createEmailProvider } from '../src/provider-factory.js';
import { MockEmailProvider } from '@notifyx/kafka';
import type { EnvConfig } from '@notifyx/config';

describe('AWS SES Provider Unit Tests', () => {
  describe('createEmailProvider factory', () => {
    it('should return MockEmailProvider when EMAIL_PROVIDER is mock', () => {
      const config = {
        EMAIL_PROVIDER: 'mock',
      } as EnvConfig;

      const provider = createEmailProvider(config);
      expect(provider).toBeInstanceOf(MockEmailProvider);
      expect(provider.name).toBe('MockEmailProvider');
    });

    it('should return SesEmailProvider when EMAIL_PROVIDER is ses with valid config', () => {
      const config = {
        EMAIL_PROVIDER: 'ses',
        AWS_REGION: 'us-east-1',
        EMAIL_FROM_ADDRESS: 'notifications@notifyx.dev',
      } as EnvConfig;

      const provider = createEmailProvider(config);
      expect(provider).toBeInstanceOf(SesEmailProvider);
      expect(provider.name).toBe('aws-ses');
    });

    it('should throw error when EMAIL_PROVIDER is ses but region or fromAddress is missing', () => {
      const configMissingRegion = {
        EMAIL_PROVIDER: 'ses',
        EMAIL_FROM_ADDRESS: 'notifications@notifyx.dev',
      } as EnvConfig;

      expect(() => createEmailProvider(configMissingRegion)).toThrow(
        /AWS_REGION and EMAIL_FROM_ADDRESS are required/
      );

      const configMissingFrom = {
        EMAIL_PROVIDER: 'ses',
        AWS_REGION: 'us-east-1',
      } as EnvConfig;

      expect(() => createEmailProvider(configMissingFrom)).toThrow(
        /AWS_REGION and EMAIL_FROM_ADDRESS are required/
      );
    });

    it('should throw error for unsupported provider type', () => {
      const config = {
        EMAIL_PROVIDER: 'sendgrid' as any,
      } as EnvConfig;

      expect(() => createEmailProvider(config)).toThrow(
        /Unsupported EMAIL_PROVIDER/
      );
    });
  });

  describe('classifySesError', () => {
    it('should classify throttling errors as retryable', () => {
      expect(classifySesError({ name: 'ThrottlingException' }).retryable).toBe(true);
      expect(classifySesError({ name: 'Throttling' }).retryable).toBe(true);
      expect(classifySesError({ name: 'LimitExceededException' }).retryable).toBe(true);
      expect(classifySesError({ $metadata: { httpStatusCode: 429 } }).retryable).toBe(true);
    });

    it('should classify service outage errors as retryable', () => {
      expect(classifySesError({ name: 'ServiceUnavailable' }).retryable).toBe(true);
      expect(classifySesError({ name: 'ServiceUnavailableException' }).retryable).toBe(true);
      expect(classifySesError({ $metadata: { httpStatusCode: 503 } }).retryable).toBe(true);
      expect(classifySesError({ $metadata: { httpStatusCode: 500 } }).retryable).toBe(true);
    });

    it('should classify network / timeout errors as retryable', () => {
      expect(classifySesError({ code: 'ECONNRESET' }).retryable).toBe(true);
      expect(classifySesError({ code: 'ETIMEDOUT' }).retryable).toBe(true);
      expect(classifySesError({ name: 'TimeoutError' }).retryable).toBe(true);
    });

    it('should classify permanent rejections as non-retryable', () => {
      expect(classifySesError({ name: 'MessageRejected' }).retryable).toBe(false);
      expect(classifySesError({ name: 'MailFromDomainNotVerifiedException' }).retryable).toBe(false);
      expect(classifySesError({ name: 'AccountSendingPausedException' }).retryable).toBe(false);
      expect(classifySesError({ name: 'InvalidParameterValue' }).retryable).toBe(false);
    });
  });

  describe('SesEmailProvider.send', () => {
    it('should send email successfully and map payload correctly', async () => {
      const mockSend = vi.fn().mockResolvedValue({
        MessageId: 'ses-msg-12345678',
        $metadata: { httpStatusCode: 200 },
      });

      const mockSesClient: any = {
        send: mockSend,
        destroy: vi.fn(),
      };

      const provider = new SesEmailProvider({
        region: 'us-east-1',
        fromAddress: 'notifications@notifyx.dev',
        sesClient: mockSesClient,
      });

      const result = await provider.send({
        idempotencyKey: 'del_test_123',
        tenantId: 'tenant_1',
        userId: 'user_1',
        recipient: 'user@example.com',
        payload: {
          subject: 'Your Invoice',
          body: 'Plain text invoice content',
          html: '<h1>Your Invoice</h1><p>Content</p>',
        },
        notificationId: 'notif_1',
        deliveryId: 'del_test_123',
      });

      expect(result.success).toBe(true);
      expect(result.providerMessageId).toBe('ses-msg-12345678');
      expect(result.metadata?.awsMessageId).toBe('ses-msg-12345678');
      expect(result.metadata?.httpStatusCode).toBe(200);

      expect(mockSend).toHaveBeenCalledTimes(1);
      const command = mockSend.mock.calls[0][0];
      expect(command.input.Source).toBe('notifications@notifyx.dev');
      expect(command.input.Destination.ToAddresses).toEqual(['user@example.com']);
      expect(command.input.Message.Subject.Data).toBe('Your Invoice');
      expect(command.input.Message.Body.Text.Data).toBe('Plain text invoice content');
      expect(command.input.Message.Body.Html.Data).toBe('<h1>Your Invoice</h1><p>Content</p>');
    });

    it('should handle text-only payload correctly', async () => {
      const mockSend = vi.fn().mockResolvedValue({
        MessageId: 'ses-msg-text-only',
        $metadata: { httpStatusCode: 200 },
      });

      const mockSesClient: any = {
        send: mockSend,
        destroy: vi.fn(),
      };

      const provider = new SesEmailProvider({
        region: 'us-east-1',
        fromAddress: 'notifications@notifyx.dev',
        sesClient: mockSesClient,
      });

      const result = await provider.send({
        idempotencyKey: 'del_text_only',
        tenantId: 'tenant_1',
        userId: 'user_1',
        recipient: 'text@example.com',
        payload: {
          subject: 'Text Only',
          text: 'Plain text message',
        },
        notificationId: 'notif_2',
        deliveryId: 'del_text_only',
      });

      expect(result.success).toBe(true);
      const command = mockSend.mock.calls[0][0];
      expect(command.input.Message.Body.Text.Data).toBe('Plain text message');
      expect(command.input.Message.Body.Html).toBeUndefined();
    });

    it('should handle html-only payload with text fallback', async () => {
      const mockSend = vi.fn().mockResolvedValue({
        MessageId: 'ses-msg-html-only',
        $metadata: { httpStatusCode: 200 },
      });

      const mockSesClient: any = {
        send: mockSend,
        destroy: vi.fn(),
      };

      const provider = new SesEmailProvider({
        region: 'us-east-1',
        fromAddress: 'notifications@notifyx.dev',
        sesClient: mockSesClient,
      });

      const result = await provider.send({
        idempotencyKey: 'del_html_only',
        tenantId: 'tenant_1',
        userId: 'user_1',
        recipient: 'html@example.com',
        payload: {
          subject: 'HTML Only',
          html: '<h2>Special Announcement</h2>',
        },
        notificationId: 'notif_3',
        deliveryId: 'del_html_only',
      });

      expect(result.success).toBe(true);
      const command = mockSend.mock.calls[0][0];
      expect(command.input.Message.Body.Html.Data).toBe('<h2>Special Announcement</h2>');
    });

    it('should handle retryable throttling errors', async () => {
      const throttlingError = new Error('Rate exceeded');
      throttlingError.name = 'ThrottlingException';

      const mockSend = vi.fn().mockRejectedValue(throttlingError);
      const mockSesClient: any = {
        send: mockSend,
        destroy: vi.fn(),
      };

      const provider = new SesEmailProvider({
        region: 'us-east-1',
        fromAddress: 'notifications@notifyx.dev',
        sesClient: mockSesClient,
      });

      const result = await provider.send({
        idempotencyKey: 'del_throttle',
        tenantId: 'tenant_1',
        userId: 'user_1',
        recipient: 'user@example.com',
        payload: { subject: 'Test', body: 'Hello' },
        notificationId: 'notif_4',
        deliveryId: 'del_throttle',
      });

      expect(result.success).toBe(false);
      expect(result.retryable).toBe(true);
      expect(result.error).toContain('SES_ThrottlingException');
    });

    it('should handle permanent rejection errors', async () => {
      const rejectionError = new Error('Email address is not verified');
      rejectionError.name = 'MessageRejected';

      const mockSend = vi.fn().mockRejectedValue(rejectionError);
      const mockSesClient: any = {
        send: mockSend,
        destroy: vi.fn(),
      };

      const provider = new SesEmailProvider({
        region: 'us-east-1',
        fromAddress: 'notifications@notifyx.dev',
        sesClient: mockSesClient,
      });

      const result = await provider.send({
        idempotencyKey: 'del_rejected',
        tenantId: 'tenant_1',
        userId: 'user_1',
        recipient: 'unverified@example.com',
        payload: { subject: 'Test', body: 'Hello' },
        notificationId: 'notif_5',
        deliveryId: 'del_rejected',
      });

      expect(result.success).toBe(false);
      expect(result.retryable).toBe(false);
      expect(result.error).toContain('SES_MessageRejected');
    });
  });
});
