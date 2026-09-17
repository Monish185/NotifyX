import twilio, { type Twilio } from 'twilio';
import {
  type ChannelProvider,
  type ProviderSendRequest,
  type ProviderResult,
} from '@notifyx/kafka';
import { logger } from '@notifyx/logger';
import { type SmsPayload } from './schemas.js';

export interface TwilioSmsProviderOptions {
  accountSid: string;
  authToken: string;
  fromNumber: string;
  twilioClient?: Twilio; // Optional injected instance for unit tests
}

/**
 * Standard E.164 phone number pattern:
 * Leading '+' followed by 1 to 15 digits (starting with non-zero).
 */
const E164_REGEX = /^\+[1-9]\d{1,14}$/;

/**
 * Safely masks sensitive phone numbers for logging.
 * Example: "+15551234567" -> "***-***-4567"
 */
export function redactPhoneNumber(phone: string): string {
  if (!phone || phone.length <= 4) {
    return '***';
  }
  return `***-***-${phone.slice(-4)}`;
}

/**
 * Classifies Twilio SDK errors into retryable (transient) vs non-retryable (permanent).
 *
 * Prioritizes official numeric Twilio error codes over HTTP status:
 * - Transient:
 *     Code 20429 (Too Many Requests / rate limit), network timeouts (ECONNRESET, ETIMEDOUT, ENOTFOUND),
 *     HTTP 429 / 5xx.
 * - Permanent:
 *     Code 21211 (Invalid phone number), 21614 (Not a valid mobile number),
 *     21610 (Message opted-out / blacklist), 20003 (Authentication error),
 *     20404 (Resource not found), 21602 (Empty body), 21617 (Body too long),
 *     HTTP 400.
 */
export function classifyTwilioError(err: any): {
  retryable: boolean;
  code: string;
  message: string;
} {
  const code =
    err?.code !== undefined && err?.code !== null
      ? String(err.code)
      : err?.errorInfo?.code
        ? String(err.errorInfo.code)
        : 'twilio/unknown-error';

  const message = err?.message || 'Twilio SMS service error';
  const httpStatus = err?.status || err?.httpResponse?.statusCode;

  // 1. Transient / Retryable Twilio Error Codes
  if (
    code === '20429' || // Rate limit exceeded
    code === '20500' || // Internal server error
    code === '20503'    // Service unavailable
  ) {
    return { retryable: true, code, message };
  }

  // 2. Transport & Network Failures
  if (
    err?.code === 'ECONNRESET' ||
    err?.code === 'ETIMEDOUT' ||
    err?.code === 'ENOTFOUND' ||
    err?.code === 'EAI_AGAIN' ||
    err?.name === 'TimeoutError'
  ) {
    return { retryable: true, code: 'twilio/network-timeout', message };
  }

  // 3. Fallback HTTP Status Codes
  if (httpStatus === 429 || (httpStatus >= 500 && httpStatus <= 504)) {
    return { retryable: true, code: `http/${httpStatus}`, message };
  }

  // 4. Explicit Permanent Rejections
  if (
    code === '21211' || // Invalid 'To' phone number
    code === '21614' || // 'To' number is not a mobile number
    code === '21610' || // Recipient opted out / unsubscribed
    code === '21608' || // Unverified trial destination
    code === '20003' || // Permission denied / authentication failure
    code === '20404' || // Resource / Account not found
    code === '21602' || // Message body is required
    code === '21617'    // Message body exceeds maximum length
  ) {
    return { retryable: false, code, message };
  }

  // 5. Default fallback to non-retryable
  return { retryable: false, code, message };
}

/**
 * Real Twilio SMS Provider Adapter.
 *
 * IDEMPOTENCY & EXTERNAL SIDE-EFFECT BOUNDARY:
 * - Uses deliveryId as the logical idempotency key.
 * - Note: Twilio Messages API does NOT offer NotifyX exactly-once idempotency deduplication.
 *   The distributed boundary relies on NotifyX database-level state machine (delivery.status === 'DELIVERED')
 *   to avoid duplicate dispatch attempts.
 * - Lifecycle: The Twilio client is created once upon worker initialization and reused for all requests.
 */
export class TwilioSmsProvider implements ChannelProvider<SmsPayload> {
  readonly name = 'TwilioSmsProvider';
  private readonly client: Twilio;
  private readonly fromNumber: string;

  constructor(options: TwilioSmsProviderOptions) {
    this.fromNumber = options.fromNumber;
    this.client = options.twilioClient || twilio(options.accountSid, options.authToken);
  }

  async send(request: ProviderSendRequest<SmsPayload>): Promise<ProviderResult> {
    const { idempotencyKey, recipient, payload, tenantId, notificationId, deliveryId } =
      request;

    const redactedTo = redactPhoneNumber(recipient);

    // E.164 phone number validation (reject malformed input without guessing country codes)
    if (!E164_REGEX.test(recipient)) {
      const errorMsg = `Invalid recipient phone number format: "${redactedTo}". Twilio SMS requires E.164 format (e.g. +1234567890).`;
      logger.warn(
        {
          provider: this.name,
          deliveryId,
          recipient: redactedTo,
        },
        errorMsg
      );

      return {
        success: false,
        error: `[TWILIO_INVALID_PHONE] ${errorMsg}`,
        retryable: false,
      };
    }

    const body = payload.message || payload.body || payload.text;
    if (!body || body.trim().length === 0) {
      return {
        success: false,
        error: '[TWILIO_EMPTY_BODY] SMS body content cannot be empty',
        retryable: false,
      };
    }

    try {
      logger.info(
        {
          provider: this.name,
          deliveryId,
          notificationId,
          tenantId,
          idempotencyKey,
          to: redactedTo,
        },
        'Dispatching SMS via Twilio'
      );

      const message = await this.client.messages.create({
        to: recipient,
        from: this.fromNumber,
        body,
      });

      const providerMessageId = message.sid || `twilio_${deliveryId}`;

      logger.info(
        {
          provider: this.name,
          deliveryId,
          providerMessageId,
          status: message.status,
        },
        'Twilio accepted SMS delivery'
      );

      return {
        success: true,
        providerMessageId,
        metadata: {
          twilioSid: message.sid,
          status: message.status,
          dateCreated: message.dateCreated,
        },
      };
    } catch (err: any) {
      const { retryable, code, message: errorMsg } = classifyTwilioError(err);

      logger.error(
        {
          provider: this.name,
          deliveryId,
          error: errorMsg,
          errorCode: code,
          retryable,
          to: redactedTo,
        },
        'Twilio SMS delivery failed'
      );

      return {
        success: false,
        error: `[TWILIO_${code}] ${errorMsg}`,
        retryable,
      };
    }
  }
}
