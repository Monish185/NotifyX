import { initializeApp, getApps, cert } from 'firebase-admin/app';
import { getMessaging, type Messaging, type Message } from 'firebase-admin/messaging';
import {
  type ChannelProvider,
  type ProviderSendRequest,
  type ProviderResult,
} from '@notifyx/kafka';
import { logger } from '@notifyx/logger';
import { type PushPayload } from './schemas.js';

export interface FcmPushProviderOptions {
  projectId: string;
  clientEmail: string;
  privateKey: string;
  messaging?: Messaging; // Optional injected instance for unit tests
}

/**
 * Safely masks sensitive FCM device registration tokens for logs.
 * Example: "fcm_token_1234567890" -> "fcm_to...7890"
 */
export function redactFcmToken(token: string): string {
  if (!token || token.length <= 10) {
    return '***';
  }
  return `${token.slice(0, 6)}...${token.slice(-4)}`;
}

/**
 * Classifies Firebase Admin SDK errors into retryable (transient) vs non-retryable (permanent).
 *
 * Prioritizes official Firebase error codes over generic HTTP status codes:
 * - Transient:
 *     messaging/server-unavailable, messaging/quota-exceeded, messaging/internal-error,
 *     network timeouts (ECONNRESET, ETIMEDOUT, ENOTFOUND, TimeoutError), HTTP 429 / 5xx.
 * - Permanent:
 *     messaging/invalid-registration-token, messaging/registration-token-not-registered,
 *     messaging/invalid-argument, messaging/authentication-error, messaging/invalid-payload,
 *     HTTP 400.
 */
export function classifyFcmError(err: any): {
  retryable: boolean;
  code: string;
  message: string;
} {
  const code =
    err?.code ||
    err?.errorInfo?.code ||
    (typeof err?.code === 'string' ? err.code : 'messaging/unknown-error');
  const message = err?.message || 'Firebase Cloud Messaging error';
  const httpStatus = err?.httpResponse?.statusCode || err?.status;

  // 1. Transient / Retryable FCM Error Codes
  if (
    code === 'messaging/server-unavailable' ||
    code === 'messaging/quota-exceeded' ||
    code === 'messaging/internal-error' ||
    code === 'messaging/device-message-rate-exceeded' ||
    code === 'messaging/topics-message-rate-exceeded'
  ) {
    return { retryable: true, code, message };
  }

  // 2. Transport & Network Failures
  if (
    err?.code === 'ECONNRESET' ||
    err?.code === 'ETIMEDOUT' ||
    err?.code === 'ENOTFOUND' ||
    err?.code === 'EAI_AGAIN' ||
    err?.name === 'TimeoutError' ||
    code === 'app/network-timeout' ||
    code === 'app/network-error'
  ) {
    return { retryable: true, code: 'messaging/network-timeout', message };
  }

  // 3. Fallback HTTP Status Codes
  if (httpStatus === 429 || (httpStatus >= 500 && httpStatus <= 504)) {
    return { retryable: true, code: `http/${httpStatus}`, message };
  }

  // 4. Explicit Permanent Rejections
  if (
    code === 'messaging/invalid-registration-token' ||
    code === 'messaging/registration-token-not-registered' ||
    code === 'messaging/invalid-argument' ||
    code === 'messaging/authentication-error' ||
    code === 'messaging/invalid-payload' ||
    code === 'messaging/mismatched-credential'
  ) {
    return { retryable: false, code, message };
  }

  // 5. Default fallback to non-retryable
  return { retryable: false, code, message };
}

/**
 * Real Firebase Cloud Messaging (FCM) Push Provider Adapter.
 *
 * IDEMPOTENCY & EXTERNAL SIDE-EFFECT BOUNDARY:
 * - Uses deliveryId as the logical idempotency key.
 * - Note: Firebase Cloud Messaging does NOT provide NotifyX exactly-once idempotency deduplication.
 *   The distributed boundary relies on NotifyX database-level state machine (delivery.status === 'DELIVERED')
 *   to avoid duplicate dispatch attempts.
 * - Lifecycle: The Firebase App and Messaging client are initialized once and reused across worker requests.
 */
export class FcmPushProvider implements ChannelProvider<PushPayload> {
  readonly name = 'FcmPushProvider';
  private readonly messaging: Messaging;

  constructor(options: FcmPushProviderOptions) {
    if (options.messaging) {
      this.messaging = options.messaging;
      return;
    }

    // Safely normalize multiline private key without mangling literal \n vs real newlines
    const rawKey = options.privateKey;
    const formattedPrivateKey = rawKey.includes('\\n')
      ? rawKey.replace(/\\n/g, '\n')
      : rawKey;

    const appName = 'notifyx-push-fcm';
    const existingApp = getApps().find((a) => a.name === appName);

    const app =
      existingApp ||
      initializeApp(
        {
          credential: cert({
            projectId: options.projectId,
            clientEmail: options.clientEmail,
            privateKey: formattedPrivateKey,
          }),
        },
        appName
      );

    this.messaging = getMessaging(app);
  }

  async send(request: ProviderSendRequest<PushPayload>): Promise<ProviderResult> {
    const { idempotencyKey, recipient, payload, tenantId, notificationId, deliveryId } =
      request;

    // Use canonical recipient resolved by worker (device registration token)
    const token = recipient;
    const redactedToken = redactFcmToken(token);

    // Build FCM message payload
    const message: Message = {
      token,
      notification: {
        title: payload.title,
        body: payload.body,
      },
    };

    // Firebase requires data dictionary values to be strings
    if (payload.data && typeof payload.data === 'object') {
      const stringData: Record<string, string> = {};
      for (const [key, val] of Object.entries(payload.data)) {
        if (val !== undefined && val !== null) {
          stringData[key] = typeof val === 'string' ? val : JSON.stringify(val);
        }
      }
      message.data = stringData;
    }

    try {
      logger.info(
        {
          provider: this.name,
          deliveryId,
          notificationId,
          tenantId,
          idempotencyKey,
          recipientToken: redactedToken,
        },
        'Dispatching push notification via Firebase Cloud Messaging'
      );

      const fcmMessageId = await this.messaging.send(message);
      const providerMessageId = fcmMessageId || `fcm_${deliveryId}`;

      logger.info(
        {
          provider: this.name,
          deliveryId,
          providerMessageId,
          status: 'accepted',
        },
        'Firebase Cloud Messaging accepted push delivery'
      );

      return {
        success: true,
        providerMessageId,
        metadata: {
          fcmMessageId,
        },
      };
    } catch (err: any) {
      const { retryable, code, message: errorMsg } = classifyFcmError(err);

      logger.error(
        {
          provider: this.name,
          deliveryId,
          error: errorMsg,
          errorCode: code,
          retryable,
          recipientToken: redactedToken,
        },
        'Firebase Cloud Messaging delivery failed'
      );

      return {
        success: false,
        error: `[FCM_${code}] ${errorMsg}`,
        retryable,
      };
    }
  }
}
