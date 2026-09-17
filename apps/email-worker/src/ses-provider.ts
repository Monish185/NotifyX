import {
  SESClient,
  SendEmailCommand,
  type SendEmailCommandInput,
  type Body,
} from '@aws-sdk/client-ses';
import {
  type ChannelProvider,
  type ProviderSendRequest,
  type ProviderResult,
} from '@notifyx/kafka';
import { logger } from '@notifyx/logger';
import { type EmailPayload } from './schemas.js';

export interface SesEmailProviderOptions {
  region: string;
  fromAddress: string;
  sesClient?: SESClient; // injected for testing or created once per process
}

/**
 * Classifies AWS SES errors into retryable (transient) vs non-retryable (permanent).
 *
 * NOTE ON ERROR CLASSIFICATION:
 * - We do NOT blindly classify every InvalidParameterException as permanently non-retryable.
 * - Transient: Network/socket timeouts, ThrottlingException, ServiceUnavailableException.
 * - Permanent: MessageRejected (invalid recipient), MailFromDomainNotVerifiedException, AccountSendingPausedException.
 * - Other errors inspect error codes/names safely.
 */
export function classifySesError(err: any): { retryable: boolean; code: string } {
  const errorName = err?.name || err?.Code || 'UnknownError';
  const statusCode = err?.$metadata?.httpStatusCode;

  // Throttling & Rate limiting
  if (
    errorName === 'Throttling' ||
    errorName === 'ThrottlingException' ||
    errorName === 'LimitExceededException' ||
    statusCode === 429
  ) {
    return { retryable: true, code: errorName };
  }

  // Temporary AWS Service Outages
  if (
    errorName === 'ServiceUnavailable' ||
    errorName === 'ServiceUnavailableException' ||
    statusCode === 503 ||
    statusCode === 500
  ) {
    return { retryable: true, code: errorName };
  }

  // Network / Connection timeouts
  if (
    err?.code === 'ECONNRESET' ||
    err?.code === 'ETIMEDOUT' ||
    err?.code === 'ENOTFOUND' ||
    errorName === 'TimeoutError'
  ) {
    return { retryable: true, code: errorName };
  }

  // Explicit Permanent Rejections
  if (
    errorName === 'MessageRejected' ||
    errorName === 'MailFromDomainNotVerifiedException' ||
    errorName === 'AccountSendingPausedException'
  ) {
    return { retryable: false, code: errorName };
  }

  // InvalidParameterException: could be bad address/formatting (permanent) or transient parameter resolution
  if (errorName === 'InvalidParameterException' || errorName === 'InvalidParameterValue') {
    return { retryable: false, code: errorName };
  }

  // Default to non-retryable unless transient indications exist
  return { retryable: false, code: errorName };
}

/**
 * Real Amazon SES Email Provider Adapter.
 *
 * IDEMPOTENCY & EXTERNAL SIDE-EFFECT BOUNDARY:
 * - Uses deliveryId as the logical idempotency key.
 * - Note: AWS SES SendEmail does NOT provide native exactly-once idempotency deduplication.
 *   The distributed boundary relies on NotifyX database-level state machine (delivery.status === 'DELIVERED')
 *   to avoid duplicate dispatch attempts.
 * - Lifecycle: The SESClient is created once upon worker initialization and reused for all requests.
 */
export class SesEmailProvider implements ChannelProvider<EmailPayload> {
  readonly name = 'aws-ses';
  private readonly client: SESClient;
  private readonly fromAddress: string;

  constructor(options: SesEmailProviderOptions) {
    this.fromAddress = options.fromAddress;
    this.client =
      options.sesClient ||
      new SESClient({
        region: options.region,
      });
  }

  async send(request: ProviderSendRequest<EmailPayload>): Promise<ProviderResult> {
    const { idempotencyKey, recipient, payload, tenantId, notificationId, deliveryId } =
      request;

    // Content mapping: prioritize HTML body with text fallback, or text/body if HTML not present
    const textContent = payload.text || payload.body || '';
    const htmlContent = payload.html;

    const bodyObj: Body = {};
    if (htmlContent) {
      bodyObj.Html = {
        Charset: 'UTF-8',
        Data: htmlContent,
      };
    }
    if (textContent || !htmlContent) {
      bodyObj.Text = {
        Charset: 'UTF-8',
        Data: textContent || ' ',
      };
    }

    const commandInput: SendEmailCommandInput = {
      Source: this.fromAddress,
      Destination: {
        ToAddresses: [recipient],
      },
      Message: {
        Subject: {
          Charset: 'UTF-8',
          Data: payload.subject,
        },
        Body: bodyObj,
      },
    };

    try {
      logger.info(
        {
          provider: this.name,
          deliveryId,
          notificationId,
          tenantId,
          idempotencyKey,
          recipient,
        },
        'Dispatching email via AWS SES'
      );

      const command = new SendEmailCommand(commandInput);
      const response = await this.client.send(command);

      const providerMessageId = response.MessageId || `ses_${deliveryId}`;

      logger.info(
        {
          provider: this.name,
          deliveryId,
          providerMessageId,
          status: 'accepted',
        },
        'AWS SES accepted email delivery'
      );

      return {
        success: true,
        providerMessageId,
        metadata: {
          awsMessageId: response.MessageId,
          httpStatusCode: response.$metadata?.httpStatusCode,
        },
      };
    } catch (err: any) {
      const { retryable, code } = classifySesError(err);

      logger.error(
        {
          provider: this.name,
          deliveryId,
          error: err.message,
          errorType: code,
          retryable,
        },
        'AWS SES delivery failed'
      );

      return {
        success: false,
        error: `[SES_${code}] ${err.message || 'AWS SES rejection'}`,
        retryable,
      };
    }
  }

  /**
   * Closes the SES client on worker shutdown.
   */
  destroy(): void {
    this.client.destroy();
  }
}
