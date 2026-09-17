import { type EnvConfig } from '@notifyx/config';
import {
  type ChannelProvider,
  MockSmsProvider,
} from '@notifyx/kafka';
import { type SmsPayload } from './schemas.js';
import { TwilioSmsProvider } from './twilio-provider.js';
import { logger } from '@notifyx/logger';

/**
 * Provider factory for SMS Worker.
 *
 * Behavior:
 * - SMS_PROVIDER=mock   -> Returns MockSmsProvider (default, zero credentials needed)
 * - SMS_PROVIDER=twilio -> Returns TwilioSmsProvider configured with TWILIO credentials
 * - Other values        -> Fails fast with an explicit configuration error
 */
export function createSmsProvider(config: EnvConfig): ChannelProvider<SmsPayload> {
  const providerType = config.SMS_PROVIDER;

  if (providerType === 'mock') {
    logger.info('SMS Worker configured with MockSmsProvider (development mode)');
    const mock = new MockSmsProvider();
    if (config.MOCK_SMS_RATE_LIMIT) mock.setRateLimitSimulation(true);
    if (config.MOCK_SMS_LATENCY_MS > 0) mock.setLatencyMs(config.MOCK_SMS_LATENCY_MS);
    if (config.MOCK_SMS_FAILURE_RATE > 0) mock.setFailureRate(config.MOCK_SMS_FAILURE_RATE);
    return mock;
  }

  if (providerType === 'twilio') {
    const accountSid = config.TWILIO_ACCOUNT_SID;
    const authToken = config.TWILIO_AUTH_TOKEN;
    const fromNumber = config.TWILIO_FROM_NUMBER;

    if (!accountSid || !authToken || !fromNumber) {
      throw new Error(
        'Invalid Twilio configuration: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, and TWILIO_FROM_NUMBER are required when SMS_PROVIDER=twilio.'
      );
    }

    logger.info(
      { accountSidPrefix: accountSid.slice(0, 4) + '...', fromNumber },
      'SMS Worker configured with TwilioSmsProvider (Twilio)'
    );

    return new TwilioSmsProvider({
      accountSid,
      authToken,
      fromNumber,
    });
  }

  throw new Error(
    `Unsupported SMS_PROVIDER: "${providerType}". Supported options are "mock" or "twilio".`
  );
}
