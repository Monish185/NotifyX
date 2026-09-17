import { type EnvConfig } from '@notifyx/config';
import {
  type ChannelProvider,
  MockEmailProvider,
} from '@notifyx/kafka';
import { type EmailPayload } from './schemas.js';
import { SesEmailProvider } from './ses-provider.js';
import { logger } from '@notifyx/logger';

/**
 * Provider factory for Email Worker.
 *
 * Behavior:
 * - EMAIL_PROVIDER=mock -> Returns MockEmailProvider (default, zero credentials needed)
 * - EMAIL_PROVIDER=ses  -> Returns SesEmailProvider configured with AWS_REGION and EMAIL_FROM_ADDRESS
 * - Other values        -> Fails fast with an explicit configuration error
 */
export function createEmailProvider(config: EnvConfig): ChannelProvider<EmailPayload> {
  const providerType = config.EMAIL_PROVIDER;

  if (providerType === 'mock') {
    logger.info('Email Worker configured with MockEmailProvider (development mode)');
    const mock = new MockEmailProvider();
    if (config.MOCK_EMAIL_RATE_LIMIT) mock.setRateLimitSimulation(true);
    if (config.MOCK_EMAIL_LATENCY_MS > 0) mock.setLatencyMs(config.MOCK_EMAIL_LATENCY_MS);
    if (config.MOCK_EMAIL_FAILURE_RATE > 0) mock.setFailureRate(config.MOCK_EMAIL_FAILURE_RATE);
    return mock;
  }

  if (providerType === 'ses') {
    const region = config.AWS_REGION;
    const fromAddress = config.EMAIL_FROM_ADDRESS;

    if (!region || !fromAddress) {
      throw new Error(
        `Invalid SES configuration: AWS_REGION and EMAIL_FROM_ADDRESS are required when EMAIL_PROVIDER=ses. Provided region: "${region}", fromAddress: "${fromAddress}"`
      );
    }

    logger.info(
      { region, fromAddress },
      'Email Worker configured with SesEmailProvider (Amazon SES)'
    );

    return new SesEmailProvider({
      region,
      fromAddress,
    });
  }

  throw new Error(
    `Unsupported EMAIL_PROVIDER: "${providerType}". Supported options are "mock" or "ses".`
  );
}
