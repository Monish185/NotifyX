import { type EnvConfig } from '@notifyx/config';
import {
  type ChannelProvider,
  MockPushProvider,
} from '@notifyx/kafka';
import { type PushPayload } from './schemas.js';
import { FcmPushProvider } from './fcm-provider.js';
import { logger } from '@notifyx/logger';

/**
 * Provider factory for Push Worker.
 *
 * Behavior:
 * - PUSH_PROVIDER=mock -> Returns MockPushProvider (default, zero credentials needed)
 * - PUSH_PROVIDER=fcm  -> Returns FcmPushProvider configured with FIREBASE credentials
 * - Other values       -> Fails fast with an explicit configuration error
 */
export function createPushProvider(config: EnvConfig): ChannelProvider<PushPayload> {
  const providerType = config.PUSH_PROVIDER;

  if (providerType === 'mock') {
    logger.info('Push Worker configured with MockPushProvider (development mode)');
    const mock = new MockPushProvider();
    if (config.MOCK_PUSH_RATE_LIMIT) mock.setRateLimitSimulation(true);
    if (config.MOCK_PUSH_LATENCY_MS > 0) mock.setLatencyMs(config.MOCK_PUSH_LATENCY_MS);
    if (config.MOCK_PUSH_FAILURE_RATE > 0) mock.setFailureRate(config.MOCK_PUSH_FAILURE_RATE);
    return mock;
  }

  if (providerType === 'fcm') {
    const projectId = config.FIREBASE_PROJECT_ID;
    const clientEmail = config.FIREBASE_CLIENT_EMAIL;
    const privateKey = config.FIREBASE_PRIVATE_KEY;

    if (!projectId || !clientEmail || !privateKey) {
      throw new Error(
        'Invalid FCM configuration: FIREBASE_PROJECT_ID, FIREBASE_CLIENT_EMAIL, and FIREBASE_PRIVATE_KEY are required when PUSH_PROVIDER=fcm.'
      );
    }

    logger.info(
      { projectId, clientEmail },
      'Push Worker configured with FcmPushProvider (Firebase Cloud Messaging)'
    );

    return new FcmPushProvider({
      projectId,
      clientEmail,
      privateKey,
    });
  }

  throw new Error(
    `Unsupported PUSH_PROVIDER: "${providerType}". Supported options are "mock" or "fcm".`
  );
}
