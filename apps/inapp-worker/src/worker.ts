import { KafkaConsumer, Semaphore } from '@notifyx/kafka';
import { logger } from '@notifyx/logger';
import { TOPICS, type NotificationDeliveryRequestedEvent } from '@notifyx/shared';
import { workerInFlightGauge, workerConcurrencyLimitGauge } from '@notifyx/metrics';
import { handleInAppDelivery } from './handler.js';

export interface InAppWorkerOptions {
  brokers: string[];
  groupId?: string;
  clientId?: string;
  concurrency?: number;
}

export class InAppWorker {
  private consumer: KafkaConsumer;
  private running: boolean = false;
  private readonly groupId: string;
  private semaphore: Semaphore;

  constructor(options: InAppWorkerOptions) {
    this.groupId = options.groupId || 'notifyx-inapp-workers';
    const concurrency = options.concurrency ?? 10;
    this.semaphore = new Semaphore(concurrency);

    this.consumer = new KafkaConsumer({
      brokers: options.brokers,
      groupId: this.groupId,
      clientId: options.clientId || `${this.groupId}-instance`,
    });
  }

  get concurrencyLimit(): number {
    return this.semaphore.maxConcurrency;
  }

  get inFlightCount(): number {
    return this.semaphore.activeCount;
  }

  async start(): Promise<void> {
    if (this.running) {
      return;
    }

    logger.info(
      { groupId: this.groupId, topic: TOPICS.NOTIFICATION_DELIVERY_REQUESTED },
      'Starting In-App Notification Worker'
    );

    workerConcurrencyLimitGauge.set(
      { service: 'inapp-worker', channel: 'IN_APP' },
      this.semaphore.maxConcurrency
    );

    await this.consumer.connect();
    await this.consumer.subscribe(
      [TOPICS.NOTIFICATION_DELIVERY_REQUESTED, TOPICS.NOTIFICATION_DELIVERY_RETRY],
      false
    );

    this.running = true;

    // Run message consumption loop with explicit offset management and bounded concurrency
    await this.consumer.run<NotificationDeliveryRequestedEvent>(
      async (context) => {
        const release = await this.semaphore.acquire();
        workerInFlightGauge.inc({ service: 'inapp-worker', channel: 'IN_APP' });
        try {
          await handleInAppDelivery(context);
        } finally {
          workerInFlightGauge.dec({ service: 'inapp-worker', channel: 'IN_APP' });
          release();
        }
      }
    );

    logger.info({ groupId: this.groupId }, 'In-App Worker consumption loop active');
  }

  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    logger.info('Stopping In-App Notification Worker');
    this.running = false;
    await this.consumer.disconnect();
    logger.info('In-App Notification Worker stopped cleanly');
  }

  isHealthy(): boolean {
    return this.running && this.consumer.isConnected();
  }

  isReady(): boolean {
    return this.running && this.consumer.isConnected();
  }
}
