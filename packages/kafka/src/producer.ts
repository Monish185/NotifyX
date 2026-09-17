import { Kafka, Producer, ProducerConfig, KafkaConfig, logLevel } from 'kafkajs';
import { logger } from '@notifyx/logger';
import {
  TOPICS,
  type NotificationDeliveryRequestedEvent,
  type NotificationDeliveryRetryEvent,
  type NotificationDeliveryDlqEvent,
} from '@notifyx/shared';

export type OutboxPublishableEvent =
  | NotificationDeliveryRequestedEvent
  | NotificationDeliveryRetryEvent
  | NotificationDeliveryDlqEvent
  | Record<string, any>;

export interface KafkaProducerOptions {
  brokers: string[];
  clientId?: string;
  producerConfig?: ProducerConfig;
}

export class KafkaProducer {
  private kafka: Kafka;
  private producer: Producer;
  private connected: boolean = false;
  private readonly brokers: string[];
  private readonly clientId: string;

  constructor(options: KafkaProducerOptions) {
    this.brokers = options.brokers;
    this.clientId = options.clientId || 'notifyx-producer';

    const kafkaConfig: KafkaConfig = {
      clientId: this.clientId,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING, // We use @notifyx/logger for application logging
    };

    this.kafka = new Kafka(kafkaConfig);
    this.producer = this.kafka.producer({
      allowAutoTopicCreation: true,
      ...options.producerConfig,
    });
  }

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }

    try {
      logger.info(
        { brokers: this.brokers, clientId: this.clientId },
        'Connecting Kafka producer'
      );
      await this.producer.connect();
      this.connected = true;
      logger.info('Kafka producer connected successfully');
    } catch (error) {
      this.connected = false;
      logger.error({ error, brokers: this.brokers }, 'Failed to connect Kafka producer');
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    if (!this.connected) {
      return;
    }

    try {
      logger.info('Disconnecting Kafka producer');
      await this.producer.disconnect();
      this.connected = false;
      logger.info('Kafka producer disconnected');
    } catch (error) {
      logger.error({ error }, 'Error disconnecting Kafka producer');
      throw error;
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  /**
   * Publishes a single delivery requested event to Kafka.
   * Serializes deterministically to JSON.
   * Uses deliveryId as the partition key for stable partitioning.
   */
  async publish(
    topic: string,
    event: OutboxPublishableEvent,
    partitionKey?: string
  ): Promise<void> {
    if (!this.connected) {
      throw new Error('KafkaProducer is not connected. Call connect() before publishing.');
    }

    const key = partitionKey || event.deliveryId || event.notificationId;
    const value = JSON.stringify(event);

    try {
      logger.info(
        {
          topic,
          eventId: event.eventId,
          deliveryId: event.deliveryId,
          notificationId: event.notificationId,
          tenantId: event.tenantId,
          channel: event.channel,
        },
        'Publishing event to Kafka'
      );

      await this.producer.send({
        topic,
        messages: [
          {
            key,
            value,
            headers: {
              'event-type': event.eventType,
              'event-id': event.eventId,
              'tenant-id': event.tenantId,
              'occurred-at': event.occurredAt,
              ...(event.correlationId ? { 'correlation-id': event.correlationId } : {}),
            },
          },
        ],
      });

      logger.info(
        {
          topic,
          eventId: event.eventId,
          deliveryId: event.deliveryId,
        },
        'Event published successfully to Kafka'
      );
    } catch (error) {
      logger.error(
        {
          error,
          topic,
          eventId: event.eventId,
          deliveryId: event.deliveryId,
        },
        'Failed to publish event to Kafka'
      );
      throw error;
    }
  }

  /**
   * Publishes a batch of events to a topic.
   */
  async publishBatch(
    topic: string,
    events: OutboxPublishableEvent[]
  ): Promise<void> {
    if (!this.connected) {
      throw new Error('KafkaProducer is not connected. Call connect() before publishing.');
    }

    if (events.length === 0) {
      return;
    }

    const messages = events.map((event) => ({
      key: event.deliveryId || event.notificationId,
      value: JSON.stringify(event),
      headers: {
        'event-type': event.eventType,
        'event-id': event.eventId,
        'tenant-id': event.tenantId,
        'occurred-at': event.occurredAt,
        ...(event.correlationId ? { 'correlation-id': event.correlationId } : {}),
      },
    }));

    try {
      logger.info(
        { topic, count: events.length },
        'Publishing batch of events to Kafka'
      );

      await this.producer.send({
        topic,
        messages,
      });

      logger.info(
        { topic, count: events.length },
        'Batch published successfully to Kafka'
      );
    } catch (error) {
      logger.error(
        { error, topic, count: events.length },
        'Failed to publish batch of events to Kafka'
      );
      throw error;
    }
  }

  /**
   * Helper to ensure topic exists (uses admin client).
   */
  async ensureTopicExists(topic: string, numPartitions = 3, replicationFactor = 1): Promise<void> {
    const admin = this.kafka.admin();
    try {
      await admin.connect();
      const topics = await admin.listTopics();
      if (!topics.includes(topic)) {
        logger.info({ topic }, 'Topic does not exist, creating topic');
        await admin.createTopics({
          topics: [
            {
              topic,
              numPartitions,
              replicationFactor,
            },
          ],
        });
        logger.info({ topic }, 'Topic created successfully');
      }
    } catch (error) {
      logger.warn({ error, topic }, 'Could not verify or create topic via Kafka Admin');
    } finally {
      await admin.disconnect().catch(() => {});
    }
  }
}
