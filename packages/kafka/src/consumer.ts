import {
  Kafka,
  Consumer,
  ConsumerConfig,
  KafkaConfig,
  logLevel,
} from 'kafkajs';
import { logger } from '@notifyx/logger';

export interface KafkaConsumerOptions {
  brokers: string[];
  groupId: string;
  clientId?: string;
  consumerConfig?: Partial<ConsumerConfig>;
}

export interface ConsumerMessageContext<T = unknown> {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  value: Buffer | null;
  headers: Record<string, string | undefined>;
  timestamp: string;
  parsedPayload: T;
  heartbeat: () => Promise<void>;
}

export type MessageHandler<T = unknown> = (
  context: ConsumerMessageContext<T>
) => Promise<void>;

export class KafkaConsumer {
  private kafka: Kafka;
  private consumer: Consumer;
  private connected: boolean = false;
  private isRunning: boolean = false;
  private readonly brokers: string[];
  private readonly groupId: string;
  private readonly clientId: string;

  constructor(options: KafkaConsumerOptions) {
    this.brokers = options.brokers;
    this.groupId = options.groupId;
    this.clientId = options.clientId || `${this.groupId}-client`;

    const kafkaConfig: KafkaConfig = {
      clientId: this.clientId,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
    };

    this.kafka = new Kafka(kafkaConfig);
    this.consumer = this.kafka.consumer({
      groupId: this.groupId,
      allowAutoTopicCreation: true,
      ...options.consumerConfig,
    });
  }

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }

    try {
      logger.info(
        { brokers: this.brokers, groupId: this.groupId, clientId: this.clientId },
        'Connecting Kafka consumer'
      );
      await this.consumer.connect();
      this.connected = true;
      logger.info(
        { groupId: this.groupId },
        'Kafka consumer connected successfully'
      );
    } catch (error) {
      this.connected = false;
      logger.error(
        { error, brokers: this.brokers, groupId: this.groupId },
        'Failed to connect Kafka consumer'
      );
      throw error;
    }
  }

  async subscribe(topic: string | string[], fromBeginning = false): Promise<void> {
    if (!this.connected) {
      throw new Error('KafkaConsumer is not connected. Call connect() before subscribe().');
    }

    const topics = Array.isArray(topic) ? topic : [topic];
    for (const t of topics) {
      logger.info({ topic: t, fromBeginning, groupId: this.groupId }, 'Subscribing to Kafka topic');
      await this.consumer.subscribe({ topic: t, fromBeginning });
    }
  }

  /**
   * Starts consuming messages.
   * Uses explicit manual offset commits (autoCommit: false):
   * Offset is committed ONLY AFTER the handler resolves successfully.
   * If handler throws, offset is NOT committed, preserving at-least-once delivery.
   */
  async run<T = unknown>(handler: MessageHandler<T>): Promise<void> {
    if (!this.connected) {
      throw new Error('KafkaConsumer is not connected. Call connect() before run().');
    }

    this.isRunning = true;

    await this.consumer.run({
      autoCommit: false, // Explicit manual offset commit after successful handling
      eachMessage: async ({ topic, partition, message, heartbeat }) => {
        const offset = message.offset;
        const key = message.key ? message.key.toString() : null;
        const valueStr = message.value ? message.value.toString() : null;

        // Parse headers
        const headers: Record<string, string | undefined> = {};
        if (message.headers) {
          for (const [k, v] of Object.entries(message.headers)) {
            headers[k] = v ? v.toString() : undefined;
          }
        }

        let parsedPayload: T;
        try {
          parsedPayload = valueStr ? JSON.parse(valueStr) : null;
        } catch (parseError) {
          logger.error(
            { parseError, topic, partition, offset, key },
            'Failed to parse JSON payload from Kafka message. Skipping unparseable message.'
          );
          // Commit offset for completely unparseable poison pills to avoid infinite poison loops
          await this.commitOffset(topic, partition, offset);
          return;
        }

        const context: ConsumerMessageContext<T> = {
          topic,
          partition,
          offset,
          key,
          value: message.value,
          headers,
          timestamp: message.timestamp,
          parsedPayload,
          heartbeat,
        };

        try {
          // Execute handler (business logic)
          await handler(context);

          // Commit offset ONLY after handler completes successfully
          await this.commitOffset(topic, partition, offset);
        } catch (handlerError) {
          logger.error(
            {
              error: handlerError,
              topic,
              partition,
              offset,
              key,
              groupId: this.groupId,
            },
            'Consumer handler failed processing message. Offset NOT committed (at-least-once retry).'
          );
          // Rethrow so KafkaJS initiates retry / backoff according to consumer config
          throw handlerError;
        }
      },
    });
  }

  /**
   * Explicitly commits the offset of a processed message (+1).
   */
  async commitOffset(topic: string, partition: number, offset: string): Promise<void> {
    const nextOffset = (BigInt(offset) + 1n).toString();
    await this.consumer.commitOffsets([
      {
        topic,
        partition,
        offset: nextOffset,
      },
    ]);
  }

  async disconnect(): Promise<void> {
    if (!this.connected) {
      return;
    }

    try {
      logger.info({ groupId: this.groupId }, 'Disconnecting Kafka consumer');
      await this.consumer.disconnect();
      this.connected = false;
      this.isRunning = false;
      logger.info({ groupId: this.groupId }, 'Kafka consumer disconnected');
    } catch (error) {
      logger.error({ error, groupId: this.groupId }, 'Error disconnecting Kafka consumer');
      throw error;
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  isConsuming(): boolean {
    return this.isRunning;
  }
}
