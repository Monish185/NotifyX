import { describe, it, expect, vi, beforeEach } from 'vitest';
import { KafkaProducer } from '../src/producer.js';
import { TOPICS, EVENT_TYPES, type NotificationDeliveryRequestedEvent } from '@notifyx/shared';

// Mock kafkajs
vi.mock('kafkajs', () => {
  const sendMock = vi.fn().mockResolvedValue([{ topicName: 'notification.delivery.requested', partition: 0, errorCode: 0 }]);
  const connectMock = vi.fn().mockResolvedValue(undefined);
  const disconnectMock = vi.fn().mockResolvedValue(undefined);

  const producerMock = vi.fn(() => ({
    connect: connectMock,
    disconnect: disconnectMock,
    send: sendMock,
  }));

  const adminMock = vi.fn(() => ({
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    listTopics: vi.fn().mockResolvedValue(['some-topic']),
    createTopics: vi.fn().mockResolvedValue(true),
  }));

  const KafkaMock = vi.fn(() => ({
    producer: producerMock,
    admin: adminMock,
  }));

  return {
    Kafka: KafkaMock,
    logLevel: { NOTHING: 0 },
    _sendMock: sendMock,
    _connectMock: connectMock,
    _disconnectMock: disconnectMock,
  };
});

describe('KafkaProducer', () => {
  let producer: KafkaProducer;

  beforeEach(() => {
    vi.clearAllMocks();
    producer = new KafkaProducer({
      brokers: ['localhost:9092'],
      clientId: 'test-producer',
    });
  });

  it('1. should initialize with proper configuration', () => {
    expect(producer).toBeDefined();
    expect(producer.isConnected()).toBe(false);
  });

  it('2. should manage connection lifecycle (connect & disconnect)', async () => {
    await producer.connect();
    expect(producer.isConnected()).toBe(true);

    await producer.disconnect();
    expect(producer.isConnected()).toBe(false);
  });

  it('3. should reject publish if not connected', async () => {
    const mockEvent: NotificationDeliveryRequestedEvent = {
      eventId: 'evt_123',
      eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
      version: 1,
      occurredAt: new Date().toISOString(),
      tenantId: 'tenant_1',
      notificationId: 'ntf_1',
      deliveryId: 'del_1',
      userId: 'usr_1',
      channel: 'EMAIL',
      priority: 'NORMAL',
      payload: { message: 'hi' },
    };

    await expect(
      producer.publish(TOPICS.NOTIFICATION_DELIVERY_REQUESTED, mockEvent)
    ).rejects.toThrow('KafkaProducer is not connected');
  });

  it('4. should serialize event and publish successfully with deliveryId as partition key', async () => {
    const { _sendMock } = await import('kafkajs') as any;

    await producer.connect();

    const mockEvent: NotificationDeliveryRequestedEvent = {
      eventId: 'evt_456',
      eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
      version: 1,
      occurredAt: '2026-09-16T12:00:00.000Z',
      tenantId: 'tenant_abc',
      notificationId: 'ntf_xyz',
      deliveryId: 'del_789',
      userId: 'usr_999',
      channel: 'PUSH',
      priority: 'HIGH',
      payload: { orderId: 'ORD-123' },
    };

    await producer.publish(TOPICS.NOTIFICATION_DELIVERY_REQUESTED, mockEvent);

    expect(_sendMock).toHaveBeenCalledTimes(1);
    const callArg = _sendMock.mock.calls[0][0];
    expect(callArg.topic).toBe('notification.delivery.requested');
    expect(callArg.messages).toHaveLength(1);
    expect(callArg.messages[0].key).toBe('del_789');

    const parsedValue = JSON.parse(callArg.messages[0].value);
    expect(parsedValue.eventId).toBe('evt_456');
    expect(parsedValue.deliveryId).toBe('del_789');
    expect(parsedValue.notificationId).toBe('ntf_xyz');
    expect(parsedValue.tenantId).toBe('tenant_abc');
    expect(parsedValue.channel).toBe('PUSH');
    expect(parsedValue.payload.orderId).toBe('ORD-123');
  });

  it('5. should handle publish failures gracefully and propagate error', async () => {
    const { _sendMock } = await import('kafkajs') as any;
    _sendMock.mockRejectedValueOnce(new Error('Broker unreachable'));

    await producer.connect();

    const mockEvent: NotificationDeliveryRequestedEvent = {
      eventId: 'evt_fail',
      eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
      version: 1,
      occurredAt: new Date().toISOString(),
      tenantId: 'tenant_1',
      notificationId: 'ntf_1',
      deliveryId: 'del_1',
      userId: 'usr_1',
      channel: 'EMAIL',
      priority: 'NORMAL',
      payload: {},
    };

    await expect(
      producer.publish(TOPICS.NOTIFICATION_DELIVERY_REQUESTED, mockEvent)
    ).rejects.toThrow('Broker unreachable');
  });

  it('6. should publish batch of events successfully', async () => {
    const { _sendMock } = await import('kafkajs') as any;

    await producer.connect();

    const events: NotificationDeliveryRequestedEvent[] = [
      {
        eventId: 'evt_1',
        eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
        version: 1,
        occurredAt: new Date().toISOString(),
        tenantId: 'tenant_1',
        notificationId: 'ntf_1',
        deliveryId: 'del_1',
        userId: 'usr_1',
        channel: 'EMAIL',
        priority: 'NORMAL',
        payload: {},
      },
      {
        eventId: 'evt_2',
        eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
        version: 1,
        occurredAt: new Date().toISOString(),
        tenantId: 'tenant_1',
        notificationId: 'ntf_1',
        deliveryId: 'del_2',
        userId: 'usr_1',
        channel: 'SMS',
        priority: 'NORMAL',
        payload: {},
      },
    ];

    await producer.publishBatch(TOPICS.NOTIFICATION_DELIVERY_REQUESTED, events);
    expect(_sendMock).toHaveBeenCalledTimes(1);
    expect(_sendMock.mock.calls[0][0].messages).toHaveLength(2);
  });
});
