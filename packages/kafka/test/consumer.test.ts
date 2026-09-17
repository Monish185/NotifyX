import { describe, it, expect, vi, beforeEach } from 'vitest';
import { KafkaConsumer } from '../src/consumer.js';

vi.mock('kafkajs', () => {
  const commitOffsetsMock = vi.fn().mockResolvedValue(undefined);
  const subscribeMock = vi.fn().mockResolvedValue(undefined);
  const connectMock = vi.fn().mockResolvedValue(undefined);
  const disconnectMock = vi.fn().mockResolvedValue(undefined);
  let capturedEachMessage: any = null;

  const runMock = vi.fn(async (options: any) => {
    capturedEachMessage = options.eachMessage;
  });

  const consumerMock = vi.fn(() => ({
    connect: connectMock,
    disconnect: disconnectMock,
    subscribe: subscribeMock,
    run: runMock,
    commitOffsets: commitOffsetsMock,
  }));

  const KafkaMock = vi.fn(() => ({
    consumer: consumerMock,
  }));

  return {
    Kafka: KafkaMock,
    logLevel: { NOTHING: 0 },
    _commitOffsetsMock: commitOffsetsMock,
    _subscribeMock: subscribeMock,
    _connectMock: connectMock,
    _disconnectMock: disconnectMock,
    _runMock: runMock,
    _getCapturedEachMessage: () => capturedEachMessage,
  };
});

describe('KafkaConsumer', () => {
  let consumer: KafkaConsumer;

  beforeEach(() => {
    vi.clearAllMocks();
    consumer = new KafkaConsumer({
      brokers: ['localhost:9092'],
      groupId: 'test-inapp-workers',
      clientId: 'test-consumer',
    });
  });

  it('1. should initialize with proper configuration and initial state', () => {
    expect(consumer).toBeDefined();
    expect(consumer.isConnected()).toBe(false);
    expect(consumer.isConsuming()).toBe(false);
  });

  it('2. should manage connection lifecycle (connect, subscribe, disconnect)', async () => {
    const { _connectMock, _subscribeMock, _disconnectMock } = (await import(
      'kafkajs'
    )) as any;

    await consumer.connect();
    expect(consumer.isConnected()).toBe(true);
    expect(_connectMock).toHaveBeenCalledTimes(1);

    await consumer.subscribe('notification.delivery.requested');
    expect(_subscribeMock).toHaveBeenCalledWith({
      topic: 'notification.delivery.requested',
      fromBeginning: false,
    });

    await consumer.disconnect();
    expect(consumer.isConnected()).toBe(false);
    expect(_disconnectMock).toHaveBeenCalledTimes(1);
  });

  it('3. should execute handler and commit offset ONLY AFTER handler succeeds', async () => {
    const { _runMock, _commitOffsetsMock, _getCapturedEachMessage } = (await import(
      'kafkajs'
    )) as any;

    await consumer.connect();

    const handlerMock = vi.fn().mockResolvedValue(undefined);
    await consumer.run(handlerMock);

    expect(_runMock).toHaveBeenCalledWith(
      expect.objectContaining({ autoCommit: false })
    );

    const eachMessage = _getCapturedEachMessage();
    expect(eachMessage).toBeDefined();

    const rawMessage = {
      offset: '42',
      key: Buffer.from('del_123'),
      value: Buffer.from(
        JSON.stringify({
          eventId: 'evt_1',
          deliveryId: 'del_123',
          channel: 'IN_APP',
        })
      ),
      headers: {
        'tenant-id': Buffer.from('tenant_abc'),
      },
      timestamp: '1789580000000',
    };

    await eachMessage({
      topic: 'notification.delivery.requested',
      partition: 2,
      message: rawMessage,
      heartbeat: vi.fn(),
    });

    expect(handlerMock).toHaveBeenCalledTimes(1);
    const ctx = handlerMock.mock.calls[0][0];
    expect(ctx.topic).toBe('notification.delivery.requested');
    expect(ctx.partition).toBe(2);
    expect(ctx.offset).toBe('42');
    expect(ctx.key).toBe('del_123');
    expect(ctx.headers['tenant-id']).toBe('tenant_abc');
    expect(ctx.parsedPayload.eventId).toBe('evt_1');
    expect(ctx.parsedPayload.channel).toBe('IN_APP');

    // Offset commit should be next offset: 42 + 1 = 43
    expect(_commitOffsetsMock).toHaveBeenCalledWith([
      {
        topic: 'notification.delivery.requested',
        partition: 2,
        offset: '43',
      },
    ]);
  });

  it('4. should NOT commit offset if handler throws an error (preserving at-least-once)', async () => {
    const { _commitOffsetsMock, _getCapturedEachMessage } = (await import(
      'kafkajs'
    )) as any;

    await consumer.connect();

    const failingHandler = vi
      .fn()
      .mockRejectedValue(new Error('Database lock timeout'));
    await consumer.run(failingHandler);

    const eachMessage = _getCapturedEachMessage();

    const rawMessage = {
      offset: '100',
      key: Buffer.from('del_fail'),
      value: Buffer.from(JSON.stringify({ eventId: 'evt_fail' })),
      headers: {},
      timestamp: '1789580000000',
    };

    await expect(
      eachMessage({
        topic: 'notification.delivery.requested',
        partition: 0,
        message: rawMessage,
        heartbeat: vi.fn(),
      })
    ).rejects.toThrow('Database lock timeout');

    // Offset must NOT have been committed
    expect(_commitOffsetsMock).not.toHaveBeenCalled();
  });
});
