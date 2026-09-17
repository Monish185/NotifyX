import { describe, it, expect, vi, beforeEach } from 'vitest';
import { BaseChannelWorker, type ChannelProvider } from '../src/channel-worker.js';
import { Channel, EVENT_TYPES, TOPICS, type NotificationDeliveryRequestedEvent, type NotificationDeliveryRetryEvent } from '@notifyx/shared';
import { prisma, Prisma } from '@notifyx/database';

// Mock kafkajs
vi.mock('kafkajs', () => {
  const consumerMock = vi.fn(() => ({
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    subscribe: vi.fn().mockResolvedValue(undefined),
    run: vi.fn().mockResolvedValue(undefined),
    commitOffsets: vi.fn().mockResolvedValue(undefined),
  }));

  return {
    Kafka: vi.fn(() => ({
      consumer: consumerMock,
    })),
    logLevel: { NOTHING: 0 },
  };
});

// Mock @notifyx/database
vi.mock('@notifyx/database', () => {
  const mockPrisma: any = {
    notificationDelivery: {
      findUnique: vi.fn(),
      update: vi.fn(),
      count: vi.fn().mockResolvedValue(0),
    },
    notification: {
      update: vi.fn(),
    },
    retryRecord: {
      findUnique: vi.fn(),
      create: vi.fn(),
    },
    deadLetterEvent: {
      findUnique: vi.fn(),
      create: vi.fn(),
    },
    outboxEvent: {
      create: vi.fn(),
    },
    $transaction: vi.fn(async (cb: any) => cb(mockPrisma)),
  };

  class MockKnownRequestError extends Error {
    code: string;
    constructor(message: string, code: string) {
      super(message);
      this.code = code;
      this.name = 'PrismaClientKnownRequestError';
    }
  }

  return {
    prisma: mockPrisma,
    Prisma: {
      PrismaClientKnownRequestError: MockKnownRequestError,
    },
  };
});

describe('BaseChannelWorker - Phase 6 Retry & DLQ Engine', () => {
  let mockProvider: ChannelProvider;
  let worker: BaseChannelWorker;

  const sampleDelivery = {
    id: 'del_123',
    notificationId: 'notif_abc',
    channel: Channel.EMAIL,
    status: 'PENDING',
    attemptCount: 0,
    notification: {
      id: 'notif_abc',
      tenantId: 'tenant_xyz',
      userId: 'user_456',
      user: {
        id: 'user_456',
        email: 'user@example.com',
      },
    },
  };

  const sampleEvent: NotificationDeliveryRequestedEvent = {
    eventId: 'evt_001',
    eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_REQUESTED,
    version: 1,
    occurredAt: new Date().toISOString(),
    tenantId: 'tenant_xyz',
    notificationId: 'notif_abc',
    deliveryId: 'del_123',
    userId: 'user_456',
    channel: Channel.EMAIL,
    priority: 'HIGH',
    payload: { subject: 'Hello', body: 'World' },
    attempt: 1,
  };

  const createMessageContext = (event: any) => ({
    topic: TOPICS.NOTIFICATION_DELIVERY_REQUESTED,
    partition: 0,
    offset: '10',
    key: event.deliveryId,
    value: Buffer.from(JSON.stringify(event)),
    headers: {},
    timestamp: Date.now().toString(),
    parsedPayload: event,
    heartbeat: vi.fn().mockResolvedValue(undefined),
  });

  beforeEach(() => {
    vi.clearAllMocks();

    mockProvider = {
      name: 'MockEmailProvider',
      send: vi.fn().mockResolvedValue({
        success: true,
        providerMessageId: 'msg_success_123',
      }),
    };

    worker = new BaseChannelWorker({
      channel: Channel.EMAIL,
      serviceName: 'email-worker-test',
      consumerGroup: 'notifyx-email-workers',
      brokers: ['localhost:9092'],
      healthPort: 3999,
      provider: mockProvider,
      retry: {
        maxAttempts: 5,
        baseDelayMs: 1000,
        maxDelayMs: 60000,
        jitterRatio: 0, // Deterministic backoff for testing
      },
      resolveRecipient: () => ({ recipient: 'user@example.com' }),
      validatePayload: (p) => ({ valid: true, data: p }),
    });
  });

  // ============================================================================
  // 1. ATTEMPT INVARIANTS & NORMAL PROGRESSION
  // ============================================================================

  it('1. Initial attempt 1 succeeds -> status: DELIVERED, attemptCount: 1', async () => {
    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      ...sampleDelivery,
      status: 'PENDING',
      attemptCount: 0,
    });

    const result = await worker.processEvent(createMessageContext(sampleEvent));

    expect(result.status).toBe('PROCESSED');
    expect(mockProvider.send).toHaveBeenCalledWith(
      expect.objectContaining({
        idempotencyKey: 'del_123',
        recipient: 'user@example.com',
      })
    );

    // Verify delivery updated to DELIVERED with attemptCount: 1
    expect(prisma.notificationDelivery.update).toHaveBeenCalledWith({
      where: { id: 'del_123' },
      data: expect.objectContaining({
        status: 'DELIVERED',
        attemptCount: 1,
        error: null,
      }),
    });
  });

  it('2. Initial attempt 1 fails (transient) -> status: RETRY_SCHEDULED, attemptCount: 1, retry event attempt: 2', async () => {
    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      ...sampleDelivery,
      status: 'PENDING',
      attemptCount: 0,
    });
    (prisma.retryRecord.findUnique as any).mockResolvedValue(null);

    (mockProvider.send as any).mockResolvedValue({
      success: false,
      error: 'Network timeout connecting to provider',
      retryable: true,
      metadata: { errorCode: 'TIMEOUT' },
    });

    const result = await worker.processEvent(createMessageContext(sampleEvent));

    expect(result.status).toBe('RETRY_SCHEDULED');

    // Invariant: delivery.attemptCount = 1 (number of attempts already executed)
    expect(prisma.notificationDelivery.update).toHaveBeenCalledWith({
      where: { id: 'del_123' },
      data: expect.objectContaining({
        status: 'RETRY_SCHEDULED',
        attemptCount: 1,
        error: 'Network timeout connecting to provider',
        lastErrorCode: 'TIMEOUT',
        nextAttemptAt: expect.any(Date),
      }),
    });

    // Invariant: RetryRecord created with attempt = 1
    expect(prisma.retryRecord.create).toHaveBeenCalledWith({
      data: expect.objectContaining({
        deliveryId: 'del_123',
        attempt: 1,
        error: 'Network timeout connecting to provider',
      }),
    });

    // Invariant: OutboxEvent created with eventType = 'notification.delivery.retry', payload.attempt = 2
    expect(prisma.outboxEvent.create).toHaveBeenCalledWith({
      data: expect.objectContaining({
        aggregateType: 'notification_delivery',
        aggregateId: 'del_123',
        deliveryId: 'del_123',
        eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_RETRY,
        status: 'PENDING',
        payload: expect.objectContaining({
          attempt: 2,
          maxAttempts: 5,
        }),
      }),
    });
  });

  it('3. Retry attempt 2 fails (transient) -> status: RETRY_SCHEDULED, attemptCount: 2, retry event attempt: 3', async () => {
    // Delivery already executed 1 attempt
    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      ...sampleDelivery,
      status: 'RETRY_SCHEDULED',
      attemptCount: 1,
    });
    (prisma.retryRecord.findUnique as any).mockResolvedValue(null);

    (mockProvider.send as any).mockResolvedValue({
      success: false,
      error: '503 Service Unavailable',
      retryable: true,
      metadata: { errorCode: 'SERVICE_UNAVAILABLE' },
    });

    const retryEvent: NotificationDeliveryRetryEvent = {
      ...sampleEvent,
      eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_RETRY,
      attempt: 2,
      maxAttempts: 5,
      scheduledAt: new Date().toISOString(),
      nextAttemptAt: new Date().toISOString(),
    };

    const result = await worker.processEvent(createMessageContext(retryEvent));

    expect(result.status).toBe('RETRY_SCHEDULED');

    // Invariant: delivery.attemptCount = 2
    expect(prisma.notificationDelivery.update).toHaveBeenCalledWith({
      where: { id: 'del_123' },
      data: expect.objectContaining({
        status: 'RETRY_SCHEDULED',
        attemptCount: 2,
        error: '503 Service Unavailable',
      }),
    });

    // Invariant: RetryRecord created with attempt = 2
    expect(prisma.retryRecord.create).toHaveBeenCalledWith({
      data: expect.objectContaining({
        deliveryId: 'del_123',
        attempt: 2,
      }),
    });

    // Invariant: OutboxEvent created with payload.attempt = 3
    expect(prisma.outboxEvent.create).toHaveBeenCalledWith({
      data: expect.objectContaining({
        eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_RETRY,
        payload: expect.objectContaining({
          attempt: 3,
        }),
      }),
    });
  });

  it('4. Attempt 5 fails (maxAttempts = 5) -> terminal exhaustion: status: FAILED, DeadLetterEvent, DLQ OutboxEvent', async () => {
    // Delivery already executed 4 attempts
    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      ...sampleDelivery,
      status: 'RETRY_SCHEDULED',
      attemptCount: 4,
    });
    (prisma.deadLetterEvent.findUnique as any).mockResolvedValue(null);

    (mockProvider.send as any).mockResolvedValue({
      success: false,
      error: 'Connection reset by peer',
      retryable: true,
      metadata: { errorCode: 'CONN_RESET' },
    });

    const finalAttemptEvent = {
      ...sampleEvent,
      eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_RETRY,
      attempt: 5,
      maxAttempts: 5,
    };

    const result = await worker.processEvent(createMessageContext(finalAttemptEvent));

    expect(result.status).toBe('FAILED');

    // Invariant: delivery.status = FAILED, attemptCount = 5
    expect(prisma.notificationDelivery.update).toHaveBeenCalledWith({
      where: { id: 'del_123' },
      data: expect.objectContaining({
        status: 'FAILED',
        attemptCount: 5,
        nextAttemptAt: null,
      }),
    });

    // Invariant: DeadLetterEvent created
    expect(prisma.deadLetterEvent.create).toHaveBeenCalledWith({
      data: expect.objectContaining({
        deliveryId: 'del_123',
        attemptCount: 5,
        channel: Channel.EMAIL,
      }),
    });

    // Invariant: OutboxEvent with DLQ event created
    expect(prisma.outboxEvent.create).toHaveBeenCalledWith({
      data: expect.objectContaining({
        aggregateType: 'notification_delivery',
        deliveryId: 'del_123',
        eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_DLQ,
        status: 'PENDING',
        payload: expect.objectContaining({
          eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_DLQ,
          attemptCount: 5,
        }),
      }),
    });
  });

  // ============================================================================
  // 2. PERMANENT NON-RETRYABLE FAILURE
  // ============================================================================

  it('5. Provider returns retryable: false -> immediately routes to DLQ without scheduling retry', async () => {
    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      ...sampleDelivery,
      status: 'PENDING',
      attemptCount: 0,
    });
    (prisma.deadLetterEvent.findUnique as any).mockResolvedValue(null);

    (mockProvider.send as any).mockResolvedValue({
      success: false,
      error: 'Invalid recipient address: mailbox does not exist',
      retryable: false,
      metadata: { errorCode: 'MAILBOX_NOT_FOUND' },
    });

    const result = await worker.processEvent(createMessageContext(sampleEvent));

    expect(result.status).toBe('FAILED');

    // Updated to FAILED immediately
    expect(prisma.notificationDelivery.update).toHaveBeenCalledWith({
      where: { id: 'del_123' },
      data: expect.objectContaining({
        status: 'FAILED',
        attemptCount: 1,
        nextAttemptAt: null,
      }),
    });

    // DeadLetterEvent created
    expect(prisma.deadLetterEvent.create).toHaveBeenCalledWith({
      data: expect.objectContaining({
        deliveryId: 'del_123',
        attemptCount: 1,
        reason: 'Invalid recipient address: mailbox does not exist',
      }),
    });

    // DLQ OutboxEvent created
    expect(prisma.outboxEvent.create).toHaveBeenCalledWith({
      data: expect.objectContaining({
        eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_DLQ,
      }),
    });

    // No retry OutboxEvent created
    expect(prisma.outboxEvent.create).not.toHaveBeenCalledWith({
      data: expect.objectContaining({
        eventType: EVENT_TYPES.NOTIFICATION_DELIVERY_RETRY,
      }),
    });
  });

  // ============================================================================
  // 3. IDEMPOTENCY & CRASH WINDOWS
  // ============================================================================

  it('6. Crash A: DB retry committed, worker crashed before Kafka ACK -> replay skips provider execution', async () => {
    // Delivery already executed attempt 1 and is in RETRY_SCHEDULED with attemptCount: 1
    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      ...sampleDelivery,
      status: 'RETRY_SCHEDULED',
      attemptCount: 1,
    });

    // Same attempt 1 message re-delivered by Kafka
    const result = await worker.processEvent(createMessageContext(sampleEvent));

    // Must skip execution because attempt 1 is already >= incomingAttempt (1)
    expect(result.status).toBe('SKIPPED');
    expect(mockProvider.send).not.toHaveBeenCalled();
    expect(prisma.notificationDelivery.update).not.toHaveBeenCalled();
    expect(prisma.outboxEvent.create).not.toHaveBeenCalled();
  });

  it('7. Replay of DELIVERED notification -> returns ALREADY_DELIVERED immediately', async () => {
    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      ...sampleDelivery,
      status: 'DELIVERED',
      attemptCount: 1,
    });

    const result = await worker.processEvent(createMessageContext(sampleEvent));

    expect(result.status).toBe('ALREADY_DELIVERED');
    expect(mockProvider.send).not.toHaveBeenCalled();
    expect(prisma.notificationDelivery.update).not.toHaveBeenCalled();
  });

  it('8. Replay of terminal FAILED notification -> returns ALREADY_DELIVERED immediately', async () => {
    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      ...sampleDelivery,
      status: 'FAILED',
      attemptCount: 5,
    });

    const result = await worker.processEvent(createMessageContext(sampleEvent));

    expect(result.status).toBe('ALREADY_DELIVERED');
    expect(mockProvider.send).not.toHaveBeenCalled();
    expect(prisma.notificationDelivery.update).not.toHaveBeenCalled();
  });

  it('9. Crash D: Duplicate DLQ event -> DeadLetterEvent and DLQ OutboxEvent are deduplicated', async () => {
    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      ...sampleDelivery,
      status: 'PENDING',
      attemptCount: 0,
    });
    // Simulate DeadLetterEvent already existing in DB
    (prisma.deadLetterEvent.findUnique as any).mockResolvedValue({
      id: 'existing_dlq_id',
      deliveryId: 'del_123',
    });

    (mockProvider.send as any).mockResolvedValue({
      success: false,
      error: 'Permanent failure',
      retryable: false,
    });

    const result = await worker.processEvent(createMessageContext(sampleEvent));

    expect(result.status).toBe('FAILED');
    // DeadLetterEvent and OutboxEvent must NOT be created again
    expect(prisma.deadLetterEvent.create).not.toHaveBeenCalled();
    expect(prisma.outboxEvent.create).not.toHaveBeenCalled();
  });

  it('10. Crash E: Unique constraint collision (P2002) on RetryRecord handled gracefully', async () => {
    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      ...sampleDelivery,
      status: 'PENDING',
      attemptCount: 0,
    });
    (prisma.retryRecord.findUnique as any).mockResolvedValue(null);
    (prisma.retryRecord.create as any).mockRejectedValue(
      new (Prisma as any).PrismaClientKnownRequestError('Unique constraint failed', 'P2002')
    );

    (mockProvider.send as any).mockResolvedValue({
      success: false,
      error: 'Transient error',
      retryable: true,
    });

    // Should not throw unhandled exception on P2002 collision
    const result = await worker.processEvent(createMessageContext(sampleEvent));
    expect(result.status).toBe('RETRY_SCHEDULED');
  });

  it('11. Security Check: Cross-tenant mismatch rejected with INVALID status', async () => {
    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      ...sampleDelivery,
      notification: {
        ...sampleDelivery.notification,
        tenantId: 'different_tenant',
      },
    });

    const result = await worker.processEvent(createMessageContext(sampleEvent));

    expect(result.status).toBe('INVALID');
    expect(result.reason).toBe('Tenant mismatch');
    expect(mockProvider.send).not.toHaveBeenCalled();
  });

  it('12. Crash F: Provider throws unhandled error -> worker re-throws so Kafka does NOT commit offset', async () => {
    (prisma.notificationDelivery.findUnique as any).mockResolvedValue({
      ...sampleDelivery,
      status: 'PENDING',
      attemptCount: 0,
    });

    (mockProvider.send as any).mockRejectedValue(new Error('Network socket disconnected'));

    await expect(worker.processEvent(createMessageContext(sampleEvent))).rejects.toThrow(
      'Network socket disconnected'
    );

    // Kafka offset will NOT be committed because handler threw error
    expect(prisma.notificationDelivery.update).not.toHaveBeenCalled();
  });
});
