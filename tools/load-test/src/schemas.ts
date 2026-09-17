import { type ConsumerMessageContext } from '@notifyx/kafka';

export function validateEmailPayload(payload: Record<string, unknown>): {
  valid: boolean;
  data?: Record<string, unknown>;
  error?: string;
} {
  if (!payload || typeof payload !== 'object') {
    return { valid: false, error: 'Payload must be an object' };
  }
  if (!payload.subject || typeof payload.subject !== 'string') {
    return { valid: false, error: 'Subject is required' };
  }
  return { valid: true, data: payload };
}

export function resolveEmailRecipient(
  user: { id: string; email?: string | null },
  payload: Record<string, unknown>
): { recipient?: string; error?: string } {
  const recipient = (typeof payload.to === 'string' && payload.to) || user.email || undefined;
  if (!recipient) {
    return { error: 'No recipient email found' };
  }
  return { recipient };
}

export function validateSmsPayload(payload: Record<string, unknown>): {
  valid: boolean;
  data?: Record<string, unknown>;
  error?: string;
} {
  if (!payload || typeof payload !== 'object') {
    return { valid: false, error: 'Payload must be an object' };
  }
  if (!payload.message || typeof payload.message !== 'string') {
    return { valid: false, error: 'Message is required' };
  }
  return { valid: true, data: payload };
}

export function resolveSmsRecipient(
  user: { id: string; phone?: string | null },
  payload: Record<string, unknown>
): { recipient?: string; error?: string } {
  const recipient = (typeof payload.to === 'string' && payload.to) || user.phone || undefined;
  if (!recipient) {
    return { error: 'No recipient phone found' };
  }
  return { recipient };
}

export function createMockKafkaContext<T>(topic: string, parsedPayload: T, offset = '0'): ConsumerMessageContext<T> {
  return {
    topic,
    partition: 0,
    offset,
    key: null,
    value: null,
    headers: {},
    timestamp: Date.now().toString(),
    parsedPayload,
    heartbeat: async () => {},
  };
}
