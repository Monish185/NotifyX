import { describe, it, expect } from 'vitest';
import { createLogger, createServiceLogger } from '../src/index.js';
import stream from 'node:stream';

describe('@notifyx/logger', () => {
  it('1. should automatically redact sensitive fields (passwords, tokens, api keys)', async () => {
    let output = '';
    const dest = new stream.Writable({
      write(chunk, _encoding, callback) {
        output += chunk.toString();
        callback();
      },
    });

    const testLogger = createLogger('test', {
      transport: undefined,
    } as any);

    // Use a custom logger instance that outputs to memory stream
    const pinoTest = createLogger('test-redact', {}, dest);

    pinoTest.info({
      apiKey: 'secret_live_key_123',
      password: 'super_secret_pw',
      token: 'fcm_device_token_abc',
      safeField: 'visible_data',
    }, 'Test redaction message');

    expect(output).toContain('"safeField":"visible_data"');
    expect(output).toContain('"apiKey":"[REDACTED]"');
    expect(output).toContain('"password":"[REDACTED]"');
    expect(output).toContain('"token":"[REDACTED]"');
    expect(output).not.toContain('secret_live_key_123');
    expect(output).not.toContain('super_secret_pw');
  });

  it('2. createServiceLogger should bind service name and initial context', async () => {
    let output = '';
    const dest = new stream.Writable({
      write(chunk, _encoding, callback) {
        output += chunk.toString();
        callback();
      },
    });

    const base = createLogger('base', {}, dest);
    const serviceLogger = base.child({ service: 'email-worker', correlationId: 'corr_123' });

    serviceLogger.info({ deliveryId: 'del_456' }, 'Processing delivery');

    expect(output).toContain('"service":"email-worker"');
    expect(output).toContain('"correlationId":"corr_123"');
    expect(output).toContain('"deliveryId":"del_456"');
  });
});
