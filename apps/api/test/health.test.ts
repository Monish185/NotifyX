import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/app.js';

describe('GET /health', () => {
  let app: FastifyInstance;

  beforeAll(async () => {
    app = await buildApp();
    await app.ready();
  });

  afterAll(async () => {
    await app.close();
  });

  it('should return HTTP 200 with { status: "ok" }', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/health',
    });

    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.payload);
    expect(body).toEqual({ status: 'ok' });
  });

  it('should return HTTP 200 on /live (liveness)', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/live',
    });

    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.payload);
    expect(body.status).toBe('ok');
    expect(body.uptime).toBeTypeOf('number');
  });

  it('should expose Prometheus metrics on /metrics', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/metrics',
    });

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('text/plain');
    expect(response.payload).toContain('http_request_duration_seconds');
  });

  it('should echo correlation ID and request ID in response headers', async () => {
    const customCorrelationId = 'corr_test_custom_123';
    const response = await app.inject({
      method: 'GET',
      url: '/health',
      headers: {
        'x-correlation-id': customCorrelationId,
      },
    });

    expect(response.statusCode).toBe(200);
    expect(response.headers['x-correlation-id']).toBe(customCorrelationId);
    expect(response.headers['x-request-id']).toBeDefined();
  });
});
