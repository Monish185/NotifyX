import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/app.js';
import { prisma } from '@notifyx/database';

describe('API Key Authentication & Security', () => {
  let app: FastifyInstance;
  let tenantId: string;
  let validApiKey: string;
  let apiKeyId: string;

  beforeAll(async () => {
    app = await buildApp();
    await app.ready();

    // Setup test tenant
    const tenant = await prisma.tenant.create({
      data: {
        name: 'Auth Test Tenant',
        slug: `auth-tenant-${Date.now()}`,
      },
    });
    tenantId = tenant.id;

    // Create an API key
    const keyRes = await app.inject({
      method: 'POST',
      url: '/v1/api-keys',
      payload: {
        name: 'Primary Key',
        env: 'TEST',
        tenantId,
      },
    });
    const keyBody = JSON.parse(keyRes.payload);
    validApiKey = keyBody.key;
    apiKeyId = keyBody.id;
  });

  afterAll(async () => {
    await app.close();
  });

  it('should authenticate with a valid API key', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/api-keys',
      headers: {
        authorization: `Bearer ${validApiKey}`,
      },
    });

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.payload);
    expect(Array.isArray(body)).toBe(true);
    expect(body.length).toBeGreaterThan(0);
  });

  it('should return 401 when Authorization header is missing', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/api-keys',
    });

    expect(res.statusCode).toBe(401);
    const body = JSON.parse(res.payload);
    expect(body.error).toBe('Unauthorized');
  });

  it('should return 401 when Authorization header is malformed', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/api-keys',
      headers: {
        authorization: `Basic ${validApiKey}`,
      },
    });

    expect(res.statusCode).toBe(401);
  });

  it('should return 401 when API key format is invalid', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/api-keys',
      headers: {
        authorization: 'Bearer invalid_prefix_12345',
      },
    });

    expect(res.statusCode).toBe(401);
  });

  it('should return 401 when API key does not exist', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/api-keys',
      headers: {
        authorization: 'Bearer nx_test_nonexistentsecretkey1234567890',
      },
    });

    expect(res.statusCode).toBe(401);
  });

  it('should verify secret is not stored in plaintext in PostgreSQL', async () => {
    const dbRecord = await prisma.apiKey.findUnique({
      where: { id: apiKeyId },
    });

    expect(dbRecord).not.toBeNull();
    // Must NOT contain the full key anywhere in plaintext
    expect(dbRecord!.keyHash).not.toBe(validApiKey);
    expect(validApiKey.includes(dbRecord!.keyHash)).toBe(false);
  });

  it('should verify secret is not returned in the list endpoint', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/api-keys',
      headers: {
        authorization: `Bearer ${validApiKey}`,
      },
    });

    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.payload);
    for (const key of body) {
      expect(key.key).toBeUndefined();
      expect(key.keyHash).toBeUndefined();
      expect(key.prefix).toBeDefined();
    }
  });

  it('should reject a revoked API key with 401', async () => {
    // 1. Create a key to revoke
    const createRes = await app.inject({
      method: 'POST',
      url: '/v1/api-keys',
      payload: {
        name: 'Key To Revoke',
        env: 'TEST',
        tenantId,
      },
    });
    const { id: keyToRevokeId, key: keyToRevokeSecret } = JSON.parse(createRes.payload);

    // 2. Soft-revoke the key
    const revokeRes = await app.inject({
      method: 'DELETE',
      url: `/v1/api-keys/${keyToRevokeId}`,
      headers: {
        authorization: `Bearer ${validApiKey}`,
      },
    });
    expect(revokeRes.statusCode).toBe(200);

    // 3. Attempt to use revoked key -> 401
    const useRes = await app.inject({
      method: 'GET',
      url: '/v1/api-keys',
      headers: {
        authorization: `Bearer ${keyToRevokeSecret}`,
      },
    });
    expect(useRes.statusCode).toBe(401);
    const body = JSON.parse(useRes.payload);
    expect(body.message).toMatch(/revoked/i);
  });

  it('should reject an expired API key with 401', async () => {
    // Create an expired key directly in database
    const crypto = await import('crypto');
    const expiredSecret = `nx_test_expired_${Date.now()}`;
    const keyHash = crypto.createHash('sha256').update(expiredSecret).digest('hex');

    await prisma.apiKey.create({
      data: {
        tenantId,
        name: 'Expired Key',
        keyHash,
        prefix: expiredSecret.substring(0, 16),
        env: 'TEST',
        expiresAt: new Date(Date.now() - 60000), // 1 min ago
      },
    });

    const res = await app.inject({
      method: 'GET',
      url: '/v1/api-keys',
      headers: {
        authorization: `Bearer ${expiredSecret}`,
      },
    });

    expect(res.statusCode).toBe(401);
    const body = JSON.parse(res.payload);
    expect(body.message).toMatch(/expired/i);
  });
});
