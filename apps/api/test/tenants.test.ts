import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/app.js';
import { prisma } from '@notifyx/database';

describe('Tenant Domain Endpoints', () => {
  let app: FastifyInstance;

  beforeAll(async () => {
    app = await buildApp();
    await app.ready();
  });

  afterAll(async () => {
    await app.close();
  });

  it('should create a new tenant (POST /v1/tenants)', async () => {
    const slug = `tenant-${Date.now()}`;
    const res = await app.inject({
      method: 'POST',
      url: '/v1/tenants',
      payload: {
        name: 'Test Tenant',
        slug,
      },
    });

    expect(res.statusCode).toBe(201);
    const body = JSON.parse(res.payload);
    expect(body.id).toBeDefined();
    expect(body.name).toBe('Test Tenant');
    expect(body.slug).toBe(slug);
  });

  it('should reject duplicate tenant slug with 409', async () => {
    const slug = `dup-${Date.now()}`;
    await app.inject({
      method: 'POST',
      url: '/v1/tenants',
      payload: { name: 'First', slug },
    });

    const res = await app.inject({
      method: 'POST',
      url: '/v1/tenants',
      payload: { name: 'Second', slug },
    });

    expect(res.statusCode).toBe(409);
  });

  it('should retrieve an existing tenant by ID (GET /v1/tenants/:id)', async () => {
    const slug = `lookup-${Date.now()}`;
    const createRes = await app.inject({
      method: 'POST',
      url: '/v1/tenants',
      payload: { name: 'Lookup Tenant', slug },
    });
    const created = JSON.parse(createRes.payload);

    const getRes = await app.inject({
      method: 'GET',
      url: `/v1/tenants/${created.id}`,
    });

    expect(getRes.statusCode).toBe(200);
    const body = JSON.parse(getRes.payload);
    expect(body.id).toBe(created.id);
    expect(body.name).toBe('Lookup Tenant');
  });

  it('should return 404 for non-existent tenant', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/v1/tenants/non-existent-id',
    });

    expect(res.statusCode).toBe(404);
  });
});
