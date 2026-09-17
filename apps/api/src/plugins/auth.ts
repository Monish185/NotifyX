import type { FastifyRequest, FastifyReply } from 'fastify';
import crypto from 'crypto';
import { prisma } from '@notifyx/database';
import '../types/auth.js';

export async function authenticateApiKey(
  request: FastifyRequest,
  reply: FastifyReply
): Promise<void> {
  const authHeader = request.headers.authorization;

  if (!authHeader) {
    return reply.status(401).send({
      statusCode: 401,
      error: 'Unauthorized',
      message: 'Missing or malformed Authorization header. Expected Bearer token.',
    });
  }

  const [scheme, token] = authHeader.split(' ');

  if (scheme !== 'Bearer' || !token) {
    return reply.status(401).send({
      statusCode: 401,
      error: 'Unauthorized',
      message: 'Invalid authorization scheme. Expected "Bearer <api_key>".',
    });
  }

  if (!token.startsWith('nx_test_') && !token.startsWith('nx_live_')) {
    return reply.status(401).send({
      statusCode: 401,
      error: 'Unauthorized',
      message: 'Invalid API key format.',
    });
  }

  const keyHash = crypto.createHash('sha256').update(token).digest('hex');

  const apiKey = await prisma.apiKey.findUnique({
    where: { keyHash },
    select: {
      id: true,
      tenantId: true,
      env: true,
      revokedAt: true,
      expiresAt: true,
    },
  });

  if (!apiKey) {
    return reply.status(401).send({
      statusCode: 401,
      error: 'Unauthorized',
      message: 'Invalid API key.',
    });
  }

  if (apiKey.revokedAt) {
    return reply.status(401).send({
      statusCode: 401,
      error: 'Unauthorized',
      message: 'API key has been revoked.',
    });
  }

  if (apiKey.expiresAt && apiKey.expiresAt < new Date()) {
    return reply.status(401).send({
      statusCode: 401,
      error: 'Unauthorized',
      message: 'API key has expired.',
    });
  }

  request.tenant = {
    id: apiKey.tenantId,
    apiKeyId: apiKey.id,
    environment: apiKey.env as 'LIVE' | 'TEST',
  };
}
