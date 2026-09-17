import crypto from 'crypto';
import { prisma, type ApiKey } from '@notifyx/database';
import type { CreateApiKeyInput } from './api-key.schema.js';

export interface CreatedApiKeyResponse {
  id: string;
  name: string;
  env: 'LIVE' | 'TEST';
  prefix: string;
  key: string;
  createdAt: Date;
}

export interface ApiKeyMetadata {
  id: string;
  name: string;
  prefix: string;
  env: 'LIVE' | 'TEST';
  revokedAt: Date | null;
  expiresAt: Date | null;
  createdAt: Date;
}

export class ApiKeyService {
  async createApiKey(tenantId: string, input: CreateApiKeyInput): Promise<CreatedApiKeyResponse> {
    const env = input.env ?? 'TEST';
    const keyPrefix = `nx_${env.toLowerCase()}_`;
    const randomSecret = crypto.randomBytes(24).toString('base64url');
    const fullKey = `${keyPrefix}${randomSecret}`;
    const keyHash = crypto.createHash('sha256').update(fullKey).digest('hex');
    const displayPrefix = fullKey.substring(0, 16);

    const apiKey = await prisma.apiKey.create({
      data: {
        tenantId,
        name: input.name,
        keyHash,
        prefix: displayPrefix,
        env,
      },
    });

    return {
      id: apiKey.id,
      name: apiKey.name,
      env: apiKey.env as 'LIVE' | 'TEST',
      prefix: apiKey.prefix,
      key: fullKey,
      createdAt: apiKey.createdAt,
    };
  }

  async listApiKeys(tenantId: string): Promise<ApiKeyMetadata[]> {
    const keys = await prisma.apiKey.findMany({
      where: { tenantId },
      select: {
        id: true,
        name: true,
        prefix: true,
        env: true,
        revokedAt: true,
        expiresAt: true,
        createdAt: true,
      },
      orderBy: { createdAt: 'desc' },
    });

    return keys.map((k) => ({
      ...k,
      env: k.env as 'LIVE' | 'TEST',
    }));
  }

  async revokeApiKey(id: string, tenantId: string): Promise<ApiKeyMetadata> {
    const apiKey = await prisma.apiKey.findFirst({
      where: { id, tenantId },
    });

    if (!apiKey) {
      const error = new Error(`API key with ID "${id}" not found`);
      (error as any).statusCode = 404;
      throw error;
    }

    if (apiKey.revokedAt) {
      return {
        id: apiKey.id,
        name: apiKey.name,
        prefix: apiKey.prefix,
        env: apiKey.env as 'LIVE' | 'TEST',
        revokedAt: apiKey.revokedAt,
        expiresAt: apiKey.expiresAt,
        createdAt: apiKey.createdAt,
      };
    }

    const updated = await prisma.apiKey.update({
      where: { id },
      data: { revokedAt: new Date() },
      select: {
        id: true,
        name: true,
        prefix: true,
        env: true,
        revokedAt: true,
        expiresAt: true,
        createdAt: true,
      },
    });

    return {
      ...updated,
      env: updated.env as 'LIVE' | 'TEST',
    };
  }
}

export const apiKeyService = new ApiKeyService();
