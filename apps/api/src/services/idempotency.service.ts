import crypto from 'node:crypto';
import { prisma } from '@notifyx/database';
import {
  idempotencyHitsCounter,
  idempotencyConflictsCounter,
} from '@notifyx/metrics';

/**
 * Deterministically canonicalizes a JSON value by sorting object keys recursively.
 */
export function canonicalizeJson(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return '[' + value.map((item) => canonicalizeJson(item)).join(',') + ']';
  }

  const keys = Object.keys(value as Record<string, unknown>).sort();
  const pairs = keys.map(
    (k) => `${JSON.stringify(k)}:${canonicalizeJson((value as Record<string, unknown>)[k])}`
  );
  return '{' + pairs.join(',') + '}';
}

/**
 * Computes a SHA-256 hash of the normalized request.
 */
export function computeRequestHash(route: string, body: unknown): string {
  const canonicalBody = canonicalizeJson(body ?? {});
  return crypto
    .createHash('sha256')
    .update(`${route}:${canonicalBody}`)
    .digest('hex');
}

export interface StoredIdempotencyResult {
  hit: boolean;
  statusCode: number;
  payload: Record<string, unknown>;
}

export class IdempotencyService {
  /**
   * Checks if an idempotency record already exists for the given tenant and key.
   * If found with a matching hash, returns the recorded response payload.
   * If found with a differing hash, throws HTTP 409 Conflict.
   * If not found, returns null so execution can proceed.
   */
  async checkIdempotency(
    tenantId: string,
    key: string,
    requestHash: string
  ): Promise<StoredIdempotencyResult | null> {
    const existing = await prisma.idempotencyRecord.findUnique({
      where: {
        tenantId_key: {
          tenantId,
          key,
        },
      },
    });

    if (!existing) {
      return null;
    }

    if (existing.requestHash !== requestHash) {
      idempotencyConflictsCounter.inc();
      const error = new Error(
        `Idempotency-Key "${key}" was previously used with a different request payload`
      );
      (error as any).statusCode = 409;
      throw error;
    }

    idempotencyHitsCounter.inc();
    return {
      hit: true,
      statusCode: existing.statusCode,
      payload: (existing.responsePayload || {}) as Record<string, unknown>,
    };
  }
}

export const idempotencyService = new IdempotencyService();
