import { z } from 'zod';

export const createApiKeySchema = z.object({
  name: z.string().trim().min(1, 'API key name is required').max(100),
  env: z.enum(['LIVE', 'TEST']).default('TEST'),
  tenantId: z.string().min(1).optional(),
});

export type CreateApiKeyInput = z.infer<typeof createApiKeySchema>;

export const revokeApiKeyParamsSchema = z.object({
  id: z.string().min(1, 'API key ID is required'),
});

export type RevokeApiKeyParams = z.infer<typeof revokeApiKeyParamsSchema>;
