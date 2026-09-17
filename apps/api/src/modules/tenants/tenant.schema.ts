import { z } from 'zod';

export const createTenantSchema = z.object({
  name: z.string().trim().min(1, 'Tenant name is required').max(100),
  slug: z
    .string()
    .trim()
    .min(1, 'Tenant slug is required')
    .max(50)
    .regex(/^[a-z0-9-]+$/, 'Slug must be lowercase alphanumeric and hyphens only'),
});

export type CreateTenantInput = z.infer<typeof createTenantSchema>;

export const getTenantParamsSchema = z.object({
  id: z.string().min(1, 'Tenant ID is required'),
});

export type GetTenantParams = z.infer<typeof getTenantParamsSchema>;
