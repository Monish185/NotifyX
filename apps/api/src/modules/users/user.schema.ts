import { z } from 'zod';

export const createUserSchema = z.object({
  externalId: z.string().trim().min(1, 'External ID is required').max(100),
  email: z.string().trim().email('Invalid email address').optional(),
  phone: z.string().trim().optional(),
  tenantId: z.string().min(1).optional(),
});

export type CreateUserInput = z.infer<typeof createUserSchema>;

export const getUserParamsSchema = z.object({
  id: z.string().min(1, 'User ID is required'),
});

export type GetUserParams = z.infer<typeof getUserParamsSchema>;
