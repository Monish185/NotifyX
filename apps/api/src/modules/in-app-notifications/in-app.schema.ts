import { z } from 'zod';

export const listInAppQuerySchema = z.object({
  userId: z.string().min(1, 'userId is required'),
  unreadOnly: z
    .string()
    .optional()
    .transform((val) => val === 'true'),
  page: z
    .string()
    .optional()
    .transform((val) => (val ? parseInt(val, 10) : 1))
    .pipe(z.number().int().positive())
    .default('1' as any),
  limit: z
    .string()
    .optional()
    .transform((val) => (val ? parseInt(val, 10) : 20))
    .pipe(z.number().int().positive().max(100))
    .default('20' as any),
});

export type ListInAppQuery = z.infer<typeof listInAppQuerySchema>;

export const inAppIdParamSchema = z.object({
  id: z.string().min(1, 'Notification ID is required'),
});

export type InAppIdParam = z.infer<typeof inAppIdParamSchema>;

export const readAllBodySchema = z.object({
  userId: z.string().min(1, 'userId is required'),
});

export type ReadAllBody = z.infer<typeof readAllBodySchema>;
