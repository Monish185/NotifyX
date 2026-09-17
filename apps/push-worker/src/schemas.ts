import { z } from 'zod';

export const pushPayloadSchema = z.object({
  title: z.string({ required_error: 'Title is required' }).min(1, 'Title is required'),
  body: z.string({ required_error: 'Body is required' }).min(1, 'Body is required'),
  deviceToken: z.string().optional(),
  token: z.string().optional(),
  data: z.record(z.unknown()).optional(),
});

export type PushPayload = z.infer<typeof pushPayloadSchema>;

export function validatePushPayload(payload: Record<string, unknown>): {
  valid: boolean;
  data?: PushPayload;
  error?: string;
} {
  const result = pushPayloadSchema.safeParse(payload);
  if (!result.success) {
    return {
      valid: false,
      error: result.error.errors.map((e) => `${e.path.join('.')}: ${e.message}`).join(', '),
    };
  }
  return { valid: true, data: result.data };
}

export function resolvePushRecipient(
  user: { id: string },
  payload: Record<string, unknown>
): { recipient?: string; error?: string } {
  const token =
    (typeof payload.deviceToken === 'string' && payload.deviceToken) ||
    (typeof payload.token === 'string' && payload.token) ||
    `device_token_user_${user.id}`;

  return { recipient: token };
}
