import { z } from 'zod';

export const emailPayloadSchema = z.object({
  subject: z.string().min(1, 'Subject is required'),
  body: z.string().optional(),
  text: z.string().optional(),
  html: z.string().optional(),
  to: z.string().email().optional(),
  recipient: z.string().email().optional(),
  templateData: z.record(z.unknown()).optional(),
}).refine(
  (data) => Boolean(data.body || data.text || data.html),
  { message: 'At least one of body, text, or html must be provided' }
);

export type EmailPayload = z.infer<typeof emailPayloadSchema>;

export function validateEmailPayload(payload: Record<string, unknown>): {
  valid: boolean;
  data?: EmailPayload;
  error?: string;
} {
  const result = emailPayloadSchema.safeParse(payload);
  if (!result.success) {
    return {
      valid: false,
      error: result.error.errors.map((e) => `${e.path.join('.')}: ${e.message}`).join(', '),
    };
  }
  return { valid: true, data: result.data };
}

export function resolveEmailRecipient(
  user: { id: string; email?: string | null },
  payload: Record<string, unknown>
): { recipient?: string; error?: string } {
  const recipient =
    (typeof payload.to === 'string' && payload.to) ||
    (typeof payload.recipient === 'string' && payload.recipient) ||
    (typeof user.email === 'string' && user.email) ||
    undefined;

  if (!recipient) {
    return { error: `No email address found in payload or user profile (userId: ${user.id})` };
  }

  return { recipient };
}
