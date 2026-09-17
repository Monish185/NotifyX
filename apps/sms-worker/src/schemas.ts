import { z } from 'zod';

export const smsPayloadSchema = z.object({
  message: z.string().min(1, 'Message is required').optional(),
  body: z.string().min(1, 'Body is required').optional(),
  text: z.string().min(1, 'Text is required').optional(),
  phoneNumber: z.string().optional(),
  to: z.string().optional(),
  phone: z.string().optional(),
}).refine(
  (data) => Boolean(data.message || data.body || data.text),
  { message: 'At least one of message, body, or text must be provided' }
);

export type SmsPayload = z.infer<typeof smsPayloadSchema>;

export function validateSmsPayload(payload: Record<string, unknown>): {
  valid: boolean;
  data?: SmsPayload;
  error?: string;
} {
  const result = smsPayloadSchema.safeParse(payload);
  if (!result.success) {
    return {
      valid: false,
      error: result.error.errors.map((e) => `${e.path.join('.')}: ${e.message}`).join(', '),
    };
  }
  return { valid: true, data: result.data };
}

export function resolveSmsRecipient(
  user: { id: string; phone?: string | null },
  payload: Record<string, unknown>
): { recipient?: string; error?: string } {
  const phone =
    (typeof payload.phoneNumber === 'string' && payload.phoneNumber) ||
    (typeof payload.to === 'string' && payload.to) ||
    (typeof payload.phone === 'string' && payload.phone) ||
    (typeof user.phone === 'string' && user.phone) ||
    undefined;

  if (!phone) {
    return { error: `No phone number found in payload or user profile (userId: ${user.id})` };
  }

  return { recipient: phone };
}
