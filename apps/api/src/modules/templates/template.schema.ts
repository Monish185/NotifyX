import { z } from 'zod';
import { Channel } from '@notifyx/shared';

const channelValues = Object.values(Channel) as [string, ...string[]];

export const createTemplateSchema = z.object({
  key: z
    .string()
    .trim()
    .min(1, 'Template key is required')
    .max(50)
    .regex(/^[a-z0-9-_]+$/, 'Key must be lowercase alphanumeric, hyphens, and underscores only'),
  name: z.string().trim().min(1, 'Template name is required').max(100),
  description: z.string().trim().max(500).optional(),
  channel: z.enum(channelValues as [Channel, ...Channel[]]),
  subject: z.string().trim().max(255).optional(),
  body: z.string().min(1, 'Template body is required').max(65536, 'Template body exceeds 64 KB limit'),
  variables: z.array(z.string().trim()).optional(),
  activateNow: z.boolean().default(true),
});

export type CreateTemplateInput = z.infer<typeof createTemplateSchema>;

export const createTemplateVersionSchema = z.object({
  channel: z.enum(channelValues as [Channel, ...Channel[]]),
  subject: z.string().trim().max(255).optional(),
  body: z.string().min(1, 'Template body is required').max(65536, 'Template body exceeds 64 KB limit'),
  variables: z.array(z.string().trim()).optional(),
  activateNow: z.boolean().default(false),
});

export type CreateTemplateVersionInput = z.infer<typeof createTemplateVersionSchema>;

export const templateParamsSchema = z.object({
  idOrKey: z.string().min(1, 'Template ID or key is required'),
});

export const templateVersionParamsSchema = z.object({
  id: z.string().min(1, 'Template ID is required'),
  versionId: z.string().min(1, 'Version ID is required'),
});

export const listTemplatesQuerySchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  limit: z.coerce.number().int().min(1).max(100).default(20),
  channel: z.enum(channelValues as [Channel, ...Channel[]]).optional(),
});
