import { z } from 'zod';
import { Channel, NotificationCategory } from '@notifyx/shared';

const channelValues = Object.values(Channel) as [string, ...string[]];
const categoryValues = Object.values(NotificationCategory) as [string, ...string[]];

export const userPreferenceParamsSchema = z.object({
  userId: z.string().min(1, 'User ID is required'),
});

export const updatePreferenceSchema = z.object({
  category: z.enum(categoryValues as [NotificationCategory, ...NotificationCategory[]]),
  channel: z.enum(channelValues as [Channel, ...Channel[]]),
  enabled: z.boolean(),
});

export type UpdatePreferenceInput = z.infer<typeof updatePreferenceSchema>;

export const bulkUpdatePreferencesSchema = z.object({
  preferences: z.array(updatePreferenceSchema).min(1),
});

export type BulkUpdatePreferencesInput = z.infer<typeof bulkUpdatePreferencesSchema>;
