import { z } from 'zod';
import {
  Channel,
  Priority,
  NotificationStatus,
  NotificationCategory,
} from '@notifyx/shared';

const channelValues = Object.values(Channel) as [string, ...string[]];
const priorityValues = Object.values(Priority) as [string, ...string[]];
const statusValues = Object.values(NotificationStatus) as [string, ...string[]];
const categoryValues = Object.values(NotificationCategory) as [string, ...string[]];

export const createNotificationSchema = z.object({
  userId: z.string().trim().min(1, 'userId is required'),
  templateId: z.string().trim().optional(),
  templateData: z.record(z.unknown()).optional(),
  channels: z
    .array(z.enum(channelValues as [Channel, ...Channel[]]))
    .min(1, 'At least one notification channel must be specified')
    .refine((items: Channel[]) => new Set(items).size === items.length, {
      message: 'Channels array must not contain duplicate values',
    })
    .optional(),
  priority: z.enum(priorityValues as [Priority, ...Priority[]]).default(Priority.NORMAL),
  category: z
    .enum(categoryValues as [NotificationCategory, ...NotificationCategory[]])
    .default(NotificationCategory.TRANSACTIONAL),
  payload: z.record(z.unknown()).default({}),
  scheduleAt: z
    .string()
    .datetime({ message: 'scheduleAt must be a valid ISO 8601 timestamp' })
    .optional(),
  scheduledFor: z
    .string()
    .datetime({ message: 'scheduledFor must be a valid ISO 8601 timestamp' })
    .optional(),
});

export type CreateNotificationInput = z.infer<typeof createNotificationSchema>;

export const getNotificationParamsSchema = z.object({
  id: z.string().min(1, 'Notification ID is required'),
});

export type GetNotificationParams = z.infer<typeof getNotificationParamsSchema>;

export const cancelNotificationParamsSchema = z.object({
  id: z.string().min(1, 'Notification ID is required'),
});

export type CancelNotificationParams = z.infer<typeof cancelNotificationParamsSchema>;

export const listNotificationsQuerySchema = z.object({
  page: z
    .string()
    .or(z.number())
    .transform((v: string | number) => Math.max(1, Number(v) || 1))
    .default(1),
  limit: z
    .string()
    .or(z.number())
    .transform((v: string | number) => Math.min(100, Math.max(1, Number(v) || 20)))
    .default(20),
  userId: z.string().trim().optional(),
  status: z.enum(statusValues as [NotificationStatus, ...NotificationStatus[]]).optional(),
  channel: z.enum(channelValues as [Channel, ...Channel[]]).optional(),
  category: z.enum(categoryValues as [NotificationCategory, ...NotificationCategory[]]).optional(),
  startDate: z.string().datetime().optional(),
  endDate: z.string().datetime().optional(),
});

export type ListNotificationsQuery = z.infer<typeof listNotificationsQuerySchema>;
