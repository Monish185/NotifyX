import { prisma } from '@notifyx/database';
import {
  Channel,
  NotificationCategory,
  type Channel as ChannelType,
  type NotificationCategory as CategoryType,
} from '@notifyx/shared';
import { preferenceUpdatesCounter } from '@notifyx/metrics';
import { cacheService } from '../../services/cache.service.js';
import type {
  UpdatePreferenceInput,
  BulkUpdatePreferencesInput,
} from './preference.schema.js';

export interface EffectivePreference {
  category: CategoryType;
  channel: ChannelType;
  enabled: boolean;
  isExplicit: boolean;
}

export class PreferenceService {
  /**
   * Helper to resolve the database User record for a tenant.
   */
  async resolveUser(tenantId: string, userIdentifier: string) {
    const user = await prisma.user.findFirst({
      where: {
        tenantId,
        OR: [{ id: userIdentifier }, { externalId: userIdentifier }],
      },
    });

    if (!user) {
      const error = new Error(`User "${userIdentifier}" not found for this tenant`);
      (error as any).statusCode = 404;
      throw error;
    }

    return user;
  }

  /**
   * Computes the default preference for a (category, channel) pair.
   * TRANSACTIONAL, SECURITY, SYSTEM => enabled by default.
   * MARKETING => disabled by default.
   */
  getDefaultPreference(category: CategoryType): boolean {
    if (category === NotificationCategory.MARKETING) {
      return false;
    }
    return true;
  }

  /**
   * Returns all effective preferences for a user across all categories and channels.
   */
  async getUserPreferences(tenantId: string, userIdentifier: string) {
    const user = await this.resolveUser(tenantId, userIdentifier);

    // 1. Try non-authoritative cache
    const cached = await cacheService.getCachedPreferences(tenantId, user.id);
    if (cached) {
      return cached;
    }

    // 2. Query PostgreSQL
    const explicitPrefs = await prisma.notificationPreference.findMany({
      where: {
        tenantId,
        userId: user.id,
      },
    });

    const explicitMap = new Map<string, boolean>();
    for (const pref of explicitPrefs) {
      explicitMap.set(`${pref.category}:${pref.channel}`, pref.enabled);
    }

    const categories = Object.values(NotificationCategory) as CategoryType[];
    const channels = Object.values(Channel) as ChannelType[];

    const effective: EffectivePreference[] = [];
    for (const category of categories) {
      for (const channel of channels) {
        const key = `${category}:${channel}`;
        const hasExplicit = explicitMap.has(key);
        const enabled = hasExplicit
          ? explicitMap.get(key)!
          : this.getDefaultPreference(category);

        effective.push({
          category,
          channel,
          enabled,
          isExplicit: hasExplicit,
        });
      }
    }

    const response = {
      userId: user.id,
      externalId: user.externalId,
      preferences: effective,
      explicitCount: explicitPrefs.length,
    };

    // Cache in Redis (non-authoritative)
    await cacheService.setCachedPreferences(tenantId, user.id, response);

    return response;
  }

  /**
   * Sets or updates a single user preference.
   */
  async updatePreference(
    tenantId: string,
    userIdentifier: string,
    input: UpdatePreferenceInput
  ) {
    const user = await this.resolveUser(tenantId, userIdentifier);

    const updated = await prisma.notificationPreference.upsert({
      where: {
        tenantId_userId_category_channel: {
          tenantId,
          userId: user.id,
          category: input.category as any,
          channel: input.channel as any,
        },
      },
      create: {
        tenantId,
        userId: user.id,
        category: input.category as any,
        channel: input.channel as any,
        enabled: input.enabled,
      },
      update: {
        enabled: input.enabled,
      },
    });

    preferenceUpdatesCounter.inc({
      category: input.category,
      channel: input.channel,
      enabled: String(input.enabled),
    });

    // Invalidate non-authoritative cache
    await cacheService.invalidatePreferences(tenantId, user.id);

    return updated;
  }

  /**
   * Bulk updates multiple preferences for a user in one transaction.
   */
  async bulkUpdatePreferences(
    tenantId: string,
    userIdentifier: string,
    input: BulkUpdatePreferencesInput
  ) {
    const user = await this.resolveUser(tenantId, userIdentifier);

    const updatedRecords = await prisma.$transaction(async (tx) => {
      const results = [];
      for (const item of input.preferences) {
        const pref = await tx.notificationPreference.upsert({
          where: {
            tenantId_userId_category_channel: {
              tenantId,
              userId: user.id,
              category: item.category as any,
              channel: item.channel as any,
            },
          },
          create: {
            tenantId,
            userId: user.id,
            category: item.category as any,
            channel: item.channel as any,
            enabled: item.enabled,
          },
          update: {
            enabled: item.enabled,
          },
        });
        results.push(pref);

        preferenceUpdatesCounter.inc({
          category: item.category,
          channel: item.channel,
          enabled: String(item.enabled),
        });
      }
      return results;
    });

    await cacheService.invalidatePreferences(tenantId, user.id);

    return {
      userId: user.id,
      updated: updatedRecords,
    };
  }

  /**
   * Evaluates requested channels against user preferences.
   * Returns allowed channels (to be created as deliveries) and blocked channels.
   */
  async filterChannels(
    tenantId: string,
    userId: string,
    category: CategoryType,
    requestedChannels: ChannelType[]
  ): Promise<{ allowed: ChannelType[]; blocked: ChannelType[] }> {
    // Query explicit preferences for this category
    const explicitPrefs = await prisma.notificationPreference.findMany({
      where: {
        tenantId,
        userId,
        category: category as any,
        channel: { in: requestedChannels as any },
      },
    });

    const explicitMap = new Map<ChannelType, boolean>();
    for (const p of explicitPrefs) {
      explicitMap.set(p.channel as ChannelType, p.enabled);
    }

    const defaultEnabled = this.getDefaultPreference(category);

    const allowed: ChannelType[] = [];
    const blocked: ChannelType[] = [];

    for (const channel of requestedChannels) {
      const isEnabled = explicitMap.has(channel)
        ? explicitMap.get(channel)!
        : defaultEnabled;

      if (isEnabled) {
        allowed.push(channel);
      } else {
        blocked.push(channel);
      }
    }

    return { allowed, blocked };
  }

  /**
   * Convenience check if a specific channel and category is allowed for a user.
   */
  async isAllowed(
    tenantId: string,
    userIdentifier: string,
    channel: ChannelType,
    category: CategoryType
  ): Promise<boolean> {
    const user = await this.resolveUser(tenantId, userIdentifier);
    const { allowed } = await this.filterChannels(tenantId, user.id, category, [channel]);
    return allowed.includes(channel);
  }

  /**
   * Convenience setter for a single preference.
   */
  async setPreference(
    tenantId: string,
    userIdentifier: string,
    channel: ChannelType,
    category: CategoryType,
    enabled: boolean
  ) {
    return this.updatePreference(tenantId, userIdentifier, {
      channel,
      category,
      enabled,
    });
  }
}

export const preferenceService = new PreferenceService();
