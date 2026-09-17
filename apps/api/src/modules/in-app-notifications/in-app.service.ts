import { prisma } from '@notifyx/database';
import type { ListInAppQuery } from './in-app.schema.js';

export class InAppNotificationService {
  async resolveUserId(tenantId: string, userIdentifier: string): Promise<string> {
    const user = await prisma.user.findFirst({
      where: {
        tenantId,
        OR: [{ id: userIdentifier }, { externalId: userIdentifier }],
      },
    });

    if (!user) {
      const error = new Error(`User "${userIdentifier}" not found in this tenant`);
      (error as any).statusCode = 404;
      throw error;
    }

    return user.id;
  }

  async listInAppNotifications(tenantId: string, query: ListInAppQuery) {
    const internalUserId = await this.resolveUserId(tenantId, query.userId);
    const { unreadOnly, page, limit } = query;

    const where: any = {
      tenantId,
      userId: internalUserId,
    };

    if (unreadOnly) {
      where.readAt = null;
    }

    const [total, unreadCount, data] = await Promise.all([
      prisma.inAppNotification.count({ where }),
      prisma.inAppNotification.count({
        where: { tenantId, userId: internalUserId, readAt: null },
      }),
      prisma.inAppNotification.findMany({
        where,
        orderBy: { createdAt: 'desc' },
        skip: (page - 1) * limit,
        take: limit,
      }),
    ]);

    return {
      data,
      unreadCount,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit) || 1,
      },
    };
  }

  async getUnreadCount(tenantId: string, userIdentifier: string): Promise<{ unreadCount: number }> {
    const internalUserId = await this.resolveUserId(tenantId, userIdentifier);
    const count = await prisma.inAppNotification.count({
      where: {
        tenantId,
        userId: internalUserId,
        readAt: null,
      },
    });

    return { unreadCount: count };
  }

  async markAsRead(tenantId: string, id: string) {
    const notification = await prisma.inAppNotification.findFirst({
      where: { id, tenantId },
    });

    if (!notification) {
      const error = new Error(`In-app notification "${id}" not found`);
      (error as any).statusCode = 404;
      throw error;
    }

    if (notification.readAt) {
      return notification; // Already read
    }

    return prisma.inAppNotification.update({
      where: { id },
      data: { readAt: new Date() },
    });
  }

  async markAllAsRead(tenantId: string, userIdentifier: string) {
    const internalUserId = await this.resolveUserId(tenantId, userIdentifier);

    const updateResult = await prisma.inAppNotification.updateMany({
      where: {
        tenantId,
        userId: internalUserId,
        readAt: null,
      },
      data: { readAt: new Date() },
    });

    return {
      updatedCount: updateResult.count,
    };
  }
}

export const inAppNotificationService = new InAppNotificationService();
