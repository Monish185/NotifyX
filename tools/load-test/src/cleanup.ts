import { prisma } from '@notifyx/database';

export interface CleanupResult {
  tenantsDeleted: number;
  notificationsDeleted: number;
  deliveriesDeleted: number;
  outboxEventsDeleted: number;
  retryRecordsDeleted: number;
  deadLetterEventsDeleted: number;
  usersDeleted: number;
  apiKeysDeleted: number;
}

/**
 * Safely clean up test tenant data matching the prefix 'tenant_load_test_'.
 *
 * Guardrail:
 * If a specific runId is provided, cleans only 'tenant_load_test_<runId>_*'.
 * If no runId is provided, cleans all 'tenant_load_test_%'.
 * NEVER affects seed tenants or development tenants lacking the prefix.
 */
export async function cleanupLoadTestTenants(runId?: string): Promise<CleanupResult> {
  const prefix = runId ? `tenant_load_test_${runId}` : 'tenant_load_test_';

  // Safety check: verify prefix begins strictly with 'tenant_load_test_'
  if (!prefix.startsWith('tenant_load_test_')) {
    throw new Error(`Security Guardrail Tripped: Refusing to clean up with unauthorized prefix '${prefix}'`);
  }

  // Find targeted tenants
  const testTenants = await prisma.tenant.findMany({
    where: {
      slug: {
        startsWith: prefix,
      },
    },
    select: { id: true, slug: true },
  });

  if (testTenants.length === 0) {
    return {
      tenantsDeleted: 0,
      notificationsDeleted: 0,
      deliveriesDeleted: 0,
      outboxEventsDeleted: 0,
      retryRecordsDeleted: 0,
      deadLetterEventsDeleted: 0,
      usersDeleted: 0,
      apiKeysDeleted: 0,
    };
  }

  const tenantIds = testTenants.map((t) => t.id);

  // Find notification IDs under these tenants
  const notifications = await prisma.notification.findMany({
    where: { tenantId: { in: tenantIds } },
    select: { id: true },
  });
  const notificationIds = notifications.map((n) => n.id);

  // Find delivery IDs under these notifications
  const deliveries = await prisma.notificationDelivery.findMany({
    where: { notificationId: { in: notificationIds } },
    select: { id: true },
  });
  const deliveryIds = deliveries.map((d) => d.id);

  // Cascading deletion in proper foreign key order
  let retryRecordsDeleted = 0;
  let deadLetterEventsDeleted = 0;
  if (deliveryIds.length > 0) {
    const resRetry = await prisma.retryRecord.deleteMany({
      where: { deliveryId: { in: deliveryIds } },
    });
    retryRecordsDeleted = resRetry.count;

    const resDlq = await prisma.deadLetterEvent.deleteMany({
      where: { deliveryId: { in: deliveryIds } },
    });
    deadLetterEventsDeleted = resDlq.count;
  }

  // Clean outbox events associated with these deliveries or tenants
  const resOutbox = await prisma.outboxEvent.deleteMany({
    where: {
      OR: tenantIds.map((tId) => ({
        payload: { path: ['tenantId'], equals: tId },
      })),
    },
  });

  const resDeliveries = await prisma.notificationDelivery.deleteMany({
    where: { notificationId: { in: notificationIds } },
  });

  const resNotifications = await prisma.notification.deleteMany({
    where: { tenantId: { in: tenantIds } },
  });

  const resApiKeys = await prisma.apiKey.deleteMany({
    where: { tenantId: { in: tenantIds } },
  });

  const resUsers = await prisma.user.deleteMany({
    where: { tenantId: { in: tenantIds } },
  });

  const resTenants = await prisma.tenant.deleteMany({
    where: { id: { in: tenantIds } },
  });

  return {
    tenantsDeleted: resTenants.count,
    notificationsDeleted: resNotifications.count,
    deliveriesDeleted: resDeliveries.count,
    outboxEventsDeleted: resOutbox.count,
    retryRecordsDeleted,
    deadLetterEventsDeleted,
    usersDeleted: resUsers.count,
    apiKeysDeleted: resApiKeys.count,
  };
}
