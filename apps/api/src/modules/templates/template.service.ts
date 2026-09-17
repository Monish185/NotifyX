import { prisma } from '@notifyx/database';
import {
  extractTemplateVariables,
  TemplateStatus,
  type Channel,
} from '@notifyx/shared';
import {
  templatesCreatedCounter,
} from '@notifyx/metrics';
import { cacheService } from '../../services/cache.service.js';
import type {
  CreateTemplateInput,
  CreateTemplateVersionInput,
} from './template.schema.js';

export class TemplateService {
  async createTemplate(tenantId: string, input: CreateTemplateInput) {
    // Check if template with this key already exists for tenant
    const existing = await prisma.notificationTemplate.findUnique({
      where: {
        tenantId_key: {
          tenantId,
          key: input.key,
        },
      },
    });

    if (existing) {
      const error = new Error(`Template with key "${input.key}" already exists for this tenant`);
      (error as any).statusCode = 409;
      throw error;
    }

    // Determine declared or extracted variables
    const detectedVars = extractTemplateVariables(
      `${input.subject || ''} ${input.body}`
    );
    const finalVariables = input.variables && input.variables.length > 0
      ? Array.from(new Set([...input.variables, ...detectedVars]))
      : detectedVars;

    // Create template + version 1 in a transaction
    const result = await prisma.$transaction(async (tx) => {
      const template = await tx.notificationTemplate.create({
        data: {
          tenantId,
          key: input.key,
          name: input.name,
          description: input.description,
        },
      });

      const initialStatus = input.activateNow ? TemplateStatus.ACTIVE : TemplateStatus.DRAFT;

      const version = await tx.notificationTemplateVersion.create({
        data: {
          templateId: template.id,
          version: 1,
          channel: input.channel as any,
          subject: input.subject,
          body: input.body,
          variables: finalVariables as any,
          status: initialStatus as any,
        },
      });

      let updatedTemplate = template;
      if (input.activateNow) {
        updatedTemplate = await tx.notificationTemplate.update({
          where: { id: template.id },
          data: { activeVersionId: version.id },
        });
      }

      return {
        ...updatedTemplate,
        activeVersion: input.activateNow ? version : null,
        versions: [version],
      };
    });

    templatesCreatedCounter.inc({ channel: input.channel });
    await cacheService.invalidateTemplate(tenantId, input.key);

    return result;
  }

  async createVersion(
    tenantId: string,
    templateId: string,
    input: CreateTemplateVersionInput
  ) {
    return prisma.$transaction(async (tx) => {
      // Explicit tenant ownership check & lock template row
      const template = await tx.notificationTemplate.findFirst({
        where: { id: templateId, tenantId },
      });

      if (!template) {
        const error = new Error(`Template "${templateId}" not found for this tenant`);
        (error as any).statusCode = 404;
        throw error;
      }

      // Lock row to serialize version creation
      await tx.$executeRaw`
        SELECT id FROM notification_templates WHERE id = ${template.id} FOR UPDATE
      `;

      // Find highest version number
      const highestVersion = await tx.notificationTemplateVersion.findFirst({
        where: { templateId: template.id },
        orderBy: { version: 'desc' },
      });

      const nextVersionNum = (highestVersion?.version ?? 0) + 1;

      // Extract variables
      const detectedVars = extractTemplateVariables(
        `${input.subject || ''} ${input.body}`
      );
      const finalVariables = input.variables && input.variables.length > 0
        ? Array.from(new Set([...input.variables, ...detectedVars]))
        : detectedVars;

      const newVersion = await tx.notificationTemplateVersion.create({
        data: {
          templateId: template.id,
          version: nextVersionNum,
          channel: input.channel as any,
          subject: input.subject,
          body: input.body,
          variables: finalVariables as any,
          status: input.activateNow ? TemplateStatus.ACTIVE : TemplateStatus.DRAFT,
        },
      });

      if (input.activateNow) {
        // Demote previous active version
        await tx.notificationTemplateVersion.updateMany({
          where: {
            templateId: template.id,
            status: TemplateStatus.ACTIVE,
            id: { not: newVersion.id },
          },
          data: { status: TemplateStatus.ARCHIVED },
        });

        await tx.notificationTemplate.update({
          where: { id: template.id },
          data: { activeVersionId: newVersion.id },
        });
      }

      return newVersion;
    });
  }

  /**
   * Concurrency-safe version activation with strict ownership validation:
   * 1. Acquires row-level lock on the template record.
   * 2. Validates that versionId belongs to the locked template and tenant.
   * 3. Demotes current active version to ARCHIVED.
   * 4. Promotes target version to ACTIVE.
   * 5. Updates authoritative activeVersionId pointer on template.
   */
  async activateVersion(tenantId: string, templateId: string, versionId: string) {
    const result = await prisma.$transaction(async (tx) => {
      // 1. Lock template row with tenant scope
      const templates = await tx.$queryRaw<Array<{ id: string; tenantId: string; key: string }>>`
        SELECT id, "tenantId", key
        FROM notification_templates
        WHERE id = ${templateId} AND "tenantId" = ${tenantId}
        FOR UPDATE
      `;

      const template = templates[0];
      if (!template) {
        const error = new Error(`Template "${templateId}" not found for this tenant`);
        (error as any).statusCode = 404;
        throw error;
      }

      // 2. EXPLICIT OWNERSHIP CHECK: Validate target versionId belongs to the locked templateId & tenant
      const targetVersion = await tx.notificationTemplateVersion.findFirst({
        where: {
          id: versionId,
          templateId: template.id,
          template: {
            tenantId,
          },
        },
      });

      if (!targetVersion) {
        const error = new Error(
          `Version "${versionId}" does not belong to template "${templateId}" or tenant "${tenantId}"`
        );
        (error as any).statusCode = 404;
        throw error;
      }

      // 3. Demote existing active versions of this template to ARCHIVED
      await tx.notificationTemplateVersion.updateMany({
        where: {
          templateId: template.id,
          status: TemplateStatus.ACTIVE,
        },
        data: {
          status: TemplateStatus.ARCHIVED,
        },
      });

      // 4. Promote target version to ACTIVE
      const activatedVersion = await tx.notificationTemplateVersion.update({
        where: { id: targetVersion.id },
        data: { status: TemplateStatus.ACTIVE },
      });

      // 5. Update authoritative activeVersionId on template
      await tx.notificationTemplate.update({
        where: { id: template.id },
        data: { activeVersionId: activatedVersion.id },
      });

      return {
        templateId: template.id,
        templateKey: template.key,
        activatedVersion,
      };
    });

    // Invalidate non-authoritative cache after transaction commits
    await cacheService.invalidateTemplate(tenantId, result.templateKey);

    return result.activatedVersion;
  }

  async archiveVersion(tenantId: string, templateId: string, versionId: string) {
    return prisma.$transaction(async (tx) => {
      const template = await tx.notificationTemplate.findFirst({
        where: { id: templateId, tenantId },
      });

      if (!template) {
        const error = new Error(`Template "${templateId}" not found`);
        (error as any).statusCode = 404;
        throw error;
      }

      const version = await tx.notificationTemplateVersion.findFirst({
        where: { id: versionId, templateId: template.id },
      });

      if (!version) {
        const error = new Error(`Version "${versionId}" not found`);
        (error as any).statusCode = 404;
        throw error;
      }

      const archived = await tx.notificationTemplateVersion.update({
        where: { id: version.id },
        data: { status: TemplateStatus.ARCHIVED },
      });

      // If this was the active version, clear activeVersionId
      if (template.activeVersionId === version.id) {
        await tx.notificationTemplate.update({
          where: { id: template.id },
          data: { activeVersionId: null },
        });
        await cacheService.invalidateTemplate(tenantId, template.key);
      }

      return archived;
    });
  }

  async listTemplates(tenantId: string, page = 1, limit = 20, channel?: Channel) {
    const skip = (page - 1) * limit;
    const where: any = { tenantId };

    if (channel) {
      where.versions = {
        some: { channel },
      };
    }

    const [total, data] = await Promise.all([
      prisma.notificationTemplate.count({ where }),
      prisma.notificationTemplate.findMany({
        where,
        skip,
        take: limit,
        orderBy: { updatedAt: 'desc' },
        include: {
          activeVersion: true,
          versions: {
            orderBy: { version: 'desc' },
            take: 5,
          },
        },
      }),
    ]);

    return {
      data,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
      },
    };
  }

  async getTemplate(tenantId: string, idOrKey: string) {
    const template = await prisma.notificationTemplate.findFirst({
      where: {
        tenantId,
        OR: [{ id: idOrKey }, { key: idOrKey }],
      },
      include: {
        activeVersion: true,
        versions: {
          orderBy: { version: 'desc' },
        },
      },
    });

    if (!template) {
      const error = new Error(`Template "${idOrKey}" not found`);
      (error as any).statusCode = 404;
      throw error;
    }

    return template;
  }

  /**
   * Resolves the authoritative active version of a template by key or ID.
   * Checks non-authoritative Redis cache first, falling back to PostgreSQL.
   */
  async resolveActiveTemplate(tenantId: string, idOrKey: string) {
    // 1. Try non-authoritative cache
    const cached = await cacheService.getCachedTemplate(tenantId, idOrKey);
    if (cached) {
      return cached;
    }

    // 2. Query PostgreSQL
    const template = await prisma.notificationTemplate.findFirst({
      where: {
        tenantId,
        OR: [{ id: idOrKey }, { key: idOrKey }],
      },
      include: {
        activeVersion: true,
      },
    });

    if (!template) {
      return null;
    }

    if (!template.activeVersion) {
      const error = new Error(`Template "${idOrKey}" does not have an ACTIVE version`);
      (error as any).statusCode = 400;
      throw error;
    }

    // Cache active template version
    await cacheService.setCachedTemplate(tenantId, idOrKey, template.activeVersion);

    return template.activeVersion;
  }
}

export const templateService = new TemplateService();
