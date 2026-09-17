import { prisma, type Tenant } from '@notifyx/database';
import type { CreateTenantInput } from './tenant.schema.js';

export class TenantService {
  async createTenant(input: CreateTenantInput): Promise<Tenant> {
    const existing = await prisma.tenant.findUnique({
      where: { slug: input.slug },
    });

    if (existing) {
      const error = new Error(`Tenant with slug "${input.slug}" already exists`);
      (error as any).statusCode = 409;
      throw error;
    }

    return prisma.tenant.create({
      data: {
        name: input.name,
        slug: input.slug,
      },
    });
  }

  async getTenantById(id: string): Promise<Tenant> {
    const tenant = await prisma.tenant.findUnique({
      where: { id },
    });

    if (!tenant) {
      const error = new Error(`Tenant with ID "${id}" not found`);
      (error as any).statusCode = 404;
      throw error;
    }

    return tenant;
  }
}

export const tenantService = new TenantService();
