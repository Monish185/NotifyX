import { prisma, type User } from '@notifyx/database';
import type { CreateUserInput } from './user.schema.js';

export class UserService {
  async createUser(tenantId: string, input: CreateUserInput): Promise<User> {
    const existing = await prisma.user.findUnique({
      where: {
        tenantId_externalId: {
          tenantId,
          externalId: input.externalId,
        },
      },
    });

    if (existing) {
      const error = new Error(
        `User with externalId "${input.externalId}" already exists for this tenant`
      );
      (error as any).statusCode = 409;
      throw error;
    }

    return prisma.user.create({
      data: {
        tenantId,
        externalId: input.externalId,
        email: input.email,
        phone: input.phone,
      },
    });
  }

  async getUserById(id: string, tenantId: string): Promise<User> {
    const user = await prisma.user.findFirst({
      where: {
        tenantId,
        OR: [{ id }, { externalId: id }],
      },
    });

    if (!user) {
      const error = new Error(`User with ID "${id}" not found`);
      (error as any).statusCode = 404;
      throw error;
    }

    return user;
  }
}

export const userService = new UserService();
