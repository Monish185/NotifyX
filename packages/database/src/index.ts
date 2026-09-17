import { PrismaClient } from '@prisma/client';
import dotenv from 'dotenv';
import path from 'path';

// Ensure .env is loaded if DATABASE_URL is not yet in process.env
if (!process.env.DATABASE_URL) {
  dotenv.config({ path: path.resolve(process.cwd(), '.env') });
  dotenv.config({ path: path.resolve(process.cwd(), '../../.env') });
}

declare global {
  // eslint-disable-next-line no-var
  var __notifyx_prisma__: PrismaClient | undefined;
}

export const prisma =
  globalThis.__notifyx_prisma__ ??
  new PrismaClient({
    log:
      process.env.NODE_ENV === 'development'
        ? ['query', 'error', 'warn']
        : ['error'],
  });

if (process.env.NODE_ENV !== 'production') {
  globalThis.__notifyx_prisma__ = prisma;
}

export * from '@prisma/client';
