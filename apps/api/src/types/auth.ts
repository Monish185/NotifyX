import '@fastify/sensible';

export interface TenantContext {
  id: string;
  apiKeyId: string;
  environment: 'LIVE' | 'TEST';
}

declare module 'fastify' {
  interface FastifyRequest {
    tenant?: TenantContext;
  }
}
