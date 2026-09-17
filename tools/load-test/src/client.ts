import http from 'node:http';
import { prisma } from '@notifyx/database';
import { Channel, Priority } from '@notifyx/shared';
import crypto from 'node:crypto';

export interface TestTenantContext {
  tenantId: string;
  tenantName: string;
  apiKey: string;
  users: Array<{ id: string; externalId: string; email: string; phone: string }>;
}

export class ApiLoadClient {
  private readonly apiUrl: string;
  private readonly agent: http.Agent;

  constructor(apiUrl = 'http://127.0.0.1:3001') {
    this.apiUrl = apiUrl;
    this.agent = new http.Agent({
      keepAlive: true,
      maxSockets: 200,
      keepAliveMsecs: 60000,
    });
  }

  /**
   * Set up dedicated, isolated test tenants with user accounts and API keys.
   * Prefix: tenant_load_test_<runId>
   */
  async setupTestTenants(runId: string, tenantCount = 2): Promise<TestTenantContext[]> {
    const contexts: TestTenantContext[] = [];

    for (let i = 1; i <= tenantCount; i++) {
      const slug = `tenant_load_test_${runId}_${i}`;
      const name = `Load Test Tenant ${runId} #${i}`;

      const tenant = await prisma.tenant.create({
        data: {
          name,
          slug,
        },
      });

      // Generate API key directly in DB for reliability in test setup
      const rawKey = `nx_test_${crypto.randomBytes(16).toString('hex')}`;
      const keyHash = crypto.createHash('sha256').update(rawKey).digest('hex');

      await prisma.apiKey.create({
        data: {
          name: `Load Test Key ${i}`,
          keyHash,
          prefix: rawKey.slice(0, 12),
          env: 'TEST',
          tenantId: tenant.id,
        },
      });

      // Create test users
      const users: Array<{ id: string; externalId: string; email: string; phone: string }> = [];
      for (let u = 1; u <= 3; u++) {
        const externalId = `user_${runId}_${i}_${u}`;
        const user = await prisma.user.create({
          data: {
            tenantId: tenant.id,
            externalId,
            email: `load_${runId}_${i}_${u}@example.test`,
            phone: `+1555${String(i).padStart(3, '0')}${String(u).padStart(4, '0')}`,
          },
        });
        users.push({
          id: user.id,
          externalId,
          email: user.email!,
          phone: user.phone!,
        });
      }

      contexts.push({
        tenantId: tenant.id,
        tenantName: name,
        apiKey: rawKey,
        users,
      });
    }

    return contexts;
  }

  /**
   * Send notification request to API with precise high-resolution latency tracking.
   */
  async sendNotification(
    apiKey: string,
    payload: {
      userId: string;
      channels: Channel[];
      priority: Priority;
      templateId?: string;
      payload?: Record<string, unknown>;
    },
    correlationId?: string
  ): Promise<{
    statusCode: number;
    latencyMs: number;
    notificationId?: string;
    correlationId?: string;
    error?: string;
  }> {
    const url = new URL('/v1/notifications', this.apiUrl);
    const bodyStr = JSON.stringify(payload);

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      'Content-Length': String(Buffer.byteLength(bodyStr)),
      Authorization: `Bearer ${apiKey}`,
    };

    if (correlationId) {
      headers['x-correlation-id'] = correlationId;
    }

    const startTime = process.hrtime.bigint();

    return new Promise((resolve) => {
      const req = http.request(
        {
          hostname: url.hostname,
          port: url.port,
          path: url.pathname,
          method: 'POST',
          headers,
          agent: this.agent,
          timeout: 10000,
        },
        (res) => {
          let responseBody = '';
          res.setEncoding('utf8');
          res.on('data', (chunk) => {
            responseBody += chunk;
          });
          res.on('end', () => {
            const endTime = process.hrtime.bigint();
            const latencyMs = Number(endTime - startTime) / 1_000_000;

            let parsed: any = null;
            try {
              if (responseBody) {
                parsed = JSON.parse(responseBody);
              }
            } catch {
              // Non-JSON response
            }

            const returnedCorrId = (res.headers['x-correlation-id'] as string) || correlationId;

            resolve({
              statusCode: res.statusCode || 0,
              latencyMs,
              notificationId: parsed?.id,
              correlationId: returnedCorrId,
              error: res.statusCode && res.statusCode >= 400 ? parsed?.message || responseBody : undefined,
            });
          });
        }
      );

      req.on('timeout', () => {
        req.destroy();
        const endTime = process.hrtime.bigint();
        const latencyMs = Number(endTime - startTime) / 1_000_000;
        resolve({
          statusCode: 0,
          latencyMs,
          error: 'REQUEST_TIMEOUT',
        });
      });

      req.on('error', (err) => {
        const endTime = process.hrtime.bigint();
        const latencyMs = Number(endTime - startTime) / 1_000_000;
        resolve({
          statusCode: 0,
          latencyMs,
          error: err.message,
        });
      });

      req.write(bodyStr);
      req.end();
    });
  }

  destroy(): void {
    this.agent.destroy();
  }
}
