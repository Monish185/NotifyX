import { z } from 'zod';
import dotenv from 'dotenv';
import path from 'path';

// Load .env if present
dotenv.config({ path: path.resolve(process.cwd(), '.env') });

export const envSchema = z.object({
  NODE_ENV: z
    .enum(['development', 'production', 'test', 'staging'])
    .default('development'),
  API_PORT: z
    .string()
    .or(z.number())
    .transform((val) => Number(val))
    .pipe(z.number().positive().max(65535))
    .default(3001),
  DATABASE_URL: z
    .string()
    .url()
    .min(1, 'DATABASE_URL is required'),
  REDIS_URL: z
    .string()
    .min(1, 'REDIS_URL is required')
    .default('redis://localhost:6379'),
  NEXT_PUBLIC_API_URL: z
    .string()
    .url()
    .default('http://localhost:3001'),
  KAFKA_BROKERS: z
    .string()
    .default('localhost:9092'),
  KAFKA_CLIENT_ID: z
    .string()
    .default('notifyx'),
  KAFKA_GROUP_ID: z
    .string()
    .default('notifyx-inapp-workers'),
  EMAIL_PROVIDER: z
    .enum(['mock', 'ses'])
    .default('mock'),
  PUSH_PROVIDER: z
    .enum(['mock', 'fcm'])
    .default('mock'),
  SMS_PROVIDER: z
    .enum(['mock', 'twilio'])
    .default('mock'),
  EMAIL_WORKER_PORT: z
    .coerce
    .number()
    .default(3004),
  PUSH_WORKER_PORT: z
    .coerce
    .number()
    .default(3005),
  SMS_WORKER_PORT: z
    .coerce
    .number()
    .default(3006),
  AWS_REGION: z
    .preprocess((val) => (val === '' ? undefined : val), z.string().optional()),
  EMAIL_FROM_ADDRESS: z
    .preprocess((val) => (val === '' ? undefined : val), z.string().email().optional()),
  VERIFY_EMAIL_RECIPIENT: z
    .preprocess((val) => (val === '' ? undefined : val), z.string().email().optional()),
  FIREBASE_PROJECT_ID: z
    .preprocess((val) => (val === '' ? undefined : val), z.string().optional()),
  FIREBASE_CLIENT_EMAIL: z
    .preprocess((val) => (val === '' ? undefined : val), z.string().email().optional()),
  FIREBASE_PRIVATE_KEY: z
    .preprocess((val) => (val === '' ? undefined : val), z.string().optional()),
  VERIFY_PUSH_TOKEN: z
    .preprocess((val) => (val === '' ? undefined : val), z.string().optional()),
  TWILIO_ACCOUNT_SID: z
    .preprocess((val) => (val === '' ? undefined : val), z.string().optional()),
  TWILIO_AUTH_TOKEN: z
    .preprocess((val) => (val === '' ? undefined : val), z.string().optional()),
  TWILIO_FROM_NUMBER: z
    .preprocess((val) => (val === '' ? undefined : val), z.string().optional()),
  VERIFY_SMS_RECIPIENT: z
    .preprocess((val) => (val === '' ? undefined : val), z.string().optional()),
  NOTIFYX_RETRY_MAX_ATTEMPTS: z
    .coerce
    .number()
    .int()
    .positive()
    .default(5),
  NOTIFYX_RETRY_BASE_DELAY_MS: z
    .coerce
    .number()
    .int()
    .positive()
    .default(5000),
  NOTIFYX_RETRY_MAX_DELAY_MS: z
    .coerce
    .number()
    .int()
    .positive()
    .default(900000),
  NOTIFYX_RETRY_JITTER_RATIO: z
    .coerce
    .number()
    .min(0)
    .max(1)
    .default(0.5),
  PROMETHEUS_PORT: z
    .coerce
    .number()
    .default(9090),
  GRAFANA_PORT: z
    .coerce
    .number()
    .default(3008),
  GRAFANA_ADMIN_USER: z
    .string()
    .default('admin'),
  GRAFANA_ADMIN_PASSWORD: z
    .preprocess((val) => (val === '' ? undefined : val), z.string().optional()),
  // Phase 11: Worker Concurrency Bounds
  EMAIL_WORKER_CONCURRENCY: z
    .coerce
    .number()
    .int()
    .positive()
    .default(5),
  PUSH_WORKER_CONCURRENCY: z
    .coerce
    .number()
    .int()
    .positive()
    .default(5),
  SMS_WORKER_CONCURRENCY: z
    .coerce
    .number()
    .int()
    .positive()
    .default(5),
  INAPP_WORKER_CONCURRENCY: z
    .coerce
    .number()
    .int()
    .positive()
    .default(10),
  // Phase 11: Provider Throttling & Mock Simulation
  MOCK_EMAIL_FAILURE_RATE: z
    .coerce
    .number()
    .min(0)
    .max(1)
    .default(0),
  MOCK_EMAIL_LATENCY_MS: z
    .coerce
    .number()
    .min(0)
    .default(0),
  MOCK_EMAIL_RATE_LIMIT: z
    .coerce
    .boolean()
    .default(false),
  MOCK_PUSH_FAILURE_RATE: z
    .coerce
    .number()
    .min(0)
    .max(1)
    .default(0),
  MOCK_PUSH_LATENCY_MS: z
    .coerce
    .number()
    .min(0)
    .default(0),
  MOCK_PUSH_RATE_LIMIT: z
    .coerce
    .boolean()
    .default(false),
  MOCK_SMS_FAILURE_RATE: z
    .coerce
    .number()
    .min(0)
    .max(1)
    .default(0),
  MOCK_SMS_LATENCY_MS: z
    .coerce
    .number()
    .min(0)
    .default(0),
  MOCK_SMS_RATE_LIMIT: z
    .coerce
    .boolean()
    .default(false),
  // Phase 11: Redis Failure Mode
  RATE_LIMIT_FAIL_CLOSED: z
    .coerce
    .boolean()
    .default(true),
}).superRefine((data, ctx) => {
  // Production-only strict validations (fail fast on localhost URLs or mock providers)
  if (data.NODE_ENV === 'production') {
    if (data.EMAIL_PROVIDER === 'mock') {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['EMAIL_PROVIDER'],
        message: 'EMAIL_PROVIDER cannot be "mock" in production. Must be "ses".',
      });
    }
    if (data.PUSH_PROVIDER === 'mock') {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['PUSH_PROVIDER'],
        message: 'PUSH_PROVIDER cannot be "mock" in production. Must be "fcm".',
      });
    }
    if (data.SMS_PROVIDER === 'mock') {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['SMS_PROVIDER'],
        message: 'SMS_PROVIDER cannot be "mock" in production. Must be "twilio".',
      });
    }

    if (data.DATABASE_URL.includes('localhost') || data.DATABASE_URL.includes('127.0.0.1')) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['DATABASE_URL'],
        message: 'DATABASE_URL cannot point to localhost/127.0.0.1 in production.',
      });
    }
    if (data.REDIS_URL.includes('localhost') || data.REDIS_URL.includes('127.0.0.1')) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['REDIS_URL'],
        message: 'REDIS_URL cannot point to localhost/127.0.0.1 in production.',
      });
    }
    if (data.KAFKA_BROKERS.includes('localhost') || data.KAFKA_BROKERS.includes('127.0.0.1')) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['KAFKA_BROKERS'],
        message: 'KAFKA_BROKERS cannot point to localhost/127.0.0.1 in production.',
      });
    }
    if (data.NEXT_PUBLIC_API_URL.includes('localhost') || data.NEXT_PUBLIC_API_URL.includes('127.0.0.1')) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['NEXT_PUBLIC_API_URL'],
        message: 'NEXT_PUBLIC_API_URL cannot point to localhost/127.0.0.1 in production.',
      });
    }
  }

  if (data.EMAIL_PROVIDER === 'ses') {
    if (!data.AWS_REGION || data.AWS_REGION.trim().length === 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['AWS_REGION'],
        message: 'AWS_REGION is required when EMAIL_PROVIDER=ses',
      });
    }
    if (!data.EMAIL_FROM_ADDRESS || data.EMAIL_FROM_ADDRESS.trim().length === 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['EMAIL_FROM_ADDRESS'],
        message: 'EMAIL_FROM_ADDRESS is required when EMAIL_PROVIDER=ses',
      });
    }
  }

  if (data.PUSH_PROVIDER === 'fcm') {
    if (!data.FIREBASE_PROJECT_ID || data.FIREBASE_PROJECT_ID.trim().length === 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['FIREBASE_PROJECT_ID'],
        message: 'FIREBASE_PROJECT_ID is required when PUSH_PROVIDER=fcm',
      });
    }
    if (!data.FIREBASE_CLIENT_EMAIL || data.FIREBASE_CLIENT_EMAIL.trim().length === 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['FIREBASE_CLIENT_EMAIL'],
        message: 'FIREBASE_CLIENT_EMAIL is required when PUSH_PROVIDER=fcm',
      });
    }
    if (!data.FIREBASE_PRIVATE_KEY || data.FIREBASE_PRIVATE_KEY.trim().length === 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['FIREBASE_PRIVATE_KEY'],
        message: 'FIREBASE_PRIVATE_KEY is required when PUSH_PROVIDER=fcm',
      });
    }
  }

  if (data.SMS_PROVIDER === 'twilio') {
    if (!data.TWILIO_ACCOUNT_SID || data.TWILIO_ACCOUNT_SID.trim().length === 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['TWILIO_ACCOUNT_SID'],
        message: 'TWILIO_ACCOUNT_SID is required when SMS_PROVIDER=twilio',
      });
    }
    if (!data.TWILIO_AUTH_TOKEN || data.TWILIO_AUTH_TOKEN.trim().length === 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['TWILIO_AUTH_TOKEN'],
        message: 'TWILIO_AUTH_TOKEN is required when SMS_PROVIDER=twilio',
      });
    }
    if (!data.TWILIO_FROM_NUMBER || data.TWILIO_FROM_NUMBER.trim().length === 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['TWILIO_FROM_NUMBER'],
        message: 'TWILIO_FROM_NUMBER is required when SMS_PROVIDER=twilio',
      });
    }
  }
});

export type EnvConfig = z.infer<typeof envSchema>;

let _config: EnvConfig | null = null;

export function loadConfig(customEnv?: Record<string, unknown>): EnvConfig {
  const source = customEnv ?? process.env;
  const result = envSchema.safeParse(source);

  if (!result.success) {
    const errorMessages = result.error.errors
      .map((err) => `  - ${err.path.join('.')}: ${err.message}`)
      .join('\n');

    throw new Error(
      `\n❌ Invalid environment configuration:\n${errorMessages}\n`
    );
  }

  _config = result.data;
  return _config;
}

export function getConfig(): EnvConfig {
  if (!_config) {
    return loadConfig();
  }
  return _config;
}
