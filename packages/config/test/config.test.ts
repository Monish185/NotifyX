import { describe, it, expect } from 'vitest';
import { loadConfig } from '../src/index.js';

describe('packages/config', () => {
  it('should validate valid environment configuration', () => {
    const config = loadConfig({
      NODE_ENV: 'test',
      API_PORT: '3001',
      DATABASE_URL: 'postgresql://postgres:postgres@localhost:5432/notifyx',
      REDIS_URL: 'redis://localhost:6379',
      NEXT_PUBLIC_API_URL: 'http://localhost:3001',
    });

    expect(config.NODE_ENV).toBe('test');
    expect(config.API_PORT).toBe(3001);
    expect(config.DATABASE_URL).toBe('postgresql://postgres:postgres@localhost:5432/notifyx');
  });

  it('should accept empty strings for optional variables and treat them as undefined', () => {
    const config = loadConfig({
      NODE_ENV: 'test',
      DATABASE_URL: 'postgresql://postgres:postgres@localhost:5432/notifyx',
      EMAIL_PROVIDER: 'mock',
      PUSH_PROVIDER: 'mock',
      SMS_PROVIDER: 'mock',
      AWS_REGION: '',
      EMAIL_FROM_ADDRESS: '',
      VERIFY_EMAIL_RECIPIENT: '',
      FIREBASE_PROJECT_ID: '',
      FIREBASE_CLIENT_EMAIL: '',
      FIREBASE_PRIVATE_KEY: '',
      TWILIO_ACCOUNT_SID: '',
      TWILIO_AUTH_TOKEN: '',
      TWILIO_FROM_NUMBER: '',
    });

    expect(config.EMAIL_PROVIDER).toBe('mock');
    expect(config.PUSH_PROVIDER).toBe('mock');
    expect(config.SMS_PROVIDER).toBe('mock');
    expect(config.AWS_REGION).toBeUndefined();
    expect(config.EMAIL_FROM_ADDRESS).toBeUndefined();
    expect(config.VERIFY_EMAIL_RECIPIENT).toBeUndefined();
    expect(config.FIREBASE_PROJECT_ID).toBeUndefined();
    expect(config.FIREBASE_CLIENT_EMAIL).toBeUndefined();
    expect(config.FIREBASE_PRIVATE_KEY).toBeUndefined();
    expect(config.TWILIO_ACCOUNT_SID).toBeUndefined();
    expect(config.TWILIO_AUTH_TOKEN).toBeUndefined();
    expect(config.TWILIO_FROM_NUMBER).toBeUndefined();
  });

  it('should require AWS_REGION and EMAIL_FROM_ADDRESS when EMAIL_PROVIDER=ses', () => {
    expect(() => {
      loadConfig({
        NODE_ENV: 'test',
        DATABASE_URL: 'postgresql://postgres:postgres@localhost:5432/notifyx',
        EMAIL_PROVIDER: 'ses',
        AWS_REGION: '',
        EMAIL_FROM_ADDRESS: '',
      });
    }).toThrow(/AWS_REGION is required when EMAIL_PROVIDER=ses/);
  });

  it('should require FIREBASE credentials when PUSH_PROVIDER=fcm', () => {
    expect(() => {
      loadConfig({
        NODE_ENV: 'test',
        DATABASE_URL: 'postgresql://postgres:postgres@localhost:5432/notifyx',
        PUSH_PROVIDER: 'fcm',
        FIREBASE_PROJECT_ID: '',
        FIREBASE_CLIENT_EMAIL: '',
        FIREBASE_PRIVATE_KEY: '',
      });
    }).toThrow(/FIREBASE_PROJECT_ID is required when PUSH_PROVIDER=fcm/);

    const validFcmConfig = loadConfig({
      NODE_ENV: 'test',
      DATABASE_URL: 'postgresql://postgres:postgres@localhost:5432/notifyx',
      PUSH_PROVIDER: 'fcm',
      FIREBASE_PROJECT_ID: 'test-project',
      FIREBASE_CLIENT_EMAIL: 'test@test-project.iam.gserviceaccount.com',
      FIREBASE_PRIVATE_KEY: '-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----',
    });
    expect(validFcmConfig.PUSH_PROVIDER).toBe('fcm');
    expect(validFcmConfig.FIREBASE_PROJECT_ID).toBe('test-project');
  });

  it('should require TWILIO credentials when SMS_PROVIDER=twilio', () => {
    expect(() => {
      loadConfig({
        NODE_ENV: 'test',
        DATABASE_URL: 'postgresql://postgres:postgres@localhost:5432/notifyx',
        SMS_PROVIDER: 'twilio',
        TWILIO_ACCOUNT_SID: '',
        TWILIO_AUTH_TOKEN: '',
        TWILIO_FROM_NUMBER: '',
      });
    }).toThrow(/TWILIO_ACCOUNT_SID is required when SMS_PROVIDER=twilio/);

    const validTwilioConfig = loadConfig({
      NODE_ENV: 'test',
      DATABASE_URL: 'postgresql://postgres:postgres@localhost:5432/notifyx',
      SMS_PROVIDER: 'twilio',
      TWILIO_ACCOUNT_SID: 'AC1234567890abcdef',
      TWILIO_AUTH_TOKEN: 'auth_token_secret',
      TWILIO_FROM_NUMBER: '+15551234567',
    });
    expect(validTwilioConfig.SMS_PROVIDER).toBe('twilio');
    expect(validTwilioConfig.TWILIO_ACCOUNT_SID).toBe('AC1234567890abcdef');
    expect(validTwilioConfig.TWILIO_FROM_NUMBER).toBe('+15551234567');
  });

  it('should reject mock providers in production environment', () => {
    expect(() => {
      loadConfig({
        NODE_ENV: 'production',
        DATABASE_URL: 'postgresql://postgres:postgres@db.prod.notifyx.internal:5432/notifyx',
        REDIS_URL: 'redis://redis.prod.notifyx.internal:6379',
        KAFKA_BROKERS: 'kafka1.prod.notifyx.internal:9092',
        NEXT_PUBLIC_API_URL: 'https://api.notifyx.example.com',
        EMAIL_PROVIDER: 'mock',
        PUSH_PROVIDER: 'mock',
        SMS_PROVIDER: 'mock',
      });
    }).toThrow(/EMAIL_PROVIDER cannot be "mock" in production/);
  });

  it('should reject localhost URLs in production environment', () => {
    expect(() => {
      loadConfig({
        NODE_ENV: 'production',
        DATABASE_URL: 'postgresql://postgres:postgres@localhost:5432/notifyx',
        REDIS_URL: 'redis://redis.prod.notifyx.internal:6379',
        KAFKA_BROKERS: 'kafka1.prod.notifyx.internal:9092',
        NEXT_PUBLIC_API_URL: 'https://api.notifyx.example.com',
        EMAIL_PROVIDER: 'ses',
        PUSH_PROVIDER: 'fcm',
        SMS_PROVIDER: 'twilio',
        AWS_REGION: 'us-east-1',
        EMAIL_FROM_ADDRESS: 'no-reply@notifyx.example.com',
        FIREBASE_PROJECT_ID: 'proj',
        FIREBASE_CLIENT_EMAIL: 'client@proj.iam.gserviceaccount.com',
        FIREBASE_PRIVATE_KEY: 'pkey',
        TWILIO_ACCOUNT_SID: 'AC123',
        TWILIO_AUTH_TOKEN: 'token',
        TWILIO_FROM_NUMBER: '+15551234',
      });
    }).toThrow(/DATABASE_URL cannot point to localhost/);
  });

  it('should accept valid production configuration with remote services and real providers', () => {
    const prodConfig = loadConfig({
      NODE_ENV: 'production',
      DATABASE_URL: 'postgresql://postgres:postgres@rds.prod.notifyx.internal:5432/notifyx',
      REDIS_URL: 'redis://elasticache.prod.notifyx.internal:6379',
      KAFKA_BROKERS: 'msk.prod.notifyx.internal:9092',
      NEXT_PUBLIC_API_URL: 'https://api.notifyx.example.com',
      EMAIL_PROVIDER: 'ses',
      PUSH_PROVIDER: 'fcm',
      SMS_PROVIDER: 'twilio',
      AWS_REGION: 'us-east-1',
      EMAIL_FROM_ADDRESS: 'no-reply@notifyx.example.com',
      FIREBASE_PROJECT_ID: 'prod-proj',
      FIREBASE_CLIENT_EMAIL: 'client@prod-proj.iam.gserviceaccount.com',
      FIREBASE_PRIVATE_KEY: '-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----',
      TWILIO_ACCOUNT_SID: 'AC1234567890abcdef',
      TWILIO_AUTH_TOKEN: 'auth_token_secret',
      TWILIO_FROM_NUMBER: '+15551234567',
    });

    expect(prodConfig.NODE_ENV).toBe('production');
    expect(prodConfig.EMAIL_PROVIDER).toBe('ses');
    expect(prodConfig.PUSH_PROVIDER).toBe('fcm');
    expect(prodConfig.SMS_PROVIDER).toBe('twilio');
  });
});

