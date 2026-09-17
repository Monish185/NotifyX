import pino, { type Logger as PinoLogger, type LoggerOptions } from 'pino';

export type Logger = PinoLogger;

export interface LogContext {
  service?: string;
  correlationId?: string;
  requestId?: string;
  tenantId?: string;
  notificationId?: string;
  deliveryId?: string;
  eventId?: string;
  attempt?: number;
  channel?: string;
  provider?: string;
  errorCode?: string;
  durationMs?: number;
  [key: string]: unknown;
}

export const DEFAULT_REDACT_PATHS = [
  'apiKey',
  '*.apiKey',
  'password',
  '*.password',
  'secret',
  '*.secret',
  'authorization',
  '*.authorization',
  'headers.authorization',
  'headers["x-api-key"]',
  'privateKey',
  '*.privateKey',
  'FIREBASE_PRIVATE_KEY',
  'GRAFANA_ADMIN_PASSWORD',
  'token',
  '*.token',
  'deviceToken',
  'fcmToken',
  'phone',
  '*.phone',
  'recipient',
  '*.recipient',
];

export function createLogger(
  nameOrOptions?: string | LoggerOptions,
  options?: LoggerOptions,
  destination?: pino.DestinationStream
): Logger {
  const isDev = process.env.NODE_ENV !== 'production' && process.env.NODE_ENV !== 'test';

  const resolvedName = typeof nameOrOptions === 'string' ? nameOrOptions : nameOrOptions?.name || 'notifyx';
  const resolvedOptions: LoggerOptions =
    typeof nameOrOptions === 'object' && nameOrOptions !== null
      ? { ...nameOrOptions, ...options }
      : options || {};
  const resolvedDestination =
    destination || (typeof nameOrOptions === 'object' ? (nameOrOptions as any)?.destination : undefined);

  const redactConfig = resolvedOptions.redact
    ? resolvedOptions.redact
    : {
        paths: DEFAULT_REDACT_PATHS,
        censor: '[REDACTED]',
      };

  const pinoOptions: LoggerOptions = {
    name: resolvedName,
    level: process.env.LOG_LEVEL || (isDev ? 'debug' : 'info'),
    redact: redactConfig,
    transport:
      isDev && !resolvedDestination
        ? {
            target: 'pino-pretty',
            options: {
              colorize: true,
              translateTime: 'SYS:standard',
              ignore: 'pid,hostname',
            },
          }
        : undefined,
    ...resolvedOptions,
  };

  return resolvedDestination ? pino(pinoOptions, resolvedDestination) : pino(pinoOptions);
}

export const logger = createLogger('notifyx-core');

/**
 * Creates a bound child logger tagged with a specific service name and initial context.
 */
export function createServiceLogger(serviceName: string, initialContext?: LogContext): Logger {
  return logger.child({ service: serviceName, ...initialContext });
}
