export interface CalculateRetryDelayOptions {
  attempt: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  jitterRatio?: number;
  random?: () => number;
}

export const DEFAULT_RETRY_CONFIG = {
  maxAttempts: 5,
  baseDelayMs: 5000,
  maxDelayMs: 900000, // 15 minutes
  jitterRatio: 0.5,
} as const;

/**
 * Calculates exponential backoff delay with bounded jitter.
 *
 * Formula:
 *   delay = baseDelayMs * 2^(attempt - 1)
 *   jitterFactor = (1 - jitterRatio) + random() * (2 * jitterRatio)
 *   finalDelay = clamp(delay * jitterFactor, 0, maxDelayMs)
 *
 * Deterministic testing:
 * Pass a custom `random` function, e.g. `() => 0.5` for jitterFactor = 1.0.
 */
export function calculateRetryDelay(options: CalculateRetryDelayOptions): number {
  const baseDelayMs = options.baseDelayMs ?? DEFAULT_RETRY_CONFIG.baseDelayMs;
  const maxDelayMs = options.maxDelayMs ?? DEFAULT_RETRY_CONFIG.maxDelayMs;
  const jitterRatio = options.jitterRatio ?? DEFAULT_RETRY_CONFIG.jitterRatio;
  const randomFn = options.random ?? Math.random;

  // Protect against negative or non-integer attempt numbers
  const safeAttempt = Math.max(1, Math.floor(options.attempt || 1));

  // Protect against 2^N integer overflow (clamp exponent at 30)
  const exponent = Math.min(safeAttempt - 1, 30);
  const exponentialDelay = baseDelayMs * Math.pow(2, exponent);

  // Apply bounded jitter if configured
  let jitterFactor = 1.0;
  if (jitterRatio > 0) {
    const rawRandom = randomFn();
    const safeRandom = Math.max(0, Math.min(1, typeof rawRandom === 'number' && !isNaN(rawRandom) ? rawRandom : 0.5));
    // When safeRandom = 0: (1 - jitterRatio) -> e.g. 0.5
    // When safeRandom = 0.5: 1.0
    // When safeRandom = 1: (1 + jitterRatio) -> e.g. 1.5
    jitterFactor = (1 - jitterRatio) + safeRandom * (2 * jitterRatio);
  }

  const rawFinalDelay = Math.round(exponentialDelay * jitterFactor);

  // Strictly clamp between 0 and maxDelayMs
  return Math.min(maxDelayMs, Math.max(0, rawFinalDelay));
}
