import { describe, it, expect } from 'vitest';
import { Semaphore } from '../src/semaphore.js';

describe('Semaphore', () => {
  it('bounds concurrent executions to maxConcurrency', async () => {
    const semaphore = new Semaphore(3);
    let peakConcurrency = 0;
    let activeWorkers = 0;

    const runWorker = async (id: number) => {
      const release = await semaphore.acquire();
      activeWorkers++;
      if (activeWorkers > peakConcurrency) {
        peakConcurrency = activeWorkers;
      }

      // Simulate work
      await new Promise((resolve) => setTimeout(resolve, 50));

      activeWorkers--;
      release();
    };

    // Launch 10 concurrent workers
    await Promise.all(Array.from({ length: 10 }, (_, i) => runWorker(i)));

    expect(peakConcurrency).toBe(3);
    expect(semaphore.activeCount).toBe(0);
    expect(semaphore.queueLength).toBe(0);
  });

  it('rejects invalid non-positive concurrency bounds', () => {
    expect(() => new Semaphore(0)).toThrow();
    expect(() => new Semaphore(-1)).toThrow();
  });
});
