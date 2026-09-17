/**
 * Asynchronous Semaphore for bounding concurrent worker operations.
 *
 * Implements bounded concurrency control without creating unbounded task queues.
 */
export class Semaphore {
  private inFlight = 0;
  private readonly queue: Array<() => void> = [];

  constructor(public readonly maxConcurrency: number) {
    if (maxConcurrency <= 0) {
      throw new Error(`Semaphore maxConcurrency must be positive, got ${maxConcurrency}`);
    }
  }

  get activeCount(): number {
    return this.inFlight;
  }

  get queueLength(): number {
    return this.queue.length;
  }

  /**
   * Acquires a permit. Returns a release function that must be called when work completes.
   */
  async acquire(): Promise<() => void> {
    if (this.inFlight < this.maxConcurrency) {
      this.inFlight++;
      let released = false;
      return () => {
        if (!released) {
          released = true;
          this.release();
        }
      };
    }

    return new Promise<() => void>((resolve) => {
      this.queue.push(() => {
        this.inFlight++;
        let released = false;
        resolve(() => {
          if (!released) {
            released = true;
            this.release();
          }
        });
      });
    });
  }

  private release(): void {
    this.inFlight--;
    if (this.queue.length > 0 && this.inFlight < this.maxConcurrency) {
      const next = this.queue.shift();
      if (next) {
        next();
      }
    }
  }
}
