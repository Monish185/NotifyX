import { describe, it, expect } from 'vitest';
import { NotificationScheduler, schedulerService } from '../src/scheduler.js';

describe('NotificationScheduler', () => {
  it('instantiates with custom options', () => {
    const customScheduler = new NotificationScheduler({
      pollIntervalMs: 2500,
      batchSize: 100,
    });
    expect(customScheduler).toBeInstanceOf(NotificationScheduler);
  });

  it('exports authoritative schedulerService instance', () => {
    expect(schedulerService).toBeDefined();
    expect(typeof schedulerService.start).toBe('function');
    expect(typeof schedulerService.stop).toBe('function');
    expect(typeof schedulerService.pollOnce).toBe('function');
    expect(typeof schedulerService.pollAndProcessDueJobs).toBe('function');
    expect(typeof schedulerService.healthCheck).toBe('function');
  });
});
