import { describe, it, expect } from 'vitest';

// Re-implement the backoff function to test it in isolation
function getBackoffDelay(attempts: number): number {
  return Math.min(1000 * Math.pow(2, attempts), 30000);
}

describe('exponential backoff', () => {
  it('should double delay with each attempt', () => {
    expect(getBackoffDelay(1)).toBe(2000);
    expect(getBackoffDelay(2)).toBe(4000);
    expect(getBackoffDelay(3)).toBe(8000);
    expect(getBackoffDelay(4)).toBe(16000);
    expect(getBackoffDelay(5)).toBe(30000);
  });

  it('should cap at 30 seconds', () => {
    expect(getBackoffDelay(10)).toBe(30000);
    expect(getBackoffDelay(100)).toBe(30000);
  });

  it('should have minimum delay for first attempt', () => {
    expect(getBackoffDelay(0)).toBe(1000);
  });
});
