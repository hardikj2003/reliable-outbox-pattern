import { logger } from '../config/logger.js';

interface MetricEntry {
  name: string;
  value: number;
  unit: 'ms' | 'count' | 'bytes';
  labels?: Record<string, string>;
}

const metricsBuffer: MetricEntry[] = [];
const MAX_BUFFER_SIZE = 1000;

export const metrics = {
  /**
   * Record a timing metric. Automatically calculates duration.
   */
  timer(name: string, labels?: Record<string, string>) {
    const start = process.hrtime.bigint();
    return {
      stop(): void {
        const end = process.hrtime.bigint();
        const durationMs = Number(end - start) / 1_000_000;
        metrics.record(name, durationMs, 'ms', labels);
      },
    };
  },

  /**
   * Record a raw metric value.
   */
  record(
    name: string,
    value: number,
    unit: 'ms' | 'count' | 'bytes',
    labels?: Record<string, string>
  ): void {
    const entry: MetricEntry = { name, value, unit, labels };
    metricsBuffer.push(entry);

    if (metricsBuffer.length > MAX_BUFFER_SIZE) {
      metricsBuffer.shift();
    }

    // In production, you would flush to Prometheus/CloudWatch/StatsD.
    // For this portfolio project, we log structured metrics.
    logger.debug({ metric: entry }, 'Metric recorded');
  },

  /**
   * Increment a counter metric.
   */
  increment(name: string, labels?: Record<string, string>): void {
    metrics.record(name, 1, 'count', labels);
  },

  /**
   * Get all recorded metrics (useful for health endpoint or debugging).
   */
  getAll(): MetricEntry[] {
    return [...metricsBuffer];
  },

  /**
   * Get aggregated metrics summary.
   */
  getSummary(): Record<string, { count: number; avg: number; min: number; max: number }> {
    const groups: Record<string, number[]> = {};

    for (const entry of metricsBuffer) {
      const key = entry.labels
        ? `${entry.name}:${JSON.stringify(entry.labels)}`
        : entry.name;
      groups[key] = groups[key] || [];
      groups[key].push(entry.value);
    }

    const summary: Record<string, { count: number; avg: number; min: number; max: number }> = {};
    for (const [key, values] of Object.entries(groups)) {
      summary[key] = {
        count: values.length,
        avg: values.reduce((a, b) => a + b, 0) / values.length,
        min: Math.min(...values),
        max: Math.max(...values),
      };
    }

    return summary;
  },
};
