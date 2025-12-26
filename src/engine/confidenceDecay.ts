// src/engine/confidenceDecay.ts
export function daysSince(iso: string | null | undefined, now: Date): number {
  if (!iso) return Number.POSITIVE_INFINITY; // never used -> treat as very old
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return Number.POSITIVE_INFINITY;
  const diffMs = now.getTime() - t;
  return Math.max(0, diffMs / (1000 * 60 * 60 * 24));
}

/**
 * Simple exponential decay:
 * effective = base * exp(-lambda * days)
 *
 * Half-life interpretation:
 * - If halfLifeDays=30, confidence halves every 30 days without usage.
 */
export function decayedConfidence(base: number, days: number, halfLifeDays = 30): number {
  if (!Number.isFinite(base)) return 0;
  if (!Number.isFinite(days)) return Math.max(0, Math.min(1, base * 0.5)); // "very old" default decay
  if (days <= 0) return clamp01(base);

  const lambda = Math.log(2) / Math.max(1e-6, halfLifeDays);
  const factor = Math.exp(-lambda * days);
  return clamp01(base * factor);
}

export function clamp01(x: number) {
  return Math.max(0, Math.min(1, x));
}
