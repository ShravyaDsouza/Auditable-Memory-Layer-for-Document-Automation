export function daysSince(iso: string | null | undefined, now: Date): number {
  if (!iso) return Number.POSITIVE_INFINITY; // never used -> very old
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return Number.POSITIVE_INFINITY;
  const diffMs = now.getTime() - t;
  if (diffMs <= 0) return 0;
  return diffMs / (1000 * 60 * 60 * 24);
}

export function decayedConfidence(base: number, days: number, halfLifeDays = 30): number {
  if (!Number.isFinite(base)) return 0;
  base = clamp01(base);

  if (!Number.isFinite(days) || days === Number.POSITIVE_INFINITY) return 0;

  if (days <= 0) return base;

  const hl = Math.max(1e-6, halfLifeDays);
  const lambda = Math.log(2) / hl;
  const factor = Math.exp(-lambda * days);
  return clamp01(base * factor);
}

export function clamp01(x: number) {
  return Math.max(0, Math.min(1, x));
}
