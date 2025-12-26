// src/db/resolutionMemory.ts
import type Database from "better-sqlite3";

export type ResolutionDecision = "approved" | "rejected";

export type ResolutionValueJson = {
  approved: number;
  rejected: number;
  lastDecision: ResolutionDecision | null;
  lastInvoiceId: string | null;
};

export type ResolutionMemoryRow = {
  id?: number; // sqlite AUTOINCREMENT
  vendor: string;
  key: string;
  valueJson: string; // stored JSON string
  confidence: number;
  rejectCount?: number;
  lastUsedAt: string | null;
  disabledAt?: string | null;
  createdAt: string;
};

let _cols: Set<string> | null = null;

function cols(db: Database.Database): Set<string> {
  if (_cols) return _cols;
  const rows = db.prepare(`PRAGMA table_info(resolution_memory)`).all() as Array<{ name: string }>;
  _cols = new Set(rows.map((r) => r.name));
  return _cols;
}

function hasCol(db: Database.Database, name: string) {
  return cols(db).has(name);
}

function nowIso(d?: Date) {
  return (d ?? new Date()).toISOString();
}

function defaultValue(): ResolutionValueJson {
  return { approved: 0, rejected: 0, lastDecision: null, lastInvoiceId: null };
}

function safeParseValueJson(v: unknown): ResolutionValueJson {
  try {
    const parsed = typeof v === "string" ? JSON.parse(v) : v;
    return {
      approved: Number((parsed as any)?.approved ?? 0),
      rejected: Number((parsed as any)?.rejected ?? 0),
      lastDecision: (((parsed as any)?.lastDecision ?? null) as ResolutionDecision | null),
      lastInvoiceId: (((parsed as any)?.lastInvoiceId ?? null) as string | null),
    };
  } catch {
    return defaultValue();
  }
}

/**
 * Fetch a resolution row (ignores disabled rows if disabledAt column exists).
 */
export function getResolution(
  db: Database.Database,
  vendor: string,
  key: string
): (ResolutionMemoryRow & { value: ResolutionValueJson }) | undefined {
  const whereDisabled = hasCol(db, "disabledAt") ? `AND (disabledAt IS NULL)` : ``;

  const row = db
    .prepare(
      `
      SELECT *
      FROM resolution_memory
      WHERE vendor = ?
        AND key = ?
        ${whereDisabled}
      LIMIT 1
      `
    )
    .get(vendor, key) as ResolutionMemoryRow | undefined;

  if (!row) return undefined;
  return { ...row, value: safeParseValueJson((row as any).valueJson) };
}

/**
 * Upsert a resolution row by (vendor,key).
 * NOTE: Table has UNIQUE(vendor,key), so we use ON CONFLICT(vendor,key).
 */
export function upsertResolution(
  db: Database.Database,
  vendor: string,
  key: string,
  patch: Partial<{
    value: ResolutionValueJson;
    confidence: number;
    lastUsedAt: string;
    disabledAt: string | null;
  }>
): { vendor: string; key: string; confidence: number; value: ResolutionValueJson } {
  const existing = getResolution(db, vendor, key);
  const nextValue: ResolutionValueJson = patch.value ?? existing?.value ?? defaultValue();
  const nextConfidence =
    typeof patch.confidence === "number" ? patch.confidence : existing?.confidence ?? 0.5;

  const nextLastUsedAt = patch.lastUsedAt ?? nowIso();
  const disabledAtCol = hasCol(db, "disabledAt");
  const rejectCountCol = hasCol(db, "rejectCount");

  // If patch explicitly sets disabledAt (or null), respect it; otherwise keep existing.
  const nextDisabledAt =
    disabledAtCol ? (patch.disabledAt !== undefined ? patch.disabledAt : (existing as any)?.disabledAt ?? null) : undefined;

  // Keep rejectCount stable unless you explicitly change it elsewhere
  const nextRejectCount =
    rejectCountCol ? Number((existing as any)?.rejectCount ?? 0) : undefined;

  // Build dynamic column sets
  const setCols: string[] = [
    `valueJson = excluded.valueJson`,
    `confidence = excluded.confidence`,
    `lastUsedAt = excluded.lastUsedAt`,
  ];

  if (disabledAtCol) setCols.push(`disabledAt = excluded.disabledAt`);
  if (rejectCountCol) setCols.push(`rejectCount = excluded.rejectCount`);

  const insertCols = ["vendor", "key", "valueJson", "confidence", "lastUsedAt", "createdAt"];
  const insertVals = ["@vendor", "@key", "@valueJson", "@confidence", "@lastUsedAt", "@createdAt"];

  if (disabledAtCol) {
    insertCols.push("disabledAt");
    insertVals.push("@disabledAt");
  }
  if (rejectCountCol) {
    insertCols.push("rejectCount");
    insertVals.push("@rejectCount");
  }

  db.prepare(
    `
    INSERT INTO resolution_memory (${insertCols.join(", ")})
    VALUES (${insertVals.join(", ")})
    ON CONFLICT(vendor, key) DO UPDATE SET
      ${setCols.join(", ")}
    `
  ).run({
    vendor,
    key,
    valueJson: JSON.stringify(nextValue),
    confidence: nextConfidence,
    lastUsedAt: nextLastUsedAt,
    createdAt: existing?.createdAt ?? nowIso(),
    ...(disabledAtCol ? { disabledAt: nextDisabledAt } : {}),
    ...(rejectCountCol ? { rejectCount: nextRejectCount } : {}),
  });

  return { vendor, key, confidence: nextConfidence, value: nextValue };
}

/**
 * Apply resolution history to a baseConfidence.
 * - boosts if mostly approved
 * - penalizes/caps if rejected frequently
 */
export function applyResolutionToConfidence(
  db: Database.Database,
  vendor: string,
  key: string,
  baseConfidence: number
): { confidence: number; note: string } {
  const row = getResolution(db, vendor, key);
  if (!row) return { confidence: baseConfidence, note: "No resolution history." };

  const a = Number(row.value.approved ?? 0);
  const r = Number(row.value.rejected ?? 0);
  const total = a + r;

  if (total === 0) return { confidence: baseConfidence, note: "No resolution history." };

  // Hard cap if repeatedly rejected and never approved
  if (r >= 2 && a === 0) {
    const capped = Math.min(baseConfidence, 0.6);
    return { confidence: capped, note: `Mostly rejected historically (approved=${a}, rejected=${r}) → capped.` };
  }

  const approvalRatio = a / total; // [0..1]
  // delta in approx [-0.1..+0.1]
  const delta = (approvalRatio - 0.5) * 0.2;
  const next = Math.max(0.2, Math.min(0.95, baseConfidence + delta));

  return { confidence: next, note: `Resolution: approved=${a}, rejected=${r}, ${baseConfidence}→${next}` };
}

/**
 * Record an APPROVED/REJECTED decision for a (vendor,key).
 * Also evolves row confidence and (optionally) disables strategy if repeatedly rejected.
 */
export function recordResolutionDecision(
  db: Database.Database,
  args: { vendor: string; key: string; decision: ResolutionDecision; invoiceId: string }
): { vendor: string; key: string; value: ResolutionValueJson; confidence: number } {
  const { vendor, key, decision, invoiceId } = args;

  // IMPORTANT: if row was disabled, treat it as non-existent for safety
  const existingAny = ((): (ResolutionMemoryRow & { value: ResolutionValueJson }) | undefined => {
    // Try to fetch even disabled rows if column exists (so we can update it)
    if (!hasCol(db, "disabledAt")) return getResolution(db, vendor, key);

    const row = db
      .prepare(
        `
        SELECT *
        FROM resolution_memory
        WHERE vendor = ?
          AND key = ?
        LIMIT 1
        `
      )
      .get(vendor, key) as ResolutionMemoryRow | undefined;

    if (!row) return undefined;
    return { ...row, value: safeParseValueJson((row as any).valueJson) };
  })();

  const value = existingAny?.value ?? defaultValue();

  if (decision === "approved") value.approved += 1;
  if (decision === "rejected") value.rejected += 1;

  value.lastDecision = decision;
  value.lastInvoiceId = invoiceId;

  // Simple confidence evolution for the resolution itself
  let confidence = existingAny?.confidence ?? 0.5;
  if (decision === "approved") confidence = Math.min(0.95, confidence + 0.05);
  if (decision === "rejected") confidence = Math.max(0.2, confidence - 0.1);

  const rejectCountCol = hasCol(db, "rejectCount");
  const disabledAtCol = hasCol(db, "disabledAt");

  // If rejected, increment rejectCount (if present)
  let nextRejectCount = rejectCountCol ? Number((existingAny as any)?.rejectCount ?? 0) : 0;
  if (decision === "rejected" && rejectCountCol) nextRejectCount += 1;

  // Disable if repeatedly rejected and never approved (optional but recommended)
  let disabledAt: string | null | undefined = undefined;
  if (disabledAtCol) {
    const a = value.approved;
    const r = value.rejected;
    if (decision === "rejected" && r >= 2 && a === 0) {
      disabledAt = nowIso();
      confidence = Math.min(confidence, 0.2);
    } else {
      // keep existing disabledAt as-is unless we disable now
      disabledAt = (existingAny as any)?.disabledAt ?? null;
    }
  }

  // Upsert base fields
  const out = upsertResolution(db, vendor, key, {
    value,
    confidence,
    lastUsedAt: nowIso(),
    ...(disabledAtCol ? { disabledAt } : {}),
  });

  // Persist rejectCount if column exists
  if (rejectCountCol) {
    db.prepare(
      `
      UPDATE resolution_memory
      SET rejectCount = ?
      WHERE vendor = ? AND key = ?
      `
    ).run(nextRejectCount, vendor, key);
  }

  return { vendor, key, value: out.value, confidence: out.confidence };
}

export function getAllResolutionsForVendor(db: Database, vendor: string) {
  return db
    .prepare(`SELECT * FROM resolution_memory WHERE vendor = ? ORDER BY key ASC`)
    .all(vendor) as any[];
}