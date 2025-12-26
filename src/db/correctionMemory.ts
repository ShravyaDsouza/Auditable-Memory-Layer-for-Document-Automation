// src/db/correctionMemory.ts
import type Database from "better-sqlite3";

export type CorrectionMemoryRow = {
  id: string;
  vendor: string;
  fieldPath: string;
  patternType: string;
  patternValue: string;
  recommendedValue: string;
  confidence: number;
  supportCount: number;
  rejectCount: number;
  lastUsedAt: string | null;
  createdAt: string;

  // optional — only if your schema has it
  status?: "active" | "disabled";
};

let _hasStatusColumn: boolean | null = null;

function hasStatusColumn(db: Database): boolean {
  if (_hasStatusColumn != null) return _hasStatusColumn;

  const cols = db
    .prepare(`PRAGMA table_info(correction_memory)`)
    .all() as Array<{ name: string }>;

  _hasStatusColumn = cols.some((c) => c.name === "status");
  return _hasStatusColumn;
}

export function getCorrectionMemories(db: Database, vendor: string): CorrectionMemoryRow[] {
  const useStatus = hasStatusColumn(db);

  const sql = useStatus
    ? `
      SELECT *
      FROM correction_memory
      WHERE vendor = ?
        AND status = 'active'
    `
    : `
      SELECT *
      FROM correction_memory
      WHERE vendor = ?
    `;

  return db.prepare(sql).all(vendor) as CorrectionMemoryRow[];
}

export function findCorrectionMemory(
  db: Database,
  vendor: string,
  fieldPath: string,
  patternType: string,
  patternValue: string
): CorrectionMemoryRow | undefined {
  const useStatus = hasStatusColumn(db);

  const sql = useStatus
    ? `
      SELECT *
      FROM correction_memory
      WHERE vendor = ?
        AND fieldPath = ?
        AND patternType = ?
        AND patternValue = ?
        AND status = 'active'
      LIMIT 1
    `
    : `
      SELECT *
      FROM correction_memory
      WHERE vendor = ?
        AND fieldPath = ?
        AND patternType = ?
        AND patternValue = ?
      LIMIT 1
    `;

  return db.prepare(sql).get(vendor, fieldPath, patternType, patternValue) as
    | CorrectionMemoryRow
    | undefined;
}

export function upsertCorrectionMemory(
  db: Database,
  row: Omit<CorrectionMemoryRow, "supportCount" | "rejectCount">
) {
  const useStatus = hasStatusColumn(db);

  const existing = db
    .prepare(`SELECT * FROM correction_memory WHERE id = ?`)
    .get(row.id) as CorrectionMemoryRow | undefined;

  if (existing) {
    const nextConfidence = Math.min(0.95, (existing.confidence ?? row.confidence ?? 0.7) + 0.1);

    db.prepare(
      `
      UPDATE correction_memory
      SET confidence = ?,
          supportCount = COALESCE(supportCount, 0) + 1,
          lastUsedAt = ?
      WHERE id = ?
      `
    ).run(nextConfidence, row.lastUsedAt, row.id);

    return { id: row.id, confidence: nextConfidence };
  }

  if (useStatus) {
    db.prepare(
      `
      INSERT INTO correction_memory (
        id, vendor, fieldPath, patternType, patternValue,
        recommendedValue, confidence, supportCount, rejectCount,
        lastUsedAt, createdAt, status
      )
      VALUES (
        @id, @vendor, @fieldPath, @patternType, @patternValue,
        @recommendedValue, @confidence, 1, 0,
        @lastUsedAt, @createdAt, 'active'
      )
      `
    ).run(row);

    return { id: row.id, confidence: row.confidence };
  }

  // schema WITHOUT status
  db.prepare(
    `
    INSERT INTO correction_memory (
      id, vendor, fieldPath, patternType, patternValue,
      recommendedValue, confidence, supportCount, rejectCount,
      lastUsedAt, createdAt
    )
    VALUES (
      @id, @vendor, @fieldPath, @patternType, @patternValue,
      @recommendedValue, @confidence, 1, 0,
      @lastUsedAt, @createdAt
    )
    `
  ).run(row);

  return { id: row.id, confidence: row.confidence };
}

export function markUsed(db: Database, id: string) {
  db.prepare(
    `
    UPDATE correction_memory
    SET lastUsedAt = ?
    WHERE id = ?
    `
  ).run(new Date().toISOString(), id);
}

export function markRejected(db: Database, id: string) {
  const useStatus = hasStatusColumn(db);

  if (useStatus) {
    db.prepare(
      `
      UPDATE correction_memory
      SET rejectCount = COALESCE(rejectCount, 0) + 1,
          status = CASE
            WHEN COALESCE(rejectCount, 0) + 1 >= 2 THEN 'disabled'
            ELSE status
          END,
          lastUsedAt = ?
      WHERE id = ?
      `
    ).run(new Date().toISOString(), id);

    return;
  }

  // schema WITHOUT status column → still track rejectCount
  db.prepare(
    `
    UPDATE correction_memory
    SET rejectCount = COALESCE(rejectCount, 0) + 1,
        lastUsedAt = ?
    WHERE id = ?
    `
  ).run(new Date().toISOString(), id);
}

export function getCorrectionMemoryById(
  db: Database,
  id: string
): CorrectionMemoryRow | undefined {
  return db.prepare(`SELECT * FROM correction_memory WHERE id = ?`).get(id) as
    | CorrectionMemoryRow
    | undefined;
}
