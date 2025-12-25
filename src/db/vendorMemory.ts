import type Database from "better-sqlite3";

export type VendorMemory = {
  id: string;
  vendor: string;
  kind: string;
  pattern: string;
  confidence: number;
  supportCount: number;
  rejectCount: number;
  lastUsedAt: string | null;
  status: "active" | "disabled" | "suspect";
};

export function getVendorMemories(db: Database.Database, vendor: string): VendorMemory[] {
  return db
    .prepare(`SELECT * FROM vendor_memory WHERE vendor = ?`)
    .all(vendor) as VendorMemory[];
}

export function upsertVendorMemory(db: Database.Database, mem: VendorMemory) {
  db.prepare(`
    INSERT INTO vendor_memory (id, vendor, kind, pattern, confidence, supportCount, rejectCount, lastUsedAt, status)
    VALUES (@id, @vendor, @kind, @pattern, @confidence, @supportCount, @rejectCount, @lastUsedAt, @status)
    ON CONFLICT(id) DO UPDATE SET
      confidence=excluded.confidence,
      supportCount=excluded.supportCount,
      rejectCount=excluded.rejectCount,
      lastUsedAt=excluded.lastUsedAt,
      status=excluded.status
  `).run(mem);
}
