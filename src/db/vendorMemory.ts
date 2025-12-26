// src/db/vendorMemory.ts
import type Database from "better-sqlite3";
import { logAuditEvent } from "./auditEvents.js";

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

function nowIso() {
  return new Date().toISOString();
}

export function getVendorMemories(db: Database, vendor: string): VendorMemory[] {
  // if you might not always have disabledAt column, simplest is just filter on BOTH
  return db
    .prepare(`
      SELECT *
      FROM vendor_memory
      WHERE vendor = ?
        AND status = 'active'
        AND (disabledAt IS NULL OR disabledAt = '')
    `)
    .all(vendor) as VendorMemory[];
}

export function upsertVendorMemory(db: Database, mem: VendorMemory) {
  db.prepare(`
    INSERT INTO vendor_memory (id, vendor, kind, pattern, confidence, supportCount, rejectCount, lastUsedAt, status)
    VALUES (@id, @vendor, @kind, @pattern, @confidence, @supportCount, @rejectCount, @lastUsedAt, @status)
    ON CONFLICT(id) DO UPDATE SET
      vendor=excluded.vendor,
      kind=excluded.kind,
      pattern=excluded.pattern,
      confidence=excluded.confidence,
      supportCount=excluded.supportCount,
      rejectCount=excluded.rejectCount,
      lastUsedAt=excluded.lastUsedAt,
      status=excluded.status
  `).run(mem);
}

export function penalizeVendorMemory(db: Database, id: string, amount = 0.15) {
  const row = db
    .prepare(`SELECT id, confidence, rejectCount, vendor, status FROM vendor_memory WHERE id = ?`)
    .get(id) as
    | { id: string; confidence: number; rejectCount: number; vendor: string; status: string }
    | undefined;

  if (!row) return;

  const nextConfidence = Math.max(0, Math.min(1, row.confidence - amount));
  const nextReject = row.rejectCount + 1;

  db.prepare(`
    UPDATE vendor_memory
    SET confidence = ?, rejectCount = ?, lastUsedAt = ?
    WHERE id = ?
  `).run(nextConfidence, nextReject, nowIso(), id);

  logAuditEvent(db, {
    eventType: "LEARN_REJECTED",
    vendor: row.vendor,
    entityType: "vendor_memory",
    entityId: row.id,
    meta: { amount, nextConfidence, nextReject },
  });

  if (nextReject >= 2 && row.status !== "disabled") {
    db.prepare(`UPDATE vendor_memory SET status = ? WHERE id = ?`).run("disabled", id);
    logAuditEvent(db, {
      eventType: "MEMORY_DISABLED",
      vendor: row.vendor,
      entityType: "vendor_memory",
      entityId: row.id,
      meta: { reason: "rejectCount>=2" },
    });
  }
}
export function incrementVendorMemoryReject(db: Database, id: string, nowIso: string) {
  db.prepare(`
    UPDATE vendor_memory
    SET rejectCount = COALESCE(rejectCount, 0) + 1,
        lastUsedAt = COALESCE(lastUsedAt, @nowIso)
    WHERE id = @id
  `).run({ id, nowIso });
}

export function disableVendorMemory(db: Database, id: string, nowIso: string) {
  db.prepare(`
    UPDATE vendor_memory
    SET status = 'disabled',
        lastUsedAt = COALESCE(lastUsedAt, @nowIso)
    WHERE id = @id
  `).run({ id, nowIso });
}
