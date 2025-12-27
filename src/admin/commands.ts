import type Database from "better-sqlite3";
import { logAuditEvent } from "../db/auditEvents.js";

function nowIso() {
  return new Date().toISOString();
}

export function disableVendorMemory(db: Database, id: string) {
  const res = db
    .prepare(`UPDATE vendor_memory SET status='disabled', disabledAt=? WHERE id=?`)
    .run(nowIso(), id);

  logAuditEvent(db, {
    eventType: "ADMIN_ACTION",
    entityType: "vendor_memory",
    entityId: id,
    meta: { action: "disable", changes: res.changes },
  });
}

export function resetVendorMemoryConfidence(db: Database, id: string, to: number) {
  const res = db
    .prepare(`UPDATE vendor_memory SET confidence=?, rejectCount=0 WHERE id=?`)
    .run(to, id);

  logAuditEvent(db, {
    eventType: "MEMORY_CONFIDENCE_RESET",
    entityType: "vendor_memory",
    entityId: id,
    meta: { action: "reset-confidence", to, changes: res.changes },
  });
}