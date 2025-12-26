// src/admin/commands.ts
import type Database from "better-sqlite3";
import { logAuditEvent } from "../db/auditEvents.js";

function nowIso() {
  return new Date().toISOString();
}

export function disableVendorMemory(db: Database, id: number) {
  db.prepare(`UPDATE vendor_memory SET disabledAt = ? WHERE id = ?`).run(nowIso(), id);
  logAuditEvent(db, {
    eventType: "ADMIN_ACTION",
    entityType: "vendor_memory",
    entityId: String(id),
    meta: { action: "disable" },
  });
}

export function resetVendorMemoryConfidence(db: Database, id: number, to: number) {
  db.prepare(`UPDATE vendor_memory SET confidence = ?, rejectCount = 0 WHERE id = ?`).run(to, id);
  logAuditEvent(db, {
    eventType: "MEMORY_CONFIDENCE_RESET",
    entityType: "vendor_memory",
    entityId: String(id),
    meta: { action: "reset-confidence", to },
  });
}
