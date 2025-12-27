import type Database from "better-sqlite3";

function nowIso(now?: Date) {
  return (now ?? new Date()).toISOString();
}

export type AuditEventType =
  | "DUPLICATE_DETECTED"
  | "LEARN_APPROVED"
  | "LEARN_REJECTED"
  | "MEMORY_DISABLED"
  | "MEMORY_CONFIDENCE_RESET"
  | "ADMIN_ACTION";

export function logAuditEvent(
  db: Database,
  args: {
    eventType: AuditEventType;
    vendor?: string | null;
    invoiceId?: string | null;
    entityType?: string | null;
    entityId?: string | null;
    meta?: unknown;
    now?: Date;
  }
) {
  const stmt = db.prepare(`
    INSERT INTO audit_events (ts, eventType, vendor, invoiceId, entityType, entityId, metaJson)
    VALUES (@ts, @eventType, @vendor, @invoiceId, @entityType, @entityId, @metaJson)
  `);

  stmt.run({
    ts: nowIso(args.now),
    eventType: args.eventType,
    vendor: args.vendor ?? null,
    invoiceId: args.invoiceId ?? null,
    entityType: args.entityType ?? null,
    entityId: args.entityId ?? null,
    metaJson: args.meta ? JSON.stringify(args.meta) : null,
  });
}
