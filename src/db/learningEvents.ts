import type Database from "better-sqlite3";

export function hasLearned(db: Database.Database, invoiceId: string): boolean {
  const row = db.prepare(`SELECT invoiceId FROM learning_events WHERE invoiceId = ?`).get(invoiceId);
  return !!row;
}

export function markLearned(db: Database.Database, invoiceId: string) {
  db.prepare(`INSERT OR IGNORE INTO learning_events (invoiceId, learnedAt) VALUES (?, ?)`)
    .run(invoiceId, new Date().toISOString());
}
