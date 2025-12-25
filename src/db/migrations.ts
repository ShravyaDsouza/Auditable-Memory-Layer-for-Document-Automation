import type Database from "better-sqlite3";

export function migrate(db: Database.Database) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS invoice_runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      invoiceId TEXT NOT NULL,
      vendor TEXT NOT NULL,
      dataset TEXT NOT NULL,
      createdAt TEXT NOT NULL
    );
  `);
}
