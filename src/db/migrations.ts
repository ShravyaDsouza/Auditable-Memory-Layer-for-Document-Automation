import type Database from "better-sqlite3";

export function migrate(db: Database) {
  db.pragma("journal_mode = WAL");

  db.exec(`
    CREATE TABLE IF NOT EXISTS invoice_runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      invoiceId TEXT NOT NULL,
      vendor TEXT NOT NULL,
      dataset TEXT NOT NULL,
      createdAt TEXT NOT NULL,
      invoiceNumber TEXT,
      fingerprint TEXT,
      isDuplicate INTEGER NOT NULL DEFAULT 0,
      duplicateOfInvoiceId TEXT
    );

    CREATE TABLE IF NOT EXISTS vendor_memory (
      id TEXT PRIMARY KEY,
      vendor TEXT NOT NULL,
      kind TEXT NOT NULL,
      pattern TEXT NOT NULL,
      confidence REAL NOT NULL,
      supportCount INTEGER NOT NULL,
      rejectCount INTEGER NOT NULL,
      lastUsedAt TEXT,
      status TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS learning_events (
      invoiceId TEXT PRIMARY KEY,
      learnedAt TEXT NOT NULL
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS audit_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts TEXT NOT NULL,
      eventType TEXT NOT NULL,
      vendor TEXT,
      invoiceId TEXT,
      entityType TEXT,
      entityId TEXT,
      metaJson TEXT
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS correction_memory (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      vendor TEXT NOT NULL,
      fieldPath TEXT NOT NULL,
      patternType TEXT NOT NULL,
      patternValue TEXT NOT NULL,
      recommendedValue TEXT,
      confidence REAL NOT NULL DEFAULT 0.5,
      rejectCount INTEGER NOT NULL DEFAULT 0,
      lastUsedAt TEXT,
      disabledAt TEXT,
      createdAt TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_correction_memory_vendor ON correction_memory(vendor);
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS resolution_memory (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      vendor TEXT NOT NULL,
      key TEXT NOT NULL,
      valueJson TEXT NOT NULL,
      confidence REAL NOT NULL DEFAULT 0.6,
      rejectCount INTEGER NOT NULL DEFAULT 0,
      lastUsedAt TEXT,
      disabledAt TEXT,
      createdAt TEXT NOT NULL,
      UNIQUE(vendor, key)
    );
    CREATE INDEX IF NOT EXISTS idx_resolution_memory_vendor ON resolution_memory(vendor);
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS duplicate_records (
      invoiceId TEXT PRIMARY KEY,
      vendor TEXT NOT NULL,
      fingerprint TEXT NOT NULL,
      duplicateOfInvoiceId TEXT,
      reason TEXT NOT NULL,
      createdAt TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_duplicate_records_vendor ON duplicate_records(vendor);
    CREATE INDEX IF NOT EXISTS idx_duplicate_records_fingerprint ON duplicate_records(fingerprint);
  `);

  const safeAdd = (sql: string) => {
    try { db.exec(sql); } catch { /* ignore */ }
  };

  safeAdd(`ALTER TABLE vendor_memory ADD COLUMN disabledAt TEXT;`);
  safeAdd(`ALTER TABLE vendor_memory ADD COLUMN metaJson TEXT;`);
}
