import Database from "better-sqlite3";
import fs from "node:fs";
import path from "node:path";

export function openDb(repoRoot: string) {
  const storageDir = path.join(repoRoot, "storage");
  if (!fs.existsSync(storageDir)) fs.mkdirSync(storageDir);

  const dbPath = path.join(storageDir, "memory.db");
  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");
  return db;
}
