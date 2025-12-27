import Database from "better-sqlite3";
import { fileURLToPath } from "node:url";
import path from "node:path";

import { listMemory } from "./listMemory.js";
import { disableVendorMemory, resetVendorMemoryConfidence } from "./commands.js";

function getDb(dbPath: string) {
  return new Database(dbPath);
}

function parseCliArgs(argv: string[]) {
  const out: Record<string, any> = { _: [] as string[] };

  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];

    if (!token.startsWith("--")) {
      out._.push(token);
      continue;
    }

    const eq = token.indexOf("=");
    if (eq !== -1) {
      const k = token.slice(2, eq);
      const v = token.slice(eq + 1);
      out[k] = v;
      continue;
    }

    const k = token.slice(2);
    const next = argv[i + 1];

    if (next && !next.startsWith("--")) {
      out[k] = next;
      i++;
    } else {
      out[k] = true;
    }
  }

  return out;
}

export async function runAdminCli(argv: string[]) {
  const args = parseCliArgs(argv);

  const dbPath = String(args.db ?? "storage/memory.db");
  const db = getDb(dbPath);

  try {
    const cmd = String(args._[0] ?? "");

    if (cmd === "list") {
      const vendor = String(args.vendor ?? "");
      await listMemory(db, { vendor });
      return;
    }

    if (cmd === "disable") {
      console.log("[adminCli] disable called", { rawId: args.id, argv, dbPath });

      const id = String(args.id ?? "").trim();
      if (!id) throw new Error("--id is required");

      disableVendorMemory(db, id);
      console.log(`Disabled vendor_memory id=${id}`);
      return;
    }

    if (cmd === "reset-confidence") {
      const id = String(args.id ?? "").trim();
      const to = Number(args.to ?? 0.75);

      if (!id) throw new Error("--id is required");
      if (!(to >= 0 && to <= 1)) throw new Error("--to must be between 0 and 1");

      resetVendorMemoryConfidence(db, id, to);
      console.log(`Reset confidence vendor_memory id=${id} -> ${to}`);
      return;
    }

    throw new Error(`Unknown admin command: ${cmd}`);
  } finally {
    db.close();
  }
}

const isEntry =
  process.argv[1] &&
  path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));

if (isEntry) {
  runAdminCli(process.argv.slice(2)).catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
