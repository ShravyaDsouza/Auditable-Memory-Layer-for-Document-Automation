// src/admin/adminCli.ts
import { getDb } from "../db/sqlite.js";
import { parseArgs } from "../utils/args.js";
import { listMemory } from "./listMemory.js";
import { disableVendorMemory, resetVendorMemoryConfidence } from "./commands.js";

export async function runAdminCli(argv: string[]) {
  const args = parseArgs(argv);

  const db = getDb();

  const cmd = args._[0]; // e.g. "list" | "disable" | "reset-confidence"

  if (cmd === "list") {
    const vendor = String(args.vendor ?? "");
    await listMemory(db, { vendor });
    return;
  }

  if (cmd === "disable") {
    const id = Number(args.id);
    if (!Number.isFinite(id)) throw new Error("--id is required");
    disableVendorMemory(db, id);
    console.log(`Disabled vendor_memory id=${id}`);
    return;
  }

  if (cmd === "reset-confidence") {
    const id = Number(args.id);
    const to = Number(args.to ?? 0.75);
    if (!Number.isFinite(id)) throw new Error("--id is required");
    if (!(to >= 0 && to <= 1)) throw new Error("--to must be between 0 and 1");
    resetVendorMemoryConfidence(db, id, to);
    console.log(`Reset confidence vendor_memory id=${id} -> ${to}`);
    return;
  }

  throw new Error(`Unknown admin command: ${cmd}`);
}
