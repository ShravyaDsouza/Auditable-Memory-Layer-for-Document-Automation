import fs from "node:fs";
import path from "node:path";

export type DatasetMode = "initial" | "full";

export type Manifest = {
  initial: { invoices: string; corrections: string; reference: string };
  full: { invoices: string; corrections: string; purchaseOrders: string; deliveryNotes: string };
};

export function loadManifest(repoRoot: string): Manifest {
  const manifestPath = path.join(repoRoot, "data", "manifest.json");
  const raw = fs.readFileSync(manifestPath, "utf-8");
  return JSON.parse(raw) as Manifest;
}
