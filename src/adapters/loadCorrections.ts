import fs from "node:fs";
import path from "node:path";
import type { DatasetMode, Manifest } from "./loadManifest.js";

export type HumanCorrection = {
  invoiceId: string;
  vendor: string;
  corrections: Array<{ field: string; from: unknown; to: unknown; reason: string }>;
  finalDecision: string; // approved | rejected | ...
};

export function loadCorrections(repoRoot: string, mode: DatasetMode, manifest: Manifest): HumanCorrection[] {
  const fileName = manifest[mode].corrections;
  const filePath = path.join(repoRoot, "data", fileName);
  return JSON.parse(fs.readFileSync(filePath, "utf-8")) as HumanCorrection[];
}
