import fs from "node:fs";
import path from "node:path";
import type { DatasetMode, Manifest } from "./loadManifest.js";

export type InvoiceExtracted = {
  invoiceId: string;
  vendor: string;
  fields: Record<string, unknown>;
  confidence: number;
  rawText?: string | null;
};

export function loadInvoices(repoRoot: string, mode: DatasetMode, manifest: Manifest): InvoiceExtracted[] {
  const fileName = manifest[mode].invoices;
  const filePath = path.join(repoRoot, "data", fileName);
  return JSON.parse(fs.readFileSync(filePath, "utf-8")) as InvoiceExtracted[];
}
