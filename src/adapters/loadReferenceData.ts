import fs from "node:fs";
import path from "node:path";
import type { DatasetMode, Manifest } from "./loadManifest.js";

export type PurchaseOrder = {
  poNumber: string;
  vendor: string;
  date: string;
  lineItems: Array<{ sku: string; qty: number; unitPrice: number }>;
};

export type DeliveryNote = {
  dnNumber: string;
  vendor: string;
  poNumber: string;
  date?: string;
  lineItems: Array<{ sku: string; qtyDelivered: number }>;
};

export type ReferenceData = {
  purchaseOrders: PurchaseOrder[];
  deliveryNotes: DeliveryNote[];
};

export function loadReferenceData(repoRoot: string, mode: DatasetMode, manifest: Manifest): ReferenceData {
  if (mode === "initial") {
    const p = path.join(repoRoot, "data", manifest.initial.reference);
    return JSON.parse(fs.readFileSync(p, "utf-8")) as ReferenceData;
  }

  const poPath = path.join(repoRoot, "data", manifest.full.purchaseOrders);
  const dnPath = path.join(repoRoot, "data", manifest.full.deliveryNotes);

  const purchaseOrders = JSON.parse(fs.readFileSync(poPath, "utf-8")) as PurchaseOrder[];
  const deliveryNotes = JSON.parse(fs.readFileSync(dnPath, "utf-8")) as DeliveryNote[];

  return { purchaseOrders, deliveryNotes };
}
