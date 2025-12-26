// src/engine/duplicateGuard.ts
import type Database from "better-sqlite3";

function normalize(s: unknown) {
  return String(s ?? "").trim().toLowerCase();
}

function nowIso(now?: Date) {
  return (now ?? new Date()).toISOString();
}

export function computeFingerprint(invoice: any) {
  const vendor = normalize(invoice.vendor);
  const invoiceNumber =
    normalize(invoice.invoiceNumber) ||
    normalize(invoice?.fields?.invoiceNumber) ||
    normalize(invoice?.normalizedInvoice?.invoiceNumber);

  const currency =
    normalize(invoice.currency) ||
    normalize(invoice?.fields?.currency) ||
    normalize(invoice?.normalizedInvoice?.currency);

  const total =
    normalize(invoice.total) ||
    normalize(invoice?.fields?.total) ||
    normalize(invoice?.normalizedInvoice?.total);

  // Raw text helps, but avoid hashing huge content. Take a small stable slice.
  const rawSlice = normalize(invoice.rawText).slice(0, 220);

  // Most duplicates in appendix are same vendor + same invoiceNumber.
  // Keep fingerprint stable and simple.
  return [vendor, invoiceNumber, currency, total, rawSlice].join("|");
}

export function detectDuplicate(
  db: Database,
  invoice: any,
  now?: Date
):
  | { isDuplicate: false }
  | { isDuplicate: true; duplicateOfInvoiceId: string; reason: string; fingerprint: string } {
  const vendor = String(invoice.vendor ?? "");
  const invoiceId = String(invoice.invoiceId ?? "");
  const invoiceNumber =
    String(invoice.invoiceNumber ?? invoice?.fields?.invoiceNumber ?? invoice?.normalizedInvoice?.invoiceNumber ?? "");

  const fingerprint = computeFingerprint(invoice);

  // 1) explicit cue words
  const raw = String(invoice.rawText ?? "").toLowerCase();
  const hasDuplicateCue =
    raw.includes("duplicate submission") ||
    raw.includes("erneute zusendung") ||
    raw.includes("duplicate") ||
    raw.includes("erneut");

  // 2) Strong rule: same vendor + same invoiceNumber seen before
  if (vendor && invoiceNumber) {
    const row = db
      .prepare(
        `
        SELECT invoiceId
        FROM invoice_runs
        WHERE vendor = ? AND invoiceNumber = ? AND invoiceId != ?
        ORDER BY rowid DESC
        LIMIT 1
      `
      )
      .get(vendor, invoiceNumber, invoiceId) as { invoiceId: string } | undefined;

    if (row?.invoiceId) {
      return {
        isDuplicate: true,
        duplicateOfInvoiceId: row.invoiceId,
        reason: hasDuplicateCue
          ? `Duplicate cue found in rawText; vendor+invoiceNumber already seen (${invoiceNumber}).`
          : `Same vendor+invoiceNumber already seen (${invoiceNumber}).`,
        fingerprint,
      };
    }
  }

  // 3) Fallback: same vendor + same fingerprint already recorded in duplicate_records
  const prior = db
    .prepare(
      `
      SELECT duplicateOfInvoiceId, invoiceId
      FROM duplicate_records
      WHERE vendor = ? AND fingerprint = ? AND invoiceId != ?
      ORDER BY createdAt DESC
      LIMIT 1
    `
    )
    .get(vendor, fingerprint, invoiceId) as { duplicateOfInvoiceId: string | null; invoiceId: string } | undefined;

  if (prior?.invoiceId) {
    return {
      isDuplicate: true,
      duplicateOfInvoiceId: prior.duplicateOfInvoiceId ?? prior.invoiceId,
      reason: `Fingerprint match for vendor (fallback match).`,
      fingerprint,
    };
  }

  // 4) If cue exists but no match, do not auto-mark duplicate; just let pipeline continue.
  // (prevents false positives)
  return { isDuplicate: false };
}

export function recordDuplicate(
  db: Database,
  args: {
    invoiceId: string;
    vendor: string;
    fingerprint: string;
    duplicateOfInvoiceId: string;
    reason: string;
    now?: Date;
  }
) {
  db.prepare(
    `
    INSERT OR REPLACE INTO duplicate_records
      (invoiceId, vendor, fingerprint, duplicateOfInvoiceId, reason, createdAt)
    VALUES
      (@invoiceId, @vendor, @fingerprint, @duplicateOfInvoiceId, @reason, @createdAt)
  `
  ).run({
    ...args,
    createdAt: nowIso(args.now),
  });
}
