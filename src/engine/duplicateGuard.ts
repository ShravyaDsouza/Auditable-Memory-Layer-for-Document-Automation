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
  normalize(invoice.grossTotal) ||
  normalize(invoice?.fields?.grossTotal) ||
  normalize(invoice?.normalizedInvoice?.grossTotal) ||
  normalize(invoice.netTotal) ||
  normalize(invoice?.fields?.netTotal) ||
  normalize(invoice?.normalizedInvoice?.netTotal);

  const rawSlice = normalize(invoice.rawText).slice(0, 220);

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
  const invoiceNumber = String(
    invoice.invoiceNumber ??
      invoice?.fields?.invoiceNumber ??
      invoice?.normalizedInvoice?.invoiceNumber ??
      ""
  );

  const fingerprint = computeFingerprint(invoice);

  const raw = String(invoice.rawText ?? "").toLowerCase();
  const hasDuplicateCue =
    raw.includes("duplicate submission") ||
    raw.includes("erneute zusendung") ||
    raw.includes("duplicate") ||
    raw.includes("erneut");

  if (vendor && invoiceNumber) {
    const original = db
      .prepare(
        `
        SELECT invoiceId
        FROM invoice_runs
        WHERE vendor = ?
          AND invoiceNumber = ?
          AND invoiceId != ?
          AND COALESCE(isDuplicate, 0) = 0
        ORDER BY rowid ASC
        LIMIT 1
        `
      )
      .get(vendor, invoiceNumber, invoiceId) as { invoiceId: string } | undefined;

    if (original?.invoiceId) {
      return {
        isDuplicate: true,
        duplicateOfInvoiceId: original.invoiceId,
        reason: hasDuplicateCue
          ? `Duplicate cue found in rawText; vendor+invoiceNumber already seen (${invoiceNumber}).`
          : `Same vendor+invoiceNumber already seen (${invoiceNumber}).`,
        fingerprint,
      };
    }

    const any = db
      .prepare(
        `
        SELECT invoiceId
        FROM invoice_runs
        WHERE vendor = ?
          AND invoiceNumber = ?
          AND invoiceId != ?
        ORDER BY rowid ASC
        LIMIT 1
        `
      )
      .get(vendor, invoiceNumber, invoiceId) as { invoiceId: string } | undefined;

    if (any?.invoiceId) {
      const root = db
        .prepare(
          `
          SELECT duplicateOfInvoiceId
          FROM duplicate_records
          WHERE invoiceId = ?
          LIMIT 1
          `
        )
        .get(any.invoiceId) as { duplicateOfInvoiceId: string | null } | undefined;

      return {
        isDuplicate: true,
        duplicateOfInvoiceId: root?.duplicateOfInvoiceId ?? any.invoiceId,
        reason: hasDuplicateCue
          ? `Duplicate cue found in rawText; vendor+invoiceNumber already seen (${invoiceNumber}).`
          : `Same vendor+invoiceNumber already seen (${invoiceNumber}).`,
        fingerprint,
      };
    }
  }

  const prior = db
    .prepare(
      `
      SELECT duplicateOfInvoiceId, invoiceId
      FROM duplicate_records
      WHERE vendor = ? AND fingerprint = ? AND invoiceId != ?
      ORDER BY createdAt ASC
      LIMIT 1
      `
    )
    .get(vendor, fingerprint, invoiceId) as
    | { duplicateOfInvoiceId: string | null; invoiceId: string }
    | undefined;

  if (prior?.invoiceId) {
    return {
      isDuplicate: true,
      duplicateOfInvoiceId: prior.duplicateOfInvoiceId ?? prior.invoiceId,
      reason: `Fingerprint match for vendor (fallback match).`,
      fingerprint,
    };
  }

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
