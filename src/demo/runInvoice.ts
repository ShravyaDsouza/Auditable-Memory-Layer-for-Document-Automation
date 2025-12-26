import { getArg } from "../utils/args.js";
import { loadManifest } from "../adapters/loadManifest.js";
import { loadInvoices } from "../adapters/loadInvoices.js";
import { loadCorrections } from "../adapters/loadCorrections.js";
import { loadReferenceData } from "../adapters/loadReferenceData.js";
import { openDb } from "../db/sqlite.js";
import { migrate } from "../db/migrations.js";
import { runPipeline } from "../engine/runPipeline.js";
import { computeFingerprint } from "../engine/duplicateGuard.js";

const repoRoot = process.cwd();

const dataset = getArg("dataset", "full") as "initial" | "full";
const invoiceId = getArg("invoiceId");

// ✅ NEW
const simulateDaysRaw = getArg("simulateDays", "0");
const simulateDays = Number(simulateDaysRaw);

if (!Number.isFinite(simulateDays)) {
  console.error(`Invalid --simulateDays: ${simulateDaysRaw} (expected a number)`);
  process.exit(1);
}

if (!invoiceId) {
  console.error("Missing --invoiceId (example: --invoiceId INV-A-001)");
  process.exit(1);
}

(async () => {
  const manifest = loadManifest(repoRoot);
  const invoices = loadInvoices(repoRoot, dataset, manifest);
  const corrections = loadCorrections(repoRoot, dataset, manifest);
  const reference = loadReferenceData(repoRoot, dataset, manifest);

  const invoice = invoices.find((x) => x.invoiceId === invoiceId);
  if (!invoice) {
    console.error(`Invoice not found: ${invoiceId}`);
    process.exit(1);
  }

  const context = {
    invoiceId: invoice.invoiceId,
    vendor: invoice.vendor,
    extracted: invoice,
    reference,
    correctionsForInvoice: corrections.filter((c) => c.invoiceId === invoiceId),
  };

  const db = openDb(repoRoot);
  migrate(db);

  const invoiceNumberRaw =
    (invoice as any).invoiceNumber ??
    (invoice as any)?.fields?.invoiceNumber ??
    (invoice as any)?.normalizedInvoice?.invoiceNumber ??
    null;

  const invoiceNumber = invoiceNumberRaw ? String(invoiceNumberRaw).trim() : null;

  const fingerprint = computeFingerprint(invoice);

  // ✅ NEW: shift "now" for the whole run
  const now = new Date(Date.now() + simulateDays * 24 * 60 * 60 * 1000);

  // Use shifted now for createdAt too (optional but makes demo consistent)
  const createdAt = now.toISOString();

  db.prepare(
    `INSERT INTO invoice_runs
      (invoiceId, vendor, dataset, createdAt, invoiceNumber, fingerprint)
     VALUES (?, ?, ?, ?, ?, ?)`
  ).run(invoice.invoiceId, invoice.vendor, dataset, createdAt, invoiceNumber, fingerprint);

  // ✅ CRITICAL: pass now into pipeline so decay is visible
  const output = await (runPipeline as any)(db, context as any, { now });

  console.log(JSON.stringify(output, null, 2));

  const last = db.prepare(`SELECT * FROM invoice_runs ORDER BY id DESC LIMIT 1`).get();
  console.log("\nDB write OK. Last invoice_runs row:");
  console.log(last);
})();
