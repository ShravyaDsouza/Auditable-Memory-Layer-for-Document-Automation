import { getArg } from "../utils/args.js";
import { loadManifest } from "../adapters/loadManifest.js";
import { loadInvoices } from "../adapters/loadInvoices.js";
import { loadCorrections } from "../adapters/loadCorrections.js";
import { loadReferenceData } from "../adapters/loadReferenceData.js";
import { openDb } from "../db/sqlite.js";
import { migrate } from "../db/migrations.js";
import { runPipeline } from "../engine/runPipeline.js";

const repoRoot = process.cwd();

const dataset = (getArg("dataset", "full") as "initial" | "full");
const invoiceId = getArg("invoiceId");

if (!invoiceId) {
  console.error("Missing --invoiceId (example: --invoiceId INV-A-001)");
  process.exit(1);
}

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

const output = runPipeline(context as any);
console.log(JSON.stringify(output, null, 2));

// DB proof (keep as-is)
const db = openDb(repoRoot);
migrate(db);

db.prepare(
  `INSERT INTO invoice_runs (invoiceId, vendor, dataset, createdAt)
   VALUES (?, ?, ?, ?)`
).run(invoice.invoiceId, invoice.vendor, dataset, new Date().toISOString());

const last = db.prepare(`SELECT * FROM invoice_runs ORDER BY id DESC LIMIT 1`).get();
console.log("\nDB write OK. Last invoice_runs row:");
console.log(last);