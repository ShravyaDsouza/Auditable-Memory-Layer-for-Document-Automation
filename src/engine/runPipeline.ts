import type Database from "better-sqlite3";
import type { AgentOutput } from "../types/output.js";
import { getVendorMemories, upsertVendorMemory } from "../db/vendorMemory.js";
import { hasLearned, markLearned } from "../db/learningEvents.js";
import { detectDuplicate, recordDuplicate } from "./duplicateGuard.js";
import { logAuditEvent } from "../db/auditEvents.js";

/* ------------------ Helpers ------------------ */

function nowIso(d?: Date) {
  return (d ?? new Date()).toISOString();
}

function round2(n: number) {
  return Math.round(n * 100) / 100;
}

function extractLeistungsdatum(rawText: string): string | null {
  const m = rawText.match(/Leistungsdatum:\s*([0-9]{2})\.([0-9]{2})\.([0-9]{4})/);
  if (!m) return null;
  const [, dd, mm, yyyy] = m;
  return `${yyyy}-${mm}-${dd}`;
}

function parseCurrency(rawText: string): string | null {
  // examples: "Currency: EUR" or "Total: 2380.00 EUR"
  const m1 = rawText.match(/\bCurrency:\s*([A-Z]{3})\b/);
  if (m1) return m1[1];

  const m3 = rawText.match(/\bTotal:\s*[\d,.]+\s*([A-Z]{3})\b/);
  if (m3) return m3[1];

  const m2 = rawText.match(/\b([A-Z]{3})\b\s*$/m);
  if (m2 && ["EUR", "USD", "GBP"].includes(m2[1])) return m2[1];

  return null;
}

function extractTotalFromRawText(rawText: string): number | null {
  // Example: "Total: 2380.00 EUR"
  const m = rawText.match(/Total:\s*([0-9]+(?:[.,][0-9]{2})?)/i);
  if (!m) return null;
  const normalized = m[1].replace(",", ".");
  const val = Number(normalized);
  return Number.isFinite(val) ? val : null;
}

function isVatInclusiveText(rawText: string): boolean {
  return /MwSt\.\s*inkl\.|Prices\s+incl\.\s*VAT|VAT\s+included/i.test(rawText);
}

function extractSkonto(rawText: string): { percent: number; days: number } | null {
  // match both "days" and German "Tage"
  const m = rawText.match(/(\d+)%\s*Skonto.*?(\d+)\s*(days|tage)/i);
  if (!m) return null;
  return { percent: Number(m[1]), days: Number(m[2]) };
}

/* Supplier PO helper (kept simple) */
function parseDateToIso(dateStr: string | null | undefined): string | null {
  if (!dateStr) return null;

  const dot = dateStr.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
  if (dot) {
    const [, dd, mm, yyyy] = dot;
    return `${yyyy}-${mm}-${dd}`;
  }

  const dash = dateStr.match(/^(\d{2})-(\d{2})-(\d{4})$/);
  if (dash) {
    const [, dd, mm, yyyy] = dash;
    return `${yyyy}-${mm}-${dd}`;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) return dateStr;
  return null;
}

function daysBetween(aIso: string, bIso: string): number {
  const a = new Date(aIso).getTime();
  const b = new Date(bIso).getTime();
  return Math.abs(Math.round((a - b) / (1000 * 60 * 60 * 24)));
}

function getInvoiceSkus(extracted: any): string[] {
  return (extracted.fields?.lineItems ?? []).map((x: any) => x?.sku).filter(Boolean);
}

function suggestPoNumber(
  context: any,
  daysWindow = 30
): { poNumber: string; reason: string } | null {
  const vendor = context.vendor;
  const extracted = context.extracted;
  const invoiceDateIso = parseDateToIso(extracted.fields?.invoiceDate);
  if (!invoiceDateIso) return null;

  const skus = getInvoiceSkus(extracted);
  if (skus.length === 0) return null;

  const candidates = (context.reference?.purchaseOrders ?? [])
    .filter((po: any) => po.vendor === vendor)
    .filter((po: any) => daysBetween(invoiceDateIso, po.date) <= daysWindow)
    .filter((po: any) => {
      const poSkus = (po.lineItems ?? []).map((li: any) => li.sku);
      return skus.some((s) => poSkus.includes(s));
    });

  if (candidates.length === 1) {
    return {
      poNumber: candidates[0].poNumber,
      reason: `Only matching PO for vendor within ${daysWindow} days and matching SKU (${skus.join(", ")}).`,
    };
  }

  return null;
}

/* ------------------ Pipeline ------------------ */

export function runPipeline(
  db: Database,
  context: any,
  opts?: { now?: Date }
): AgentOutput {
  const now = opts?.now ?? new Date();

  const vendor = context.vendor as string;
  const extracted = context.extracted;
  const rawText: string = extracted.rawText ?? "";
  const invoiceId: string = extracted.invoiceId;

  // 1) DUPLICATE GUARD (hard stop)
  const dup = detectDuplicate(db, extracted, now);
  if (dup.isDuplicate) {
    recordDuplicate(db, {
      invoiceId: extracted.invoiceId,
      vendor: extracted.vendor,
      fingerprint: dup.fingerprint,
      duplicateOfInvoiceId: dup.duplicateOfInvoiceId,
      reason: dup.reason,
      now,
    });

    // invoice_runs is inserted BEFORE calling runPipeline (demo runner),
    // so this should succeed; keep try/catch to avoid brittle demo runs.
    try {
      db.prepare(
        `
        UPDATE invoice_runs
        SET isDuplicate = 1,
            duplicateOfInvoiceId = @duplicateOf
        WHERE invoiceId = @invoiceId
      `
      ).run({ invoiceId: extracted.invoiceId, duplicateOf: dup.duplicateOfInvoiceId });
    } catch {
      /* noop */
    }

    logAuditEvent(db, {
      eventType: "DUPLICATE_DETECTED",
      vendor: extracted.vendor,
      invoiceId: extracted.invoiceId,
      meta: {
        duplicateOfInvoiceId: dup.duplicateOfInvoiceId,
        reason: dup.reason,
      },
      now,
    });

    return {
      normalizedInvoice: extracted.fields ?? extracted.normalizedInvoice ?? {},
      proposedCorrections: [],
      requiresHumanReview: true,
      reasoning: `Duplicate detected: ${dup.reason} (duplicate of ${dup.duplicateOfInvoiceId}).`,
      confidenceScore: 0.2,
      memoryUpdates: [],
      auditTrail: [
        {
          step: "duplicateGuard",
          timestamp: nowIso(now),
          details: `Duplicate of ${dup.duplicateOfInvoiceId}: ${dup.reason}`,
        },
      ],
    };
  }

  const auditTrail: AgentOutput["auditTrail"] = [];
  const proposedCorrections: AgentOutput["proposedCorrections"] = [];
  const memoryUpdates: AgentOutput["memoryUpdates"] = [];
  const reasoningParts: string[] = [];

  let confidenceScore = 0.25;

  /* -------- RECALL -------- */
  const vendorMems = getVendorMemories(db, vendor).filter((m) => m.status === "active");

  auditTrail.push({
    step: "recall",
    timestamp: nowIso(now),
    details: `Recalled ${vendorMems.length} vendor memories for ${vendor}.`,
  });

  /* -------- APPLY -------- */

  // Supplier: serviceDate
  const hasServiceDateMemory = vendorMems.some(
    (m) => m.kind === "serviceDate_from_label" && m.pattern === "Leistungsdatum"
  );

  const candidateServiceDate = extractLeistungsdatum(rawText);
  if (candidateServiceDate && extracted.fields?.serviceDate == null) {
    const appliedConfidence = hasServiceDateMemory ? 0.85 : 0.55;

    proposedCorrections.push({
      field: "serviceDate",
      from: null,
      to: candidateServiceDate,
      source: hasServiceDateMemory ? "vendor_memory" : "rawText_heuristic",
      confidence: appliedConfidence,
      reason: `Found Leistungsdatum in rawText (${candidateServiceDate}).`,
    });

    reasoningParts.push(
      hasServiceDateMemory
        ? `Applied vendor memory: "${vendor}" uses Leistungsdatum as service date.`
        : `Heuristic: rawText contains Leistungsdatum, suggesting serviceDate.`
    );

    confidenceScore = Math.max(confidenceScore, appliedConfidence);
  }

  // Supplier: PO inference (only if learned)
  const hasPoMemory = vendorMems.some(
    (m) => m.kind === "po_match_strategy" && m.pattern === "sku_and_date_window"
  );

  if (extracted.fields?.poNumber == null && hasPoMemory) {
    const suggestion = suggestPoNumber(context, 30);
    if (suggestion) {
      proposedCorrections.push({
        field: "poNumber",
        from: null,
        to: suggestion.poNumber,
        source: "vendor_memory",
        confidence: 0.82,
        reason: suggestion.reason,
      });

      reasoningParts.push("Applied vendor memory: inferred PO using SKU + 30-day window.");
      confidenceScore = Math.max(confidenceScore, 0.82);
    }
  }

  // Parts AG: VAT inclusive strategy (heuristic first, memory later)
  const hasVatInclusiveMemory = vendorMems.some(
    (m) => m.kind === "vat_inclusive_pricing" && m.pattern === "mwst_inkl"
  );

  const vatInclusiveDetected = isVatInclusiveText(rawText);

  if (vatInclusiveDetected) {
    const appliedConfidence = hasVatInclusiveMemory ? 0.85 : 0.6;

    const net = Number(extracted.fields?.netTotal);
    const rate = Number(extracted.fields?.taxRate ?? 0.19);
    const grossFromText = extractTotalFromRawText(rawText);

    const currentGross = Number(extracted.fields?.grossTotal);
    const currentTax = Number(extracted.fields?.taxTotal);

    if (
      Number.isFinite(net) &&
      Number.isFinite(rate) &&
      Number.isFinite(currentGross) &&
      Number.isFinite(currentTax)
    ) {
      const expectedGross =
        grossFromText != null && Number.isFinite(grossFromText)
          ? round2(grossFromText)
          : round2(net * (1 + rate));

      const expectedTax = round2(expectedGross - net);

      let pushedAnyVatFix = false;

      if (Math.abs(currentGross - expectedGross) > 0.01) {
        proposedCorrections.push({
          field: "grossTotal",
          from: currentGross,
          to: expectedGross,
          source: hasVatInclusiveMemory ? "vendor_memory" : "rawText_heuristic",
          confidence: appliedConfidence,
          reason:
            grossFromText != null
              ? `VAT-inclusive pricing detected; rawText Total=${expectedGross} used for grossTotal.`
              : `VAT-inclusive pricing detected; recomputed grossTotal from netTotal and taxRate.`,
        });
        pushedAnyVatFix = true;
      }

      if (Math.abs(currentTax - expectedTax) > 0.01) {
        proposedCorrections.push({
          field: "taxTotal",
          from: currentTax,
          to: expectedTax,
          source: hasVatInclusiveMemory ? "vendor_memory" : "rawText_heuristic",
          confidence: appliedConfidence,
          reason: `VAT-inclusive pricing detected; taxTotal should be grossTotal - netTotal (${expectedTax}).`,
        });
        pushedAnyVatFix = true;
      }

      if (pushedAnyVatFix) {
        reasoningParts.push(
          hasVatInclusiveMemory
            ? "Applied vendor memory: VAT-inclusive pricing → recompute totals."
            : "Heuristic: VAT-inclusive pricing detected in rawText → propose recompute totals."
        );
        confidenceScore = Math.max(confidenceScore, appliedConfidence);
      } else {
        reasoningParts.push(
          hasVatInclusiveMemory
            ? "Recognized VAT-inclusive pricing (vendor memory). Totals appear consistent; no correction needed."
            : "Recognized VAT-inclusive pricing (heuristic). Totals appear consistent; no correction needed."
        );
        confidenceScore = Math.max(confidenceScore, hasVatInclusiveMemory ? 0.75 : 0.55);
      }
    }
  }

  // Currency memory awareness
  const hasCurrencyMemory = vendorMems.some((m) => m.kind === "currency_from_rawText");

  // Currency recovery (only if currency missing)
  if (extracted.fields?.currency == null) {
    const cur = parseCurrency(rawText);
    if (cur) {
      const appliedConfidence = hasCurrencyMemory ? 0.85 : 0.7;

      proposedCorrections.push({
        field: "currency",
        from: null,
        to: cur,
        source: hasCurrencyMemory ? "vendor_memory" : "rawText_heuristic",
        confidence: appliedConfidence,
        reason: `Recovered currency from rawText (${cur}).`,
      });

      reasoningParts.push(
        hasCurrencyMemory
          ? "Applied vendor memory: currency can be recovered from rawText for this vendor."
          : "Heuristic: recovered missing currency from rawText."
      );

      confidenceScore = Math.max(confidenceScore, appliedConfidence);
    }
  }

  /* -------- Freight & Co rules (guarded) -------- */
  const isFreightVendor = /freight\s*&\s*co/i.test(vendor);

  if (isFreightVendor) {
    // Freight & Co: Skonto extraction (only if discountTerms missing)
    if (extracted.fields?.discountTerms == null) {
      const skonto = extractSkonto(rawText);
      if (skonto) {
        const hasSkontoMemory = vendorMems.some((m) => m.kind === "skonto_terms");
        const appliedConfidence = hasSkontoMemory ? 0.85 : 0.6;

        proposedCorrections.push({
          field: "discountTerms",
          from: null,
          to: skonto,
          source: hasSkontoMemory ? "vendor_memory" : "rawText_heuristic",
          confidence: appliedConfidence,
          reason: `Detected ${skonto.percent}% Skonto if paid within ${skonto.days} days.`,
        });

        reasoningParts.push(
          hasSkontoMemory
            ? "Applied vendor memory: Freight & Co uses Skonto payment terms."
            : "Heuristic: detected Skonto payment terms in rawText."
        );

        confidenceScore = Math.max(confidenceScore, appliedConfidence);
      }
    }

    // Freight & Co: SKU mapping (only if any freight-ish line has sku missing)
    const freightItem = extracted.fields?.lineItems?.find(
      (li: any) => li.sku == null && /shipping|seefracht|transport/i.test(li.description ?? "")
    );

    if (freightItem) {
      const hasSkuMemory = vendorMems.some(
        (m) => m.kind === "sku_mapping" && m.pattern === "FREIGHT"
      );
      const appliedConfidence = hasSkuMemory ? 0.85 : 0.6;

      proposedCorrections.push({
        field: "lineItems[].sku",
        from: null,
        to: "FREIGHT",
        source: hasSkuMemory ? "vendor_memory" : "rawText_heuristic",
        confidence: appliedConfidence,
        reason: "Freight-related description mapped to SKU FREIGHT.",
      });

      reasoningParts.push(
        hasSkuMemory
          ? "Applied vendor memory: Freight descriptions map to SKU FREIGHT."
          : "Heuristic: freight-related description suggests SKU FREIGHT."
      );

      confidenceScore = Math.max(confidenceScore, appliedConfidence);
    }
  }

  auditTrail.push({
    step: "apply",
    timestamp: nowIso(now),
    details: `Proposed ${proposedCorrections.length} correction(s).`,
  });

  /* -------- DECIDE -------- */
  const REVIEW_THRESHOLD = 0.75;

  const requiresHumanReview =
    proposedCorrections.length > 0 &&
    proposedCorrections.some((c: any) => (c.confidence ?? 0) < REVIEW_THRESHOLD);

  auditTrail.push({
    step: "decide",
    timestamp: nowIso(now),
    details:
      proposedCorrections.length === 0
        ? "No corrections proposed."
        : requiresHumanReview
        ? "Escalated: at least one correction below confidence threshold."
        : "Auto-correct possible: all corrections high confidence.",
  });

  /* -------- LEARN -------- */
  if (hasLearned(db, invoiceId)) {
    auditTrail.push({
      step: "learn",
      timestamp: nowIso(now),
      details: `Skipped learning: invoiceId ${invoiceId} already learned.`,
    });
  } else {
    const correctionsForInvoice = context.correctionsForInvoice ?? [];
    const approved = correctionsForInvoice.find((x: any) => x.finalDecision === "approved");

    if (approved) {
      // Supplier memory learns (serviceDate/poNumber)
      const serviceFix = approved.corrections?.find((c: any) => c.field === "serviceDate");
      if (serviceFix) {
        const id = `${vendor}::serviceDate_from_label::Leistungsdatum`;
        const existing = vendorMems.find((m) => m.id === id);
        const nextConfidence = Math.min(0.95, (existing?.confidence ?? 0.6) + 0.1);

        upsertVendorMemory(db, {
          id,
          vendor,
          kind: "serviceDate_from_label",
          pattern: "Leistungsdatum",
          confidence: nextConfidence,
          supportCount: (existing?.supportCount ?? 0) + 1,
          rejectCount: existing?.rejectCount ?? 0,
          lastUsedAt: nowIso(now),
          status: "active",
        });

        memoryUpdates.push({ type: "vendor_memory_upsert", id, confidence: nextConfidence });
      }

      const poFix = approved.corrections?.find((c: any) => c.field === "poNumber");
      if (poFix) {
        const id = `${vendor}::po_match_strategy::sku_and_date_window`;
        const existing = vendorMems.find((m) => m.id === id);
        const nextConfidence = Math.min(0.95, (existing?.confidence ?? 0.6) + 0.1);

        upsertVendorMemory(db, {
          id,
          vendor,
          kind: "po_match_strategy",
          pattern: "sku_and_date_window",
          confidence: nextConfidence,
          supportCount: (existing?.supportCount ?? 0) + 1,
          rejectCount: existing?.rejectCount ?? 0,
          lastUsedAt: nowIso(now),
          status: "active",
        });

        memoryUpdates.push({ type: "vendor_memory_upsert", id, confidence: nextConfidence });
      }

      // Learn VAT-inclusive strategy if totals were corrected and rawText hints VAT included
      const touchesTotals = (approved.corrections ?? []).some(
        (c: any) => c.field === "taxTotal" || c.field === "grossTotal"
      );

      if (touchesTotals && isVatInclusiveText(rawText)) {
        const id = `${vendor}::vat_inclusive_pricing::mwst_inkl`;
        const existing = vendorMems.find((m) => m.id === id);
        const nextConfidence = Math.min(0.95, (existing?.confidence ?? 0.6) + 0.1);

        upsertVendorMemory(db, {
          id,
          vendor,
          kind: "vat_inclusive_pricing",
          pattern: "mwst_inkl",
          confidence: nextConfidence,
          supportCount: (existing?.supportCount ?? 0) + 1,
          rejectCount: existing?.rejectCount ?? 0,
          lastUsedAt: nowIso(now),
          status: "active",
        });

        memoryUpdates.push({
          type: "vendor_memory_upsert",
          id,
          confidence: nextConfidence,
          note: "Learned VAT-inclusive pricing behavior.",
        });
      }

      // Learn currency recovery behavior
      const currencyFix = approved.corrections?.find((c: any) => c.field === "currency");
      if (currencyFix) {
        const id = `${vendor}::currency_from_rawText::default`;
        const existing = vendorMems.find((m) => m.id === id);
        const nextConfidence = Math.min(0.95, (existing?.confidence ?? 0.6) + 0.1);

        upsertVendorMemory(db, {
          id,
          vendor,
          kind: "currency_from_rawText",
          pattern: "default",
          confidence: nextConfidence,
          supportCount: (existing?.supportCount ?? 0) + 1,
          rejectCount: existing?.rejectCount ?? 0,
          lastUsedAt: nowIso(now),
          status: "active",
        });

        memoryUpdates.push({
          type: "vendor_memory_upsert",
          id,
          confidence: nextConfidence,
          note: "Learned currency-from-rawText behavior.",
        });
      }

      /* -------- Freight & Co: learning (Skonto + SKU mapping) -------- */
      if (isFreightVendor) {
        // Learn Skonto terms
        const skontoFix = approved.corrections?.find((c: any) => c.field === "discountTerms");
        if (skontoFix) {
          const id = `${vendor}::skonto_terms::default`;
          const existing = vendorMems.find((m) => m.id === id);
          const supportCount = (existing?.supportCount ?? 0) + 1;
          const rejectCount = existing?.rejectCount ?? 0;
          const nextConfidence = Math.min(0.95, (existing?.confidence ?? 0.6) + 0.1);

          upsertVendorMemory(db, {
            id,
            vendor,
            kind: "skonto_terms",
            pattern: "default",
            confidence: nextConfidence,
            supportCount,
            rejectCount,
            lastUsedAt: nowIso(now),
            status: "active",
          });

          memoryUpdates.push({
            type: "vendor_memory_upsert",
            id,
            confidence: nextConfidence,
            note: "Learned Skonto terms pattern for Freight & Co.",
          });
        }

        // Learn SKU mapping (robust field match)
        const skuFix = approved.corrections?.find(
          (c: any) =>
            c.field === "lineItems[].sku" ||
            (typeof c.field === "string" && c.field.includes("lineItems") && c.field.includes("sku"))
        );

        if (skuFix) {
          const id = `${vendor}::sku_mapping::FREIGHT`;
          const existing = vendorMems.find((m) => m.id === id);
          const supportCount = (existing?.supportCount ?? 0) + 1;
          const rejectCount = existing?.rejectCount ?? 0;
          const nextConfidence = Math.min(0.95, (existing?.confidence ?? 0.6) + 0.1);

          upsertVendorMemory(db, {
            id,
            vendor,
            kind: "sku_mapping",
            pattern: "FREIGHT",
            confidence: nextConfidence,
            supportCount,
            rejectCount,
            lastUsedAt: nowIso(now),
            status: "active",
          });

          memoryUpdates.push({
            type: "vendor_memory_upsert",
            id,
            confidence: nextConfidence,
            note: "Learned freight SKU mapping to FREIGHT.",
          });
        }
      }

      markLearned(db, invoiceId);

      // Optional: audit event for learning
      logAuditEvent(db, {
        eventType: "LEARN_APPROVED",
        vendor,
        invoiceId,
        meta: { updates: memoryUpdates.map((u: any) => u.id).filter(Boolean) },
        now,
      });

      auditTrail.push({
        step: "learn",
        timestamp: nowIso(now),
        details: `Learning applied from approved corrections for invoiceId ${invoiceId}.`,
      });
    } else {
      auditTrail.push({
        step: "learn",
        timestamp: nowIso(now),
        details: "No approved human correction available for learning.",
      });
    }
  }

  /* -------- OUTPUT -------- */
  return {
    normalizedInvoice: extracted.fields ?? extracted.normalizedInvoice ?? {},
    proposedCorrections,
    requiresHumanReview,
    reasoning: reasoningParts.join(" "),
    confidenceScore,
    memoryUpdates,
    auditTrail,
  };
}