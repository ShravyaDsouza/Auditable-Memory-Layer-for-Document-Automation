import type Database from "better-sqlite3";
import type { AgentOutput } from "../types/output.js";
import { getVendorMemories, upsertVendorMemory, markVendorRejected } from "../db/vendorMemory.js";
import { hasLearned, markLearned } from "../db/learningEvents.js";
import { detectDuplicate, recordDuplicate } from "./duplicateGuard.js";
import { logAuditEvent } from "../db/auditEvents.js";
import {
  getCorrectionMemories,
  findCorrectionMemory,
  markUsed,
  upsertCorrectionMemory,
  markRejected,
} from "../db/correctionMemory.js";
import { getResolution, recordResolutionDecision } from "../db/resolutionMemory.js";
import { daysSince, decayedConfidence } from "./confidenceDecay.js";

const RES_KEYS = {
  serviceDate: "serviceDate_from_leistungsdatum",
  poInfer: "po_infer_sku_date_window",
  qtyToDn: "qty_to_delivery_note",
  vatTotals: "vat_inclusive_recompute_totals",
  currency: "currency_from_rawText",
  skonto: "skonto_terms_extraction",
  freightSku: "freight_sku_mapping",
} as const;

function applyResolutionMemoryToConfidence(args: {
  db: Database;
  vendor: string;
  key: string;
  base: number;
}): { adjusted: number; note: string } {
  const { db, vendor, key, base } = args;
  const res = getResolution(db, vendor, key);

  if (!res) return { adjusted: base, note: `No resolution history.` };

  const a = res.value.approved;
  const r = res.value.rejected;

  let adjusted = base;

  if (a >= 2 && r === 0) adjusted = Math.min(0.95, base + 0.15);
  if (r >= 1) adjusted = Math.max(0.2, base - 0.2);

  return {
    adjusted,
    note: `Resolution: approved=${a}, rejected=${r}, ${base}→${adjusted}`,
  };
}

function nowIso(d?: Date) {
  return (d ?? new Date()).toISOString();
}

function round2(n: number) {
  return Math.round(n * 100) / 100;
}

function isMissing(v: any) {
  return v === null || v === undefined || v === "";
}

function hasCriticalMissingFields(normalized: any, proposed: any[]) {
  const missingCurrency = isMissing(normalized.currency);
  const missingInvoiceNumber = isMissing(normalized.invoiceNumber);
  const missingInvoiceDate = isMissing(normalized.invoiceDate);

  const missingNet = !Number.isFinite(Number(normalized.netTotal));
  const missingGross = !Number.isFinite(Number(normalized.grossTotal));

  const missingPo = isMissing(normalized.poNumber);

  const hasPoSuggestionHigh = proposed?.some(
    (c: any) => c.field === "poNumber" && Number(c.confidence ?? 0) >= 0.75
  );

  const missingPoIsCritical = missingPo && !hasPoSuggestionHigh;

  return (
    missingInvoiceNumber ||
    missingInvoiceDate ||
    missingCurrency ||
    missingNet ||
    missingGross ||
    missingPoIsCritical
  );
}

function extractLeistungsdatum(rawText: string): string | null {
  const m = rawText.match(/Leistungsdatum:\s*([0-9]{2})\.([0-9]{2})\.([0-9]{4})/);
  if (!m) return null;
  const [, dd, mm, yyyy] = m;
  return `${yyyy}-${mm}-${dd}`;
}

function parseCurrency(rawText: string): string | null {
  const m1 = rawText.match(/\bCurrency:\s*([A-Z]{3})\b/);
  if (m1) return m1[1];

  const m3 = rawText.match(/\bTotal:\s*[\d,.]+\s*([A-Z]{3})\b/);
  if (m3) return m3[1];

  const m2 = rawText.match(/\b([A-Z]{3})\b\s*$/m);
  if (m2 && ["EUR", "USD", "GBP"].includes(m2[1])) return m2[1];

  return null;
}

function extractTotalFromRawText(rawText: string): number | null {
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
  const m = rawText.match(/(\d+)%\s*Skonto.*?(\d+)\s*(days|tage)/i);
  if (!m) return null;
  return { percent: Number(m[1]), days: Number(m[2]) };
}

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

function suggestPoNumber(context: any, daysWindow = 30): { poNumber: string; reason: string } | null {
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

function findMatchingDeliveryQty(context: any, sku: string): number | null {
  const vendor = context.vendor;
  const invoicePo = context.extracted.fields?.poNumber;

  const dns = (context.reference?.deliveryNotes ?? [])
    .filter((dn: any) => dn.vendor === vendor)
    .filter((dn: any) => !invoicePo || dn.poNumber === invoicePo);

  for (const dn of dns) {
    const li = dn.lineItems?.find((x: any) => x.sku === sku);
    if (li && Number.isFinite(li.qtyDelivered)) return li.qtyDelivered;
  }

  return null;
}

function isSuspectCorrectionMemory(args: { mem: any; resolution?: any | null; now: Date }) {
  const { mem, resolution, now } = args;

  if (!mem) return true;

  if (mem.status && mem.status !== "active") return true;

  const rejectCount = Number(mem.rejectCount ?? 0);
  if (rejectCount >= 2) return true;

  const baseConf = Number(mem.confidence ?? 0);
  if (baseConf > 0 && baseConf < 0.65) return true;

  const r = Number(resolution?.value?.rejected ?? 0);
  if (r >= 2) return true;

  const ageDays = daysSince(mem.lastUsedAt ?? mem.createdAt ?? null, now);
  const eff = decayedConfidence(baseConf, ageDays);
  if (eff < 0.65) return true;

  return false;
}

function canApplyVendorMem(mem: any, now: Date) {
  if (!mem) return false;
  if (mem.status && mem.status !== "active") return false;
  const rejects = Number(mem.rejectCount ?? 0);
  if (rejects >= 2) return false;

  const base = Number(mem.confidence ?? 0);
  const ageDays = daysSince(mem.lastUsedAt ?? mem.createdAt ?? null, now);
  const eff = decayedConfidence(base, ageDays);

  return eff >= 0.65;
}

export function runPipeline(db: Database, context: any, opts?: { now?: Date }): AgentOutput {
  const now = opts?.now ?? new Date();

  const vendor = context.vendor as string;
  const extracted = context.extracted;
  const rawText: string = extracted.rawText ?? "";
  const invoiceId: string = extracted.invoiceId;

  const usedResolutionKeys = new Set<string>();
  const usedCorrectionMemoryIds = new Set<string>();

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

    try {
      db.prepare(
        `
        UPDATE invoice_runs
        SET isDuplicate = 1,
            duplicateOfInvoiceId = @duplicateOf
        WHERE invoiceId = @invoiceId
      `
      ).run({ invoiceId: extracted.invoiceId, duplicateOf: dup.duplicateOfInvoiceId });
    } catch {}

    logAuditEvent(db, {
      eventType: "DUPLICATE_DETECTED",
      vendor: extracted.vendor,
      invoiceId: extracted.invoiceId,
      meta: { duplicateOfInvoiceId: dup.duplicateOfInvoiceId, reason: dup.reason },
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

  const vendorMemsAll = getVendorMemories(db, vendor);
  auditTrail.push({
    step: "recall",
    timestamp: nowIso(now),
    details: `Recalled ${vendorMemsAll.length} vendor memories for ${vendor}.`,
  });

  const corrMems = getCorrectionMemories(db, vendor);
  auditTrail.push({
    step: "recall",
    timestamp: nowIso(now),
    details: `Recalled ${corrMems.length} correction memories for ${vendor}.`,
  });

  const serviceMem = vendorMemsAll.find(
    (m: any) => m.kind === "serviceDate_from_label" && m.pattern === "Leistungsdatum"
  );

  const canUseServiceMem = serviceMem ? canApplyVendorMem(serviceMem, now) : false;

  const candidateServiceDate = extractLeistungsdatum(rawText);
  if (candidateServiceDate && extracted.fields?.serviceDate == null) {
    const resKey = RES_KEYS.serviceDate;
    usedResolutionKeys.add(resKey);

    const base = canUseServiceMem ? 0.85 : 0.55;

    const baseAfterDecay = canUseServiceMem
      ? decayedConfidence(
          base,
          daysSince(serviceMem?.lastUsedAt ?? serviceMem?.createdAt ?? null, now)
        )
      : base;

    const shaped = applyResolutionMemoryToConfidence({ db, vendor, key: resKey, base: baseAfterDecay });
    const appliedConfidence = shaped.adjusted;

    auditTrail.push({
      step: "apply",
      timestamp: nowIso(now),
      details: `Resolution memory applied for ${resKey}: ${shaped.note}`,
    });

    proposedCorrections.push({
      field: "serviceDate",
      from: null,
      to: candidateServiceDate,
      source: canUseServiceMem ? "vendor_memory" : "rawText_heuristic",
      confidence: appliedConfidence,
      reason: `Found Leistungsdatum in rawText (${candidateServiceDate}).`,
    });

    reasoningParts.push(
      canUseServiceMem
        ? `Applied vendor memory (decayed): "${vendor}" uses Leistungsdatum as service date.`
        : `Heuristic: rawText contains Leistungsdatum, suggesting serviceDate.`
    );

    confidenceScore = Math.max(confidenceScore, appliedConfidence);
  } else if (serviceMem && !canUseServiceMem) {
    auditTrail.push({
      step: "apply",
      timestamp: nowIso(now),
      details: `Vendor memory for serviceDate exists but is disabled/suspect/decayed; not auto-applied (id=${serviceMem.id}).`,
    });
  }

  const poMem = vendorMemsAll.find(
    (m: any) => m.kind === "po_match_strategy" && m.pattern === "sku_and_date_window"
  );
  const canUsePoMem = poMem ? canApplyVendorMem(poMem, now) : false;

  if (extracted.fields?.poNumber == null) {
    const suggestion = suggestPoNumber(context, 30);

    if (suggestion) {
      const resKey = RES_KEYS.poInfer ?? "po_infer_sku_date_window";
      usedResolutionKeys.add(resKey);

      const base = canUsePoMem ? 0.88 : 0.80;

      const baseAfterDecay = canUsePoMem
        ? decayedConfidence(base, daysSince(poMem?.lastUsedAt ?? poMem?.createdAt ?? null, now))
        : base;

      const shaped = applyResolutionMemoryToConfidence({
        db,
        vendor,
        key: resKey,
        base: baseAfterDecay,
      });
      const appliedConfidence = shaped.adjusted;

      auditTrail.push({
        step: "apply",
        timestamp: nowIso(now),
        details: `PO inference (${canUsePoMem ? "vendor_memory" : "heuristic"}) → ${suggestion.poNumber}. ${shaped.note}`,
      });

      proposedCorrections.push({
        field: "poNumber",
        from: null,
        to: suggestion.poNumber,
        source: canUsePoMem ? "vendor_memory" : "reference_heuristic",
        confidence: appliedConfidence,
        reason: suggestion.reason,
      });

      reasoningParts.push(
        canUsePoMem
          ? "Applied vendor memory (decayed): inferred PO using SKU + 30-day window."
          : "Heuristic: inferred PO using SKU + 30-day window."
      );

      confidenceScore = Math.max(confidenceScore, appliedConfidence);
    } else {
      auditTrail.push({
        step: "apply",
        timestamp: nowIso(now),
        details: "PO inference attempted but no unique/strong match found (SKU+date window).",
      });
    }
  }

  const vatMem = vendorMemsAll.find(
    (m: any) => m.kind === "vat_inclusive_pricing" && m.pattern === "mwst_inkl"
  );
  const canUseVatMem = vatMem ? canApplyVendorMem(vatMem, now) : false;

  const vatInclusiveDetected = isVatInclusiveText(rawText);
  if (vatInclusiveDetected) {
    const resKey = RES_KEYS.vatTotals;
    usedResolutionKeys.add(resKey);

    const base = canUseVatMem ? 0.85 : 0.6;
    const baseAfterDecay = canUseVatMem
      ? decayedConfidence(base, daysSince(vatMem?.lastUsedAt ?? vatMem?.createdAt ?? null, now))
      : base;

    const shaped = applyResolutionMemoryToConfidence({ db, vendor, key: resKey, base: baseAfterDecay });
    const appliedConfidence = shaped.adjusted;

    auditTrail.push({
      step: "apply",
      timestamp: nowIso(now),
      details: `Resolution memory applied for ${resKey}: ${shaped.note}`,
    });

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

      let pushedAny = false;

      if (Math.abs(currentGross - expectedGross) > 0.01) {
        proposedCorrections.push({
          field: "grossTotal",
          from: currentGross,
          to: expectedGross,
          source: canUseVatMem ? "vendor_memory" : "rawText_heuristic",
          confidence: appliedConfidence,
          reason:
            grossFromText != null
              ? `VAT-inclusive pricing detected; rawText Total=${expectedGross} used for grossTotal.`
              : `VAT-inclusive pricing detected; recomputed grossTotal from netTotal and taxRate.`,
        });
        pushedAny = true;
      }

      if (Math.abs(currentTax - expectedTax) > 0.01) {
        proposedCorrections.push({
          field: "taxTotal",
          from: currentTax,
          to: expectedTax,
          source: canUseVatMem ? "vendor_memory" : "rawText_heuristic",
          confidence: appliedConfidence,
          reason: `VAT-inclusive pricing detected; taxTotal should be grossTotal - netTotal (${expectedTax}).`,
        });
        pushedAny = true;
      }

      if (pushedAny) {
        reasoningParts.push(
          canUseVatMem
            ? "Applied vendor memory (decayed): VAT-inclusive pricing → recompute totals."
            : "Heuristic: VAT-inclusive pricing detected in rawText → propose recompute totals."
        );
        confidenceScore = Math.max(confidenceScore, appliedConfidence);
      } else {
        auditTrail.push({
          step: "apply",
          timestamp: nowIso(now),
          details:
            `VAT-inclusive pricing detected; totals verified (no correction needed). ` +
            `Expected gross=${expectedGross}, tax=${expectedTax}.`,
        });

        reasoningParts.push(
          canUseVatMem
            ? "Known vendor pattern (decayed): VAT-inclusive pricing detected; totals verified, no correction required."
            : "VAT-inclusive pricing detected; totals verified, no correction required."
        );
        confidenceScore = Math.max(confidenceScore, Math.min(0.85, appliedConfidence));
      }
    }
  }

  for (const li of extracted.fields?.lineItems ?? []) {
    if (!li.sku || !Number.isFinite(li.qty)) continue;

    const dnQty = findMatchingDeliveryQty(context, li.sku);
    if (dnQty == null || dnQty === li.qty) continue;

    const resKey = RES_KEYS.qtyToDn;
    usedResolutionKeys.add(resKey);

    const res = getResolution(db, vendor, resKey);

    const foundMem = findCorrectionMemory(db, vendor, "lineItems[].qty", "sku", li.sku);
    const canUseMem = foundMem && !isSuspectCorrectionMemory({ mem: foundMem, resolution: res, now });

    if (foundMem && !canUseMem) {
      auditTrail.push({
        step: "apply",
        timestamp: nowIso(now),
        details: `Skipped correction memory for qty as suspect/disabled/decayed (id=${foundMem.id}).`,
      });
    }

    const base = canUseMem ? Math.max(0.75, Number(foundMem.confidence ?? 0.75)) : 0.6;

    const baseAfterDecay = canUseMem
      ? decayedConfidence(base, daysSince(foundMem?.lastUsedAt ?? foundMem?.createdAt ?? null, now))
      : base;

    const shaped = applyResolutionMemoryToConfidence({ db, vendor, key: resKey, base: baseAfterDecay });
    const confidence = shaped.adjusted;

    auditTrail.push({
      step: "apply",
      timestamp: nowIso(now),
      details: `Resolution memory applied for ${resKey}: ${shaped.note}`,
    });

    proposedCorrections.push({
      field: `lineItems[sku=${li.sku}].qty`,
      from: li.qty,
      to: dnQty,
      source: canUseMem ? "correction_memory" : "reference_heuristic",
      confidence,
      reason: `Delivery note quantity (${dnQty}) differs from invoice quantity (${li.qty}) for SKU ${li.sku}.`,
    });

    if (canUseMem) {
      markUsed(db, foundMem.id);
      usedCorrectionMemoryIds.add(foundMem.id);
      reasoningParts.push(`Applied correction memory (decayed) for qty by SKU ${li.sku}.`);
    } else {
      reasoningParts.push(`Heuristic: delivery note qty differs for SKU ${li.sku}.`);
    }

    confidenceScore = Math.max(confidenceScore, confidence);
  }

  const currencyMem = vendorMemsAll.find((m: any) => m.kind === "currency_from_rawText");
  const canUseCurrencyMem = currencyMem ? canApplyVendorMem(currencyMem, now) : false;

  if (extracted.fields?.currency == null) {
    const cur = parseCurrency(rawText);

    if (cur) {
      const resKey = RES_KEYS.currency;
      usedResolutionKeys.add(resKey);

      const base = canUseCurrencyMem ? 0.85 : 0.7;
      const baseAfterDecay = canUseCurrencyMem
        ? decayedConfidence(base, daysSince(currencyMem?.lastUsedAt ?? currencyMem?.createdAt ?? null, now))
        : base;

      const shaped = applyResolutionMemoryToConfidence({ db, vendor, key: resKey, base: baseAfterDecay });
      const appliedConfidence = shaped.adjusted;

      auditTrail.push({
        step: "apply",
        timestamp: nowIso(now),
        details: `Resolution memory applied for ${resKey}: ${shaped.note}`,
      });

      proposedCorrections.push({
        field: "currency",
        from: null,
        to: cur,
        source: canUseCurrencyMem ? "vendor_memory" : "rawText_heuristic",
        confidence: appliedConfidence,
        reason: `Recovered currency from rawText (${cur}).`,
      });

      reasoningParts.push(
        canUseCurrencyMem
          ? "Applied vendor memory (decayed): currency can be recovered from rawText."
          : "Heuristic: recovered missing currency from rawText."
      );

      confidenceScore = Math.max(confidenceScore, appliedConfidence);
    } else {
      // fallback correction memory: currency by vendor default
      const resKey = RES_KEYS.currency;
      usedResolutionKeys.add(resKey);

      const res = getResolution(db, vendor, resKey);
      const mem = findCorrectionMemory(db, vendor, "currency", "vendor", "default");
      const canUseMem = mem && !isSuspectCorrectionMemory({ mem, resolution: res, now });

      if (mem && !canUseMem) {
        auditTrail.push({
          step: "apply",
          timestamp: nowIso(now),
          details: `Skipped correction memory for currency as suspect/disabled/decayed (id=${mem.id}).`,
        });
      }

      if (canUseMem) {
        const base = Math.max(0.7, Number(mem.confidence ?? 0.7));
        const baseAfterDecay = decayedConfidence(
          base,
          daysSince(mem.lastUsedAt ?? mem.createdAt ?? null, now)
        );

        const shaped = applyResolutionMemoryToConfidence({ db, vendor, key: resKey, base: baseAfterDecay });

        proposedCorrections.push({
          field: "currency",
          from: null,
          to: mem.recommendedValue,
          source: "correction_memory",
          confidence: shaped.adjusted,
          reason: "Currency missing and rawText parse failed; applied correction memory fallback.",
        });

        markUsed(db, mem.id);
        usedCorrectionMemoryIds.add(mem.id);

        auditTrail.push({
          step: "apply",
          timestamp: nowIso(now),
          details: `Applied correction memory fallback for currency (id=${mem.id}). ${shaped.note}`,
        });

        confidenceScore = Math.max(confidenceScore, shaped.adjusted);
      }
    }
  }

  const isFreightVendor = /freight\s*&\s*co/i.test(vendor);

  if (isFreightVendor) {
    if (extracted.fields?.discountTerms == null) {
      const skonto = extractSkonto(rawText);

      if (skonto) {
        const resKey = RES_KEYS.skonto;
        usedResolutionKeys.add(resKey);

        const skontoMem = vendorMemsAll.find((m: any) => m.kind === "skonto_terms");
        const canUseSkontoMem = skontoMem ? canApplyVendorMem(skontoMem, now) : false;

        const base = canUseSkontoMem ? 0.85 : 0.6;
        const baseAfterDecay = canUseSkontoMem
          ? decayedConfidence(base, daysSince(skontoMem?.lastUsedAt ?? skontoMem?.createdAt ?? null, now))
          : base;

        const shaped = applyResolutionMemoryToConfidence({ db, vendor, key: resKey, base: baseAfterDecay });
        const appliedConfidence = shaped.adjusted;

        auditTrail.push({
          step: "apply",
          timestamp: nowIso(now),
          details: `Resolution memory applied for ${resKey}: ${shaped.note}`,
        });

        proposedCorrections.push({
          field: "discountTerms",
          from: null,
          to: skonto,
          source: canUseSkontoMem ? "vendor_memory" : "rawText_heuristic",
          confidence: appliedConfidence,
          reason: `Detected ${skonto.percent}% Skonto if paid within ${skonto.days} days.`,
        });

        reasoningParts.push(
          canUseSkontoMem
            ? "Applied vendor memory (decayed): Freight & Co uses Skonto terms."
            : "Heuristic: detected Skonto payment terms in rawText."
        );

        confidenceScore = Math.max(confidenceScore, appliedConfidence);
      } else {
        const resKey = RES_KEYS.skonto;
        usedResolutionKeys.add(resKey);

        const res = getResolution(db, vendor, resKey);
        const mem = findCorrectionMemory(db, vendor, "discountTerms", "vendor", "default");
        const canUseMem = mem && !isSuspectCorrectionMemory({ mem, resolution: res, now });

        if (mem && !canUseMem) {
          auditTrail.push({
            step: "apply",
            timestamp: nowIso(now),
            details: `Skipped correction memory for discountTerms as suspect/disabled/decayed (id=${mem.id}).`,
          });
        }

        if (canUseMem) {
          usedResolutionKeys.add(resKey);
          const base = Math.max(0.7, Number(mem.confidence ?? 0.7));
          const baseAfterDecay = decayedConfidence(
            base,
            daysSince(mem.lastUsedAt ?? mem.createdAt ?? null, now)
          );
          const shaped = applyResolutionMemoryToConfidence({ db, vendor, key: resKey, base: baseAfterDecay });

          const parsed = (() => {
            try {
              return JSON.parse(mem.recommendedValue);
            } catch {
              return mem.recommendedValue;
            }
          })();

          proposedCorrections.push({
            field: "discountTerms",
            from: null,
            to: parsed,
            source: "correction_memory",
            confidence: shaped.adjusted,
            reason: "Skonto missing and rawText extraction failed; applied correction memory fallback.",
          });

          markUsed(db, mem.id);
          usedCorrectionMemoryIds.add(mem.id);

          auditTrail.push({
            step: "apply",
            timestamp: nowIso(now),
            details: `Applied correction memory fallback for discountTerms (id=${mem.id}). ${shaped.note}`,
          });

          confidenceScore = Math.max(confidenceScore, shaped.adjusted);
        }
      }
    }

    const freightItem = extracted.fields?.lineItems?.find(
      (li: any) => li.sku == null && /shipping|seefracht|transport/i.test(li.description ?? "")
    );

    if (freightItem) {
      const resKey = RES_KEYS.freightSku;
      usedResolutionKeys.add(resKey);

      const res = getResolution(db, vendor, resKey);

      const mem = findCorrectionMemory(db, vendor, "lineItems[].sku", "vendor", "freight_desc_missing_sku");
      const canUseMem = mem && !isSuspectCorrectionMemory({ mem, resolution: res, now });

      const skuVendorMem = vendorMemsAll.find((m: any) => m.kind === "sku_mapping" && m.pattern === "FREIGHT");
      const canUseSkuVendorMem = skuVendorMem ? canApplyVendorMem(skuVendorMem, now) : false;

      const pickedSku = canUseMem ? mem.recommendedValue : "FREIGHT";

      const base = canUseMem ? Math.max(0.7, Number(mem.confidence ?? 0.7)) : canUseSkuVendorMem ? 0.85 : 0.6;

      const baseAfterDecay = canUseMem
        ? decayedConfidence(base, daysSince(mem.lastUsedAt ?? mem.createdAt ?? null, now))
        : canUseSkuVendorMem
        ? decayedConfidence(base, daysSince(skuVendorMem?.lastUsedAt ?? skuVendorMem?.createdAt ?? null, now))
        : base;

      const shaped = applyResolutionMemoryToConfidence({ db, vendor, key: resKey, base: baseAfterDecay });
      const appliedConfidence = shaped.adjusted;

      auditTrail.push({
        step: "apply",
        timestamp: nowIso(now),
        details: `Resolution memory applied for ${resKey}: ${shaped.note}`,
      });

      proposedCorrections.push({
        field: "lineItems[].sku",
        from: null,
        to: pickedSku,
        source: canUseMem ? "correction_memory" : canUseSkuVendorMem ? "vendor_memory" : "rawText_heuristic",
        confidence: appliedConfidence,
        reason: canUseMem
          ? "Applied correction memory freight SKU mapping."
          : "Freight-related description mapped to SKU FREIGHT.",
      });

      if (canUseMem) {
        markUsed(db, mem.id);
        usedCorrectionMemoryIds.add(mem.id);
      }

      reasoningParts.push(
        canUseMem
          ? "Applied correction memory (decayed): freight descriptions map to a learned SKU."
          : canUseSkuVendorMem
          ? "Applied vendor memory (decayed): Freight descriptions map to SKU FREIGHT."
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

  const REVIEW_THRESHOLD = 0.75;

  const normalized = extracted.fields ?? extracted.normalizedInvoice ?? {};

  const anyLowConfidence =
    proposedCorrections.length > 0 &&
    proposedCorrections.some((c: any) => (c.confidence ?? 0) < REVIEW_THRESHOLD);

  const criticalMissing = hasCriticalMissingFields(normalized, proposedCorrections);

  const requiresHumanReview = anyLowConfidence || criticalMissing;

  auditTrail.push({
    step: "decide",
    timestamp: nowIso(now),
    details:
      proposedCorrections.length === 0
        ? criticalMissing
          ? "Escalated: no corrections proposed but critical fields are missing."
          : "No corrections proposed; auto-accept."
        : requiresHumanReview
        ? anyLowConfidence
          ? "Escalated: at least one correction below confidence threshold."
          : "Escalated: critical fields missing."
        : "Auto-correct possible: all corrections high confidence.",
  });

  if (proposedCorrections.length > 0) {
    const minC = Math.min(...proposedCorrections.map((c: any) => Number(c.confidence ?? 0)));
    confidenceScore = round2(minC);
  }

  if (hasLearned(db, invoiceId)) {
    auditTrail.push({
      step: "learn",
      timestamp: nowIso(now),
      details: `Skipped learning: invoiceId ${invoiceId} already learned.`,
    });
  } else {
    const decisionsForInvoice = context.correctionsForInvoice ?? [];
    const decisionRow = decisionsForInvoice.find(
      (x: any) => x.finalDecision === "approved" || x.finalDecision === "rejected"
    );

    const finalDecision = decisionRow?.finalDecision as "approved" | "rejected" | undefined;

    if (finalDecision) {
      if (finalDecision === "rejected" && usedCorrectionMemoryIds.size > 0) {
        for (const memId of usedCorrectionMemoryIds) {
          const out = markRejected(db, memId);
          auditTrail.push({
            step: "learn",
            timestamp: nowIso(now),
            details: `Correction memory rejected: ${memId} (rejectCount=${out.rejectCount}, status=${out.status ?? "n/a"}).`,
          });

          logAuditEvent(db, {
            eventType: "CORRECTION_MEMORY_REJECTED",
            vendor,
            invoiceId,
            meta: { id: memId, rejectCount: out.rejectCount, status: out.status },
            now,
          });
        }
      }

      if (finalDecision === "rejected") {
        const maybeUsedVendorMemIds: string[] = [];
        for (const vmId of maybeUsedVendorMemIds) {
          try {
            const out = markVendorRejected(db, vmId);
            auditTrail.push({
              step: "learn",
              timestamp: nowIso(now),
              details: `Vendor memory rejected: ${vmId} (rejectCount=${out.rejectCount}, status=${out.status ?? "n/a"}).`,
            });
          } catch {}
        }
      }

      if (finalDecision === "approved") {
        const serviceFix = decisionRow.corrections?.find((c: any) => c.field === "serviceDate");
        if (serviceFix) {
          const id = `${vendor}::serviceDate_from_label::Leistungsdatum`;
          const existing = vendorMemsAll.find((m: any) => m.id === id);
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

        const poFix = decisionRow.corrections?.find((c: any) => c.field === "poNumber");
        if (poFix) {
          const id = `${vendor}::po_match_strategy::sku_and_date_window`;
          const existing = vendorMemsAll.find((m: any) => m.id === id);
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

        const touchesTotals = (decisionRow.corrections ?? []).some(
          (c: any) => c.field === "taxTotal" || c.field === "grossTotal"
        );

        if (touchesTotals && isVatInclusiveText(rawText)) {
          const id = `${vendor}::vat_inclusive_pricing::mwst_inkl`;
          const existing = vendorMemsAll.find((m: any) => m.id === id);
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

        const currencyFix = decisionRow.corrections?.find((c: any) => c.field === "currency");
        if (currencyFix) {
          const id = `${vendor}::currency_from_rawText::default`;
          const existing = vendorMemsAll.find((m: any) => m.id === id);
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

        const qtyFixes = (decisionRow.corrections ?? []).filter(
          (c: any) => typeof c.field === "string" && c.field.includes(".qty")
        );

        for (const fix of qtyFixes) {
          const skuMatch = String(fix.field).match(/sku=([A-Z0-9\-]+)/i);
          if (!skuMatch) continue;

          const sku = skuMatch[1];
          const id = `${vendor}::lineItems[].qty::sku::${sku}`;

          const result = upsertCorrectionMemory(db, {
            id,
            vendor,
            fieldPath: "lineItems[].qty",
            patternType: "sku",
            patternValue: sku,
            recommendedValue: String(fix.to),
            confidence: 0.7,
            lastUsedAt: nowIso(now),
            createdAt: nowIso(now),
            status: "active",
          });

          memoryUpdates.push({ type: "correction_memory_upsert", id, confidence: result.confidence });
        }
        const skontoFix = decisionRow.corrections?.find((c: any) => c.field === "discountTerms");
        if (skontoFix) {
          const id = `${vendor}::skonto_terms::default`;
          const existing = vendorMemsAll.find((m: any) => m.id === id);
          const nextConfidence = Math.min(0.95, (existing?.confidence ?? 0.6) + 0.1);

          upsertVendorMemory(db, {
            id,
            vendor,
            kind: "skonto_terms",
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
            note: "Learned skonto terms behavior.",
          });
        }
        const freightSkuFix = (decisionRow.corrections ?? []).find(
          (c: any) => c.field === "lineItems[].sku" && String(c.to).toUpperCase() === "FREIGHT"
        );

        if (freightSkuFix) {
          const id = `${vendor}::sku_mapping::FREIGHT`;
          const existing = vendorMemsAll.find((m: any) => m.id === id);
          const nextConfidence = Math.min(0.95, (existing?.confidence ?? 0.6) + 0.1);

          upsertVendorMemory(db, {
            id,
            vendor,
            kind: "sku_mapping",
            pattern: "FREIGHT",
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
            note: "Learned freight description → SKU FREIGHT mapping.",
          });
        }
      }

      if (usedResolutionKeys.size > 0) {
        for (const key of usedResolutionKeys) {
          const out = recordResolutionDecision(db, {
            vendor,
            key,
            decision: finalDecision,
            invoiceId,
          });

          logAuditEvent(db, {
            eventType: finalDecision === "approved" ? "RESOLUTION_APPROVED" : "RESOLUTION_REJECTED",
            vendor,
            invoiceId,
            meta: {
              key,
              approved: out.value.approved,
              rejected: out.value.rejected,
              lastDecision: out.value.lastDecision,
              lastInvoiceId: out.value.lastInvoiceId,
            },
            now,
          });

          auditTrail.push({
            step: "learn",
            timestamp: nowIso(now),
            details:
              finalDecision === "approved"
                ? `Resolution memory: ${key} recorded APPROVED (approved=${out.value.approved}, rejected=${out.value.rejected}).`
                : `Resolution memory: ${key} recorded REJECTED (approved=${out.value.approved}, rejected=${out.value.rejected}).`,
          });
        }
      }

      markLearned(db, invoiceId);

      logAuditEvent(db, {
        eventType: finalDecision === "approved" ? "LEARN_APPROVED" : "LEARN_REJECTED",
        vendor,
        invoiceId,
        meta: { updates: memoryUpdates.map((u: any) => u.id).filter(Boolean) },
        now,
      });

      auditTrail.push({
        step: "learn",
        timestamp: nowIso(now),
        details:
          finalDecision === "approved"
            ? `Learning applied from APPROVED corrections for invoiceId ${invoiceId}.`
            : `Recorded REJECTED decision for invoiceId ${invoiceId} (no vendor/correction learning applied).`,
      });
    } else {
      auditTrail.push({
        step: "learn",
        timestamp: nowIso(now),
        details: "No human decision available for learning.",
      });
    }
  }

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