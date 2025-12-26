import { openDb } from "../db/sqlite.js";
import { migrate } from "../db/migrations.js";
import { getVendorMemories } from "../db/vendorMemory.js";
import { getCorrectionMemories } from "../db/correctionMemory.js";
import { getAllResolutionsForVendor } from "../db/resolutionMemory.js";
import { daysSince, decayedConfidence } from "../engine/confidenceDecay.js";

export function adminList(vendor: string, simulateDays = 0) {
  const repoRoot = process.cwd();
  const db = openDb(repoRoot);
  migrate(db);

  const now = new Date(Date.now() + simulateDays * 86400000);

  const vms = getVendorMemories(db, vendor);
  const cms = getCorrectionMemories(db, vendor);
  const rms = getAllResolutionsForVendor(db, vendor);

  console.log(`\n== Vendor Memories (${vendor}) ==`);
  if (vms.length === 0) console.log("(none)");
  for (const m of vms) {
    const d = daysSince(m.lastUsedAt, now);
    const eff = decayedConfidence(Number(m.confidence ?? 0), d, 30);
    console.log({
      id: m.id,
      kind: m.kind,
      pattern: m.pattern,
      baseConfidence: m.confidence,
      lastUsedAt: m.lastUsedAt,
      effectiveConfidence: eff,
      supportCount: m.supportCount,
      rejectCount: m.rejectCount,
      status: m.status,
    });
  }

  console.log(`\n== Correction Memories (${vendor}) ==`);
  if (cms.length === 0) console.log("(none)");
  for (const m of cms) {
    const d = daysSince(m.lastUsedAt, now);
    const eff = decayedConfidence(Number(m.confidence ?? 0), d, 30);
    console.log({
      id: m.id,
      fieldPath: m.fieldPath,
      patternType: m.patternType,
      patternValue: m.patternValue,
      baseConfidence: m.confidence,
      lastUsedAt: m.lastUsedAt,
      effectiveConfidence: eff,
      supportCount: m.supportCount,
      rejectCount: m.rejectCount,
      status: m.status ?? "(no status col)",
    });
  }

  console.log(`\n== Resolution Memory (${vendor}) ==`);
  if (rms.length === 0) console.log("(none)");
  for (const r of rms) {
    // If you add decay to resolution confidence too, compute eff here similarly
    console.log({
      key: r.key,
      value: r.value,
      confidence: r.confidence,
      lastUsedAt: r.lastUsedAt,
      status: r.status,
    });
  }
}
