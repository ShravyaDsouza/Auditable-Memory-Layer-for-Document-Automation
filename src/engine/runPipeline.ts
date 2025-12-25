import type { AgentOutput } from "../types/output.js";

export function runPipeline(context: Record<string, unknown>): AgentOutput {
  const now = new Date().toISOString();

  return {
    normalizedInvoice: context["extracted"] as Record<string, unknown>, // temporary
    proposedCorrections: [],
    requiresHumanReview: true,
    reasoning: "Stub: pipeline not implemented yet. Escalating by default.",
    confidenceScore: 0.2,
    memoryUpdates: [],
    auditTrail: [
      { step: "recall", timestamp: now, details: "Stub recall: no memory used yet." },
      { step: "apply", timestamp: now, details: "Stub apply: no corrections applied." },
      { step: "decide", timestamp: now, details: "Stub decide: requiresHumanReview=true." },
      { step: "learn", timestamp: now, details: "Stub learn: no memory updates." }
    ]
  };
}
