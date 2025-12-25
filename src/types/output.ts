export type AuditStep = "recall" | "apply" | "decide" | "learn";

export type AuditTrailEntry = {
  step: AuditStep;
  timestamp: string;
  details: string;
};

export type AgentOutput = {
  normalizedInvoice: Record<string, unknown>;
  proposedCorrections: Array<Record<string, unknown>>;
  requiresHumanReview: boolean;
  reasoning: string;
  confidenceScore: number;
  memoryUpdates: Array<Record<string, unknown>>;
  auditTrail: AuditTrailEntry[];
};
