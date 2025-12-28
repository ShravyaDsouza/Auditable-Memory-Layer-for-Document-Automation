# Auditable Memory Layer for Document Automation
### Memory-Driven Invoice Processing Pipeline

This project implements a **memory-augmented invoice processing pipeline** that improves accuracy over time by learning vendor-specific patterns from prior invoices and human approvals. 

Instead of relying solely on static rules, the system recalls past decisions, applies learned strategies, decides whether to auto-correct or escalate, and learns from outcomes—all while safely handling duplicate invoices to prevent data corruption.

---

## Core Design

### High-Level Flow
1. **Input Invoice:** Raw data enters the pipeline.
2. **Recall Memories:** System fetches Vendor, Correction, and Resolution histories.
3. **Apply Heuristics + Learned Patterns:** Combines hardcoded rules with historical insights.
4. **Decision:** Determines if the confidence is high enough for **Auto-correction** or requires **Human Review**.
5. **Learn:** Approved outcomes update the memory stores.
6. **Audit:** Produces a structured JSON output with a step-by-step trace of all logic.

### Memory Types
* **Vendor Memory:** Captures behavioral patterns (e.g., Supplier GmbH uses "Leistungsdatum" as the service date).
* **Resolution Memory:** Tracks the success rate of specific strategies to increase or decrease confidence over time.
* **Correction Memory:** Tracks repeated field-level fixes (e.g., currency recovery or SKU mapping).

---

## Duplicate Guard
To ensure memory integrity, invoices are flagged as duplicates using Vendor ID + Invoice Number + raw-text fingerprinting
* Duplicates are **escalated** for review.
* Duplicates **do not** create or modify memory to prevent contradictory learning.

---

## How to Run

### Local Setup
1. **Install dependencies:**
   ```bash
   npm ci
   ```
2. **Run a single invoice:**
   ```bash
   npm run demo -- --dataset full --invoiceId INV-A-003
   ```
3. **Run the full test suite:**
   ```bash
   npm run demo:all
   ```
### Docker
1. **Build the image:**
   ```bash
    docker build -t mem_layer .
   ```
2. **Run the full demo:**
   ```bash
    docker run --rm mem_layer
   ```

---

## Repository Structure
.
├── Dockerfile
├── README.md
├── data/                       # Datasets and reference manifests
├── docker-compose.yml
├── package.json
├── src/
│   ├── adapters/               # Data loaders (Invoices, Corrections, etc.)
│   ├── admin/                  # CLI tools for memory management
│   ├── db/                     # SQLite schema and memory persistence
│   ├── engine/                 # Core logic (Pipeline, Duplicate Guard, Decay)
│   ├── demo/                   # Demo runner scripts
│   ├── scripts/                # Shell scripts for automation
│   ├── types/                  # TypeScript definitions
│   └── utils/                  # Shared helpers
└── tsconfig.json

---

## Output Format
Each run produces a JSON response structured for auditability:
| Field               | Description                                      |
|--------------------|--------------------------------------------------|
| normalizedInvoice  | Final processed invoice data                     |
| proposedCorrections| List of adjustments made                         |
| requiresHumanReview| Boolean flag based on confidence                 |
| confidenceScore    | Numerical value (0.0 – 1.0)                      |
| memoryUpdates      | What the system learned from this run            |
| auditTrail         | Step-by-step trace for explainability            |

---

## Demo Runner Scripts

* ```bash
  src/scripts/expected.sh
  ```
  : Runs all invoices required to demonstrate the grading criteria

* ```bash
  src/scripts/bonus.sh
  ```
  : Demonstrates extended scenarios

---

## Learning Behavior (Concrete Examples)
* Supplier GmbH
- INV-A-001: Learns that Leistungsdatum corresponds to serviceDate.
- INV-A-002: Automatically fills serviceDate using vendor memory.
- INV-A-003: Auto-suggests PO-A-051 using SKU + 30-day window (single matching PO → auto-correct).
- INV-A-004: Flagged as duplicate → no learning applied.

* Parts AG
- INV-B-001: Detects VAT-inclusive pricing ("MwSt. inkl.") and recomputes totals. Learns vendor VAT behavior.
- INV-B-002: Recognizes VAT-inclusive pattern → verifies totals without correction.
- INV-B-003: Recovers missing currency (EUR) from rawText and learns behavior.
- INV-B-004: Detected as duplicate → no memory update.

* Freight & Co
- INV-C-001: Detects Skonto terms and freight-only line items; stores structured memory.
- INV-C-002: Recognizes freight mapping but still escalates.
- INV-C-003 / INV-C-004: Freight SKU mapping confidence increases → auto-corrected.

--- 

## Troubleshooting
* npm ci fails: Ensure you are using Node ≥ 18.
* Permission denied: If scripts fail to run, use chmod +x src/scripts/*.sh.
* Docker build: Native SQLite bindings require a brief compilation step in the Alpine image.

---

## Assumptions & Limitations

1. Confidence thresholds are heuristic-based
2. Memory decay is optional and non-essential to correctness
3. Designed for clarity and auditability, not throughput
4. SQLite chosen for simplicity and portability

---

## Decision Logic

* Corrections with high confidence are auto-applied
* Any low-confidence correction triggers human review
* Learning occurs only from approved outcomes
* Duplicate invoices are fully isolated from learning
