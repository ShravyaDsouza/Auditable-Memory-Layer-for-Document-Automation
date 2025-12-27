#!/bin/sh
set -e

DATASET="${DATASET:-full}"

echo "=== Running expected outcomes using dataset: $DATASET ==="

echo "=== Supplier GmbH ==="
npm run demo -- --dataset "$DATASET" --invoiceId INV-A-001
npm run demo -- --dataset "$DATASET" --invoiceId INV-A-002
npm run demo -- --dataset "$DATASET" --invoiceId INV-A-003
npm run demo -- --dataset "$DATASET" --invoiceId INV-A-004

echo "=== Parts AG ==="
npm run demo -- --dataset "$DATASET" --invoiceId INV-B-001
npm run demo -- --dataset "$DATASET" --invoiceId INV-B-002
npm run demo -- --dataset "$DATASET" --invoiceId INV-B-003
npm run demo -- --dataset "$DATASET" --invoiceId INV-B-004

echo "=== Freight & Co ==="
npm run demo -- --dataset "$DATASET" --invoiceId INV-C-001
npm run demo -- --dataset "$DATASET" --invoiceId INV-C-002
npm run demo -- --dataset "$DATASET" --invoiceId INV-C-003
npm run demo -- --dataset "$DATASET" --invoiceId INV-C-004

echo "=== DONE ==="
