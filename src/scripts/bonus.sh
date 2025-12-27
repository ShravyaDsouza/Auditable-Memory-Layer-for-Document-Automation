#!/bin/sh
set -e

DATASET="${DATASET:-full}"
VENDOR="Supplier GmbH"

echo "=== BONUS: Simulate Days Passing ==="
echo "Dataset: $DATASET"
echo "Vendor: $VENDOR"

snapshot() {
  DAYS="$1"
  echo ""
  echo "Admin snapshot (simulateDays=$DAYS)"
  npm run admin:list -- --vendor "$VENDOR" --simulateDays "$DAYS" || true
}

echo ""
echo "=== Day 0 — Initial learning ==="
npm run demo -- --dataset "$DATASET" --invoiceId INV-A-001
npm run demo -- --dataset "$DATASET" --invoiceId INV-A-002
snapshot 0

echo ""
echo "=== +10 days — Memory still valid ==="
npm run demo -- --dataset "$DATASET" --invoiceId INV-A-003 --simulateDays 10
snapshot 10

echo ""
echo "=== +40 days — Memory decayed (may escalate) ==="
npm run demo -- --dataset "$DATASET" --invoiceId INV-A-003 --simulateDays 40
snapshot 40

echo ""
echo "=== +41 days — Human correction refreshes memory ==="
npm run demo -- --dataset "$DATASET" --invoiceId INV-A-003 --simulateDays 41
snapshot 41

echo ""
echo "=== +90 days — Old memory mostly expired ==="
npm run demo -- --dataset "$DATASET" --invoiceId INV-A-003 --simulateDays 90
snapshot 90

echo ""
echo "=== BONUS DONE ==="