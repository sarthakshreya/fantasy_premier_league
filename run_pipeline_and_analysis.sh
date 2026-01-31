#!/usr/bin/env bash
# Run full FPL pipeline: extract raw → transform → last3/next3 analysis.
# Run from project root. Requires: python, pip install -r requirements.txt

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== 1/2 Pipeline (raw + transform) ==="
python src/fpl_weekly/orchestrate.py

echo ""
echo "=== 2/2 Analysis (last3/next3 shortlists) ==="
if [ ! -d "fpl_dump/transformed" ]; then
  echo "Error: fpl_dump/transformed not found after pipeline run." >&2
  exit 1
fi

python src/fpl_weekly/fpl_last3_next3_analysis.py \
  --in fpl_dump/transformed \
  --out ./fpl_analysis

echo ""
echo "Done. Outputs:"
echo "  - Raw/transformed: fpl_dump/"
echo "  - Analysis CSVs:   ./fpl_analysis/"
