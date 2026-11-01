#!/bin/bash
# GoldXFlow (Linux/macOS): process the downloaded history. NEVER downloads (use scripts/download_data.sh first) and never
# changes the downloaded data. Safe to re-run: normalize does only new days; validate and pipeline skip when up to date.
set -uo pipefail
cd "$(dirname "$0")/.."
G="python3 -m goldflow.cli"
if ! $G data-status >/dev/null 2>&1; then $G data-status; echo "Run scripts/download_data.sh first, then this script again."; exit 1; fi
step() { echo "== $*"; "$@"; rc=$?; if [ $rc -ne 0 ] && [ $rc -ne 3 ]; then echo "step failed (exit $rc): $*"; exit $rc; fi; }
step $G normalize
step $G validate --if-changed
step $G pipeline --name main --if-changed   # Phases 5-11 in one replay + every verifier (data/reports/pipeline_main.json)
$G providers-report --no-probe
$G ops-check
echo "Done. Dashboard: python3 -m goldflow.cli dashboard"
