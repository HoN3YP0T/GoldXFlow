#!/bin/bash
# GoldXFlow end to end (Linux/macOS). Every step is resumable; re-running continues where it stopped.
#   scripts/run_all.sh [--start ISO --end ISO]     history + Phases 5-11 + verification + reports
# A US host cannot reach fapi.binance.com (HTTP 451): discover is skipped there and acquisition runs without REST.
set -uo pipefail
cd "$(dirname "$0")/.."
G="python3 -m goldflow.cli"
RANGE=("$@")
step() { echo "== $*"; "$@"; rc=$?; if [ $rc -ne 0 ] && [ $rc -ne 3 ]; then echo "step failed (exit $rc): $*"; exit $rc; fi; }
$G discover >/dev/null 2>&1 || echo "(discover skipped: REST not reachable from this host)"
step $G acquire "${RANGE[@]}" ${RANGE[@]:+--new-range} --no-rest
step $G normalize
step $G validate
step $G pipeline --name main            # Phases 5-11 in one replay, then every verifier (data/reports/pipeline_main.json)
$G providers-report --no-probe
$G ops-check
echo "Done. Next: 'python3 -m goldflow.cli dashboard', venue-import/venue-run, parity, live-run on a non-US host."
