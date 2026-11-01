#!/bin/bash
# GoldXFlow: download the Binance XAUUSDT history into this project's data/ folder (and nothing else).
# Already complete -> does nothing (no network traffic). Interrupted -> resumes, never re-downloading verified files.
# A US host cannot reach Binance REST (HTTP 451): discover is skipped there and the archives download without REST.
set -uo pipefail
cd "$(dirname "$0")/.."
G="python3 -m goldflow.cli"
if $G data-status >/dev/null 2>&1; then echo "The history is already downloaded and verified. Nothing to do."; $G data-status; exit 0; fi
$G discover >/dev/null 2>&1 || echo "(discover skipped: REST not reachable from this host)"
REST=""; curl -s -o /dev/null -w "%{http_code}" https://fapi.binance.com/fapi/v1/ping 2>/dev/null | grep -q 451 && REST="--no-rest"
$G acquire $REST || { echo "Download not complete yet: run this script again (it resumes)."; exit 1; }
$G data-status || { echo "Download not complete yet: run this script again (it resumes)."; exit 1; }
echo "Download complete. Next: scripts/run_all.sh (it never downloads)."
