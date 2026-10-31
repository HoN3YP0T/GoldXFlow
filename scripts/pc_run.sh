#!/bin/bash
# Run one goldflow step on a restricted mount (e.g. a Windows folder shared into a Linux VM).
#   scripts/pc_run.sh <seconds> <goldflow cli args...>
# Working SQLite DB lives on local disk ($HOME/gf_state); a consistent snapshot is saved to data/state after
# every step. Steps are resumable, so a step stopped by the time limit just continues on the next call.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL="${GOLDFLOW_LOCAL_STATE:-$HOME/gf_state}/acquisition.sqlite"
SNAP="$ROOT/data/state/acquisition.sqlite"
export GOLDFLOW_STATE_DB="$LOCAL" GOLDFLOW_SQLITE_JOURNAL=WAL
mkdir -p "$(dirname "$LOCAL")"
if [ ! -f "$LOCAL" ] && [ -f "$SNAP" ]; then python3 "$ROOT/scripts/snapshot.py" restore "$SNAP" "$LOCAL"; fi
cd "$ROOT"; T="$1"; shift
timeout --signal=INT --kill-after=10 "$T" python3 -m goldflow.cli "$@"; rc=$?
if [ "${GOLDFLOW_SNAPSHOT:-1}" = "1" ]; then python3 "$ROOT/scripts/snapshot.py" save "$LOCAL" "$SNAP"; fi
echo "goldflow exit code: $rc"
exit $rc
