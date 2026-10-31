#!/bin/bash
# One command for all quality gates: lint, format (Phase 1+ modules), types, tests.
set -euo pipefail
cd "$(dirname "$0")/.."
NEW="goldflow/settings.py goldflow/profiles.py goldflow/observability.py goldflow/migrations.py goldflow/providers tests/test_foundation.py
     goldflow/liquidity tests/test_phase8.py tests/depth_sim.py scripts/verify_depth_reference.py goldflow/orderflow"
echo "== ruff check";        ruff check goldflow tests scripts
echo "== ruff format --check (Phase 1+ modules)"; ruff format --check $NEW
echo "== mypy";              mypy --python-executable "$(command -v python3)"
echo "== pytest";            python3 -m pytest -q -p no:cacheprovider tests
echo "ALL CHECKS PASSED"
