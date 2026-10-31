# Phase 1 completion report: project foundation and modular architecture

Date: 2026-10-31. Built after Phases 2–4, as a non-breaking foundation layer. Verified in the cloud workspace, which holds the full dataset.

## 1. Scope completed / not completed
**Completed**
- `pyproject.toml` (pinned compatible ranges, `.[dev]` extras, `goldflow` console script).
- Validated settings with cross-field rules and spec-drift detection.
- Run profiles with a live-trading lock.
- Provider contract with capability declarations; registry; Binance adapter (wrapping the Phase 2 code, no behaviour change); deterministic dummy provider refused outside tests.
- Structured JSON logging with run, event and correlation ids.
- Metrics registry, wired into REST and replay.
- Versioned, checksummed DB migrations and the Parquet schema-version policy.
- Ruff, formatting and mypy `--strict` gates, plus `scripts/check.sh`.
- New CLI commands `status` and `providers`.
- ADR-0002.

**Not completed / deferred by design**
- Live profile usability (Phase 16).
- Metrics export to a dashboard (Phase 17).
- PostgreSQL migration (ADR-0001 trigger not reached).
- Style-only cleanup of the four `code_version` modules (see §7).

## 2. Files
**Created**
- `pyproject.toml`
- `goldflow/settings.py`, `goldflow/profiles.py`, `goldflow/observability.py`, `goldflow/migrations.py`
- `goldflow/providers/{__init__,base,registry}.py`, `goldflow/providers/binance/{__init__,adapter}.py`, `goldflow/providers/dummy/__init__.py`
- `scripts/check.sh`
- `tests/test_foundation.py`
- `docs/ADR-0002-foundation.md`, `docs/PHASE1_COMPLETION_REPORT.md`

**Modified**
- `goldflow/config.py`: validation on load; `settings` and `config_hash`.
- `goldflow/cli.py`:
  - `--profile`, validation and live lock at startup (exit 2), structured logging, migrations at startup;
  - new commands `status` and `providers`;
  - `discover` goes through the provider adapter.
- `goldflow/rest_client.py`, `goldflow/replay.py`: metrics.
- Unused-import cleanup (no logic change) in `acquire.py`, `report.py`, `dataset_report.py`, `vision_archive.py`, `cli.py` and the tests.
- `README.md`, `docs/BUILD_GUIDE.md` (status).

**Unchanged, byte-for-byte:** `normalize.py`, `schemas.py`, `integrity.py`, `catalog.py`. These modules define the Phase 3 `code_version`, which is still `703a9b89…` and matches the PC's catalog, so no re-normalisation is needed.

**Deleted:** none.

## 3. Architecture
- **Startup:** CLI → `resolve_profile` → `Config.load` (pydantic validation and cross-field rules, plus a spec-drift check against `instrument_discovery.json`) → `assert_profile_allowed` (live lock) → `setup_logging` (JSON file plus console) → `apply_migrations` → command.
- **Data access:** through `providers.create_provider(cfg, profile)`, which enforces the synthetic-only-in-test rule. Capabilities are declared per data type and enforced with `require()`.
- **Config hash:** new config blocks have defaults in code, so the config file and its hash (`f0b71abc…`) are unchanged.

## 4. Commands
```
pip install -e ".[dev]"
goldflow status [--profile research|test|live]
goldflow providers
scripts/check.sh
python -m goldflow.cli validate      # regression: Phase 3 digest
python -m goldflow.cli replay --name r   # regression: replay digest
```

## 5. Tests and quality gates
- **pytest: 56 passed, 0 failed.** Phase 2: 4; Phase 3: 15; Phase 4: 15; Phase 1: 22 with parametrised cases.
- **Phase 1 tests cover:**
  - the repo config is valid and its hash stable;
  - four field errors reported together;
  - seven conflicting-setting cases rejected;
  - spec-drift rejection;
  - profile resolution;
  - live lock (both conditions, approval fields);
  - CLI `status` validates and reports non-trading mode with structured logs;
  - CLI refuses an invalid config and the live profile (exit 2);
  - CLI refuses a synthetic provider outside test;
  - registry rules and duplicate-id refusal;
  - dummy determinism and raw schema;
  - Binance adapter contract offline (spec mapping, capabilities, L2 refusal, only published days listed, geo-block mapped to `ProviderUnavailable`, health);
  - abstract contract;
  - JSON log structure;
  - metrics;
  - migrations (apply once, idempotent, tamper detection, unknown-version detection, gap detection).
- **ruff:** all checks passed. **ruff format:** 11 Phase 1+ files formatted. **mypy --strict:** no issues in 10 foundation source files.
- **Fresh environment:** a new venv with `pip install -e ".[dev]"` succeeded (pyarrow 26.0.0, pydantic 2.14.2); `goldflow status` reported VALID / DISABLED; 56 tests passed in the venv.

## 6. Regression evidence (no results changed)

| Check | Before Phase 1 | After Phase 1 |
|---|---|---|
| Phase 3 `validate` result digest (full recomputation of 912 partitions) | `7eb02d13…f704` | `7eb02d13…f704`, identical = 1, 0 partition differences |
| Full replay sequence digest (155,997,644 events, 0 violations) | `eeacba10…2d50` | `eeacba10…2d50` |
| Replay throughput, warm cache | 2.80 M ev/s (55.8 s) | 3.14 M ev/s (49.7 s). A cold-cache run straight after validation took 217.9 s because of disk reads, not code. |
| Config hash | `f0b71abc…` | `f0b71abc…` |
| Phase 3 `code_version` | `703a9b89…` | `703a9b89…` |

## 7. Known limitations
- The four `code_version` modules still contain unused imports and locals, exempted in `pyproject.toml` and to be cleaned when they next change for a real reason.
- Style rules (one statement per line, import order) are enforced only on Phase 1+ modules. Older modules are checked for real errors only.
- The mypy analysis target is 3.12 (numpy stubs); runtime supports 3.10+ (the PC runs 3.10).
- Metrics are in-process only until Phase 17.
- The Binance adapter's `historical_trades()` streams verified daily files through a cache. Bulk acquisition should still use `acquire`, which is catalogued and resumable.
- The PC copy isn't updated yet: the PC wasn't linked during this phase. Run `git pull` in `Documents\python\goldflow` (or re-link and I'll apply it). No data reprocessing is needed.

## 8. Security and operations
- Secrets must be referenced by environment-variable name. The validator rejects a value that looks like a key in `api_key_env`.
- Live trading is locked by default behind two independent conditions.
- Logs contain no credentials.

## 9. Acceptance criteria

| Criterion | Result | Evidence |
|---|---|---|
| Fresh environment setup succeeds from README instructions | **PASS** | New venv, `pip install -e ".[dev]"`, `goldflow status` VALID, 56 tests pass |
| Application validates configuration and starts in non-trading mode | **PASS** | `status`: config VALID, trading mode DISABLED; invalid config exits 2 with all problems listed; live profile refused |
| Unit tests pass; secrets not present in repository | **PASS** | 56/56; `.gitignore` plus `.env.example`; credential names only |
| A dummy test adapter can exercise the provider contract without being used as production data | **PASS** | Dummy deterministic in `test`; `ProviderNotAllowed` in research and live (unit and CLI tests) |
| Phase 2–4 behaviour unchanged | **PASS** | Both digests reproduced; config hash and `code_version` unchanged |

## 10. Recommended next phase
**Phase 5: trade processing, classification and price-level aggregation** (see `docs/BUILD_GUIDE.md`). Not started.
