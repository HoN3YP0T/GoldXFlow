# ADR-0002: Phase 1 foundation (provider contract, settings, profiles, observability, migrations)

**Status:** accepted (2026-10-31).

## Context
Phases 2–4 were built before the foundation phase. They work and are verified by digests: Phase 3 result `7eb02d13…` and replay sequence `eeacba10…`. Phase 1 must add the foundation without changing any result.

## Decisions
1. **Provider contract** (`goldflow/providers/base.py`).
   - An abstract `Provider` with `capabilities()`, `discover_instrument()`, `historical_trades()`, `historical_depth()`, `subscribe()`, `health()` and `close()`.
   - Capabilities are declared per data type with an `Availability`: AVAILABLE, AVAILABLE_COARSE, EXCHANGE_REPORTED, NOT_CONTIGUOUS, NOT_IMPLEMENTED, REQUIRES_CREDENTIALS, GEO_RESTRICTED or UNAVAILABLE.
   - Callers use `require()`, so an unsupported request raises `CapabilityUnavailable` instead of returning empty or substitute data.
   - One provider instance = one venue. Nothing in the contract merges venues.
2. **Binance adapter** (`providers/binance/`) **wraps** the Phase 2 components (`BinanceFuturesREST`, `VisionArchive`, built by the same `make_rest`/`make_archive`). It does not reimplement them, so behaviour is unchanged.
   - Bulk acquisition stays in `goldflow.acquire`, which is resumable and catalogued.
   - `discover` now goes through the adapter, with identical output.
3. **Dummy provider** (`providers/dummy/`): deterministic synthetic trades in the raw Binance schema, flagged `synthetic=True`. The registry refuses it outside the `test` profile.
4. **Validated settings** (`goldflow/settings.py`).
   - Pydantic models for every block, plus cross-field rules: one primary dataset (trades); unique replay ranks; time columns that exist in the canonical schema; offset + latency < 1 day; https URLs; an env-var *name* rather than a key for credentials; relative paths.
   - Spec drift against the last live `exchangeInfo` discovery is also checked.
   - All problems are reported in one `ConfigError`. The CLI exits with code 2 and a clear message.
   - Defaults for new blocks (`execution.live_trading_enabled=false`) live in code, not in the config file. The config hash recorded by existing datasets and checkpoints is therefore unchanged, so nothing has to be reprocessed.
5. **Profiles** (`goldflow/profiles.py`): research (default), test, live.
   - Live is refused unless `execution.live_trading_enabled` is true **and** `data/state/live_gates.json` records the M7 demo gate with operator approval (written by Phase 16 tooling).
   - Until Phase 16 exists, live always refuses.
6. **Observability** (`goldflow/observability.py`).
   - JSON-lines logs (`data/state/goldflow.jsonl`) with ts, severity, component, message, event_id, correlation_id (run id), profile, provider, instrument and extras.
   - The console keeps the human format, so existing wrappers still work.
   - An in-process `Metrics` registry (counters, gauges, timings) is wired into REST requests and latency and into replay events and consumer time. Phase 17 exports it.
7. **Migrations** (`goldflow/migrations.py`).
   - A `schema_migrations` table with numbered, checksummed migrations, applied once in a transaction.
   - Editing an applied migration is refused.
   - Version 1 records the Phases 2–4 baseline. Parquet schema-version bumps follow the policy written in the module docstring.
8. **Tooling.**
   - `pyproject.toml` with compatible pinned ranges and `pip install -e .[dev]`.
   - Ruff: real-error rules for the whole package; formatting enforced on Phase 1+ modules.
   - mypy `--strict` on the foundation modules.
   - `scripts/check.sh` runs all of these plus the tests.

## Consequences
- The four modules that define the Phase 3 `code_version` (`normalize`, `schemas`, `integrity`, `catalog`) were deliberately left byte-identical. Ruff findings in them (unused imports and locals only) are exempted until they change for a real reason. This avoids a forced re-normalisation of every day on the PC.
- The mypy analysis target is 3.12 because the numpy ≥ 2.3 stubs need it. Runtime support stays ≥ 3.10.
- The live profile is unusable until Phase 16. That is intended.
