# ADR-0001: Phase 3 storage and canonical source selection

**Status:** accepted for Phase 3 (2026-10-30). Revisit at Phase 14 (live ingestion) or Phase 17 (production).

## Decisions
1. **Normalised events go in partitioned Parquet** (zstd level 3, 262,144-row groups), with one file per provider/market/instrument/data type/UTC date. The build guide recommends this layout. A day (≤2 M trades) is the unit of validation, rewrite and replay, so nothing ever loads six months into RAM.
2. **Metadata, checkpoints and catalog go in SQLite** (WAL, `synchronous=FULL`, explicit transactions), not PostgreSQL/TimescaleDB yet. This is a single-user research workload on one machine, and SQLite needs no server on Windows. The catalog tables are plain SQL, so moving to PostgreSQL later is a dump/load. Trigger to migrate: concurrent live writers (Phase 14) or a multi-process dashboard (Phase 17).
3. **Daily archive files are canonical; monthly files are cross-checks only.** The June 2026 monthly trades file held 76 duplicated ids with wrong timestamps, while the daily files were clean. Daily files also map one-to-one onto partitions.
4. **Recovered trades are stored inside `trades` and flagged** (`record_origin`, `data_quality_status`, `aggregate_trade_id`), not kept in a side table. Replay sees one ordered stream, and any consumer can exclude them with one filter.
5. **The raw archive is made read-only** (chmod 0444) and re-hashed against the registry before every read. A changed raw file aborts the run.
6. **The first day is kept whole.** Normalised coverage starts 2026-04-30 00:00 UTC rather than at the acquisition start of 08:42:51, so warm-up calculations have the full day.

## Consequences
- Determinism: partition content digests come from the Arrow IPC stream with metadata stripped. File SHA-256 values are recorded too, and both are compared between runs.
- Parquet bytes depend on the pyarrow version. The logical digest is the cross-machine comparison key; file hashes only compare like with like.
