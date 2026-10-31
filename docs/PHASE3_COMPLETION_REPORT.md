# Phase 3 completion report: historical validation, normalisation and storage

Date: 2026-10-30. Instrument: Binance USDⓈ-M **XAUUSDT** (TradFi perpetual). Code version `3eac8d1e07c5e02a`, schema v1.

## 1. Scope completed / not completed
**Completed**
- Immutable raw archive: 927 files registered with roles, made read-only, and re-hashed before every read.
- Versioned canonical schemas, with reported, derived, constant and reference fields kept in separate columns.
- Record-level validation with 19 reason codes, plus a quarantine dataset.
- Partitioned Parquet storage and a transactional SQLite catalog.
- Dataset manifest, validation and quarantine reports, daily/monthly coverage summaries.
- Reproducible `normalize` / `validate` commands, with determinism proven across runs.

**Not completed**
- PostgreSQL/TimescaleDB is deferred; SQLite is used instead (see ADR-0001).
- Lint/type-check tooling is not configured (that is Phase 1 scope).
- Instrument spec re-verification via REST is blocked (HTTP 451 from the cloud host).
- The 2026-10-30 tail and the upstream gaps listed in §7 are still open.

## 2. Files created, modified, deleted
- **Created:** `goldflow/schemas.py`, `goldflow/normalize.py`, `goldflow/catalog.py`, `goldflow/dataset_report.py`, `tests/test_phase3.py`, `docs/SCHEMAS_V1.md`, `docs/ADR-0001-phase3-storage.md`, `docs/PHASE3_COMPLETION_REPORT.md`.
- **Modified:**
  - `goldflow/acquire.py`: `prefer_daily` planning.
  - `goldflow/cli.py`: `normalize` / `validate` commands.
  - `config/binance_xauusdt.json`: `instrument` block and `prefer_daily`.
  - `README.md`, and `docs/L2_ORDERBOOK_AVAILABILITY.md` (12 bands, not 10).
- **Deleted:** none. No raw file was modified or removed.
- **Data written:** `data/normalized/**` (912 partitions, 3.3 GB), `data/manifests/dataset_manifest.json`, `data/reports/*`. No quarantine files exist because no record failed.

## 3. Architecture and data flow
Raw daily zip (registry sha256 check) → parse (typed; a string fallback isolates bad rows) → record rules → id/time sequence-outlier detection → duplicate resolution → canonical columns.

Trades then go through:
- recovery of single-trade aggTrades that are absent from the trades file;
- completeness evidence against 1m klines (per-minute count and volume) and aggTrades (quantity per row);
- day/hour status.

Days run in parallel worker processes. The driver then:
1. resolves aggTrades whose fills straddle midnight against the neighbouring day;
2. writes catalog rows in one transaction;
3. checks cross-partition id overlaps;
4. cross-checks monthly archive files against the daily files;
5. builds the reports.

Parquet writes are atomic (tmp + fsync + rename).

## 4. Commands
```
pip install pyarrow requests numpy pandas pytest
python -m goldflow.cli acquire --no-rest      # (Phase 2, now with daily files for every day)
python -m goldflow.cli normalize              # 4.8–5 min for 183 days on 2 CPUs
python -m goldflow.cli validate               # ~3.5 min; writes nothing but a validation_runs row
python -m pytest -q tests
```

## 5. Tests
**19 passed, 0 failed** (4 Phase 2 + 15 Phase 3). Coverage:
- every value rule and its reason code;
- float-safe tick/step checks;
- PARSE_ERROR row isolation;
- exact versus conflicting duplicates;
- the June-monthly bogus-timestamp pattern;
- the sequence-outlier unit cases;
- foreign-instrument rows caught by the kline range, and the fallback median rule;
- day pipeline recovery and provenance metadata;
- PARTIAL hour flagging;
- idempotent rewrite (same bytes and digest);
- raw-file tamper detection;
- midnight-straddle resolution, and the PARTIAL result when the neighbouring day is absent.

## 6. Dataset counts, coverage, integrity
Normalised coverage runs from 2026-04-30 00:00:00.276Z to 2026-10-29 23:59:59.381Z (183 days).

| Dataset | Rows | Day status |
|---|---|---|
| trades | 149,515,816 (149,511,048 archive + 4,768 recovered) | 181 COMPLETE, 1 PARTIAL (10-29), 1 UNVERIFIED (10-28), 10-30 MISSING |
| aggTrades | 60,365,927 | 143 COMPLETE, 39 PARTIAL, 10-28 MISSING |
| klines_1m | 262,080 | 182 COMPLETE, 10-28 MISSING |
| bookDepth | 6,165,348 | 145 COMPLETE, 37 PARTIAL, 10-08 MISSING |
| metrics | 52,704 | 183 COMPLETE |

- **Quarantined: 0 records in every dataset.** Every trade and aggTrade price lies inside its minute's kline range, and there are no duplicates, ordering breaks or invalid values.
- Trade-id gaps total 834,366 unused ids. They are not data loss: per-minute kline counts equal the trades on all 181 COMPLETE days.
- **Monthly cross-check:** 152/153 trades days, 153/153 aggTrades days and 153/153 klines days match the daily files. The exception is the **rejected June monthly trades file**. On 2026-06-07 it contains **3,468 rows from another instrument** (prices ≈ $95 and ≈ $1,140, ids around 150k) and is missing 3,582 real XAUUSDT trades.
- Raw files: 927 total (912 CANONICAL, 14 CROSS_CHECK, 1 REJECTED), 0 unaccounted, 0 hash mismatches. Cross-partition id overlaps: 0.

## 7. Known limitations and unresolved items
- **trades 2026-10-29, 21:00–24:00 UTC: PARTIAL.** 31,747 trades are missing versus the klines count, which corresponds to 6,819 aggTrade rows (18,525 oz). The day's other 4,767 missing trades were recovered. Fix: Binance republishing the file (detected automatically by ETag), or `historicalTrades` with an API key from a permitted host, within one month.
- **trades 2026-10-28: UNVERIFIED.** Neither klines nor aggTrades are published for that day. The checksum passes, but there is no independent count.
- **aggTrades** is incomplete on 39 days (23,783 trades absent, including a 27-minute hole on 10-27). It is a secondary dataset; trades is the source of truth.
- **bookDepth** has 843 snapshot holes between Apr 30 and Jun 3, and an outage from 10-07 02:49 to 10-09 04:05. It holds coarse bands only, never L2.
- No historical L2 and no bid/ask quotes exist. Order-book metrics cannot be backtested on this period.
- The 2026-10-30 tail (00:00–08:42:51) is missing for every dataset.
- The instrument spec comes from Phase 0. It is confirmed empirically (every price and quantity is an exact tick/step multiple) but not re-queried, because REST returns 451 from the cloud host.
- Binance XAUUSDT is a venue-specific perpetual. It is not COMEX or OTC gold orderflow.

## 8. Security and operational considerations
- No secrets are stored. `BINANCE_API_KEY` is read from the environment only.
- **The raw (3.98 GB) and normalised (3.3 GB) data exist only in a temporary cloud workspace.** They must be regenerated or kept on persistent storage, such as the trading PC or a VPS, before Phase 4. The pipeline is deterministic, so a local rerun reproduces identical partition digests.
- Partition files are byte-reproducible only under the same pyarrow version. The logical digest is the cross-machine comparison key.

## 9. Acceptance criteria
| Criterion | Result | Evidence |
|---|---|---|
| All source files are accounted for | **PASS** | 927/927 registered with a role; 0 unaccounted; 0 hash mismatches |
| Every invalid record has a reason or is explicitly unclassified | **PASS** | 0 invalid in canonical files. Rules are reason-coded and unit-tested; parse errors are isolated per row. |
| Duplicate detection is tested and idempotent | **PASS** | Exact/conflict/bogus-timestamp tests; idempotent-rewrite test; 0 cross-partition overlaps |
| A second validation run gives the same result | **PASS** | Runs `validate-…131341Z`, `normalize-…132015Z` and `validate-…132643Z` all have digest `e2cada67…` (identical = 1). 15 rewritten partitions are byte-identical. |
| Dataset status labelled complete/partial/unverified | **PASS** | Per day and per hour in the catalog. Overall status is **PARTIAL**, with every non-complete interval listed. |

## 10. Recommended next phase
**Phase 4, historical event replay engine.** Prerequisite: move the dataset onto persistent storage first, either by running the pipeline on your PC or a VPS. Phase 4 has not been started.
