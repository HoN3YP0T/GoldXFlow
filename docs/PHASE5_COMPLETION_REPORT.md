# Phase 5 completion report: trade processing, classification and price-level aggregation

Date: 2026-10-31. Instrument: Binance USDⓈ-M XAUUSDT. Input: Phase 4 replay over the Phase 3 dataset. Specification: `docs/PROCESSING_V1.md`.

## 1. Scope completed / not completed
**Completed**
- **`TradeProcessor`:** one shared processor, implemented as a replay `Consumer`. It works on any `EventChunk`, so the Phase 14 live bus can drive it unchanged (tested with hand-built "live-style" chunks).
- **Side classification** from `is_buyer_maker` (false → BUY, true → SELL, missing → UNKNOWN), cross-checked against the Phase 3 column. UNKNOWN is never reassigned. An optional tick-rule INFERRED side, off by default, lives in separate columns.
- **Idempotency:** watermark plus a ring of recent ids. Duplicates are ignored and counted; late arrivals are accepted and counted; unprovable ids are dropped and counted.
- **Exact integer aggregation:** lots, ticks and tick·lot notional, per price level, at a configurable bin.
- **Footprint bars** at 1s/5s/1m/5m/15m/1h, with OHLC, volume, buy/sell/unknown, delta, counts, notional, source trade-id range and a per-level footprint. Developing bars become final only when the clock passes `end + shift`.
- **Windows:** UTC day, trading day (18:00 New York), trading week, sessions (Asia/Tokyo, Europe/London, America/New_York via zoneinfo) and custom ranges. Each is labelled with `weekend`, `dq_status` and `coverage`.
- **Snapshot/restore** inside the replay checkpoint, refused if the processing settings change.
- **Derived Parquet footprint dataset** (1m) with a manifest, plus a reconciliation command that checks it against the source partitions and Binance's 1m klines.
- **CLI:** `process` (resumable, budgeted) and `reconcile`. 28 new tests. `docs/PROCESSING_V1.md`.

**Not completed / by design**
- **Session times** are the guide's *proposed* defaults. Decision D4 is still open and must be confirmed before Phase 10.
- **No exchange-holiday calendar** (Phase 10). Weekend detection uses Binance's regular schedule, Sun 22:00 → Fri 21:00 UTC, which still has to be re-verified from a permitted host.
- **Built on Phase 1**, which landed on `main` during this phase and was rebased under it. The `processing`/`sessions` blocks are validated at `Config.load` through Phase 1's `validate_config` (`ConfigError`, CLI exit 2). `goldflow.processing` passes `mypy --strict`, and `scripts/check.sh` passes in full.
- **Persistence:** only the 1m footprint is persisted by default. The other intervals exist in memory for downstream sinks.

## 2. Files created, modified, deleted
**Created**
- `goldflow/processing/__init__.py`, `trade_processor.py`, `classify.py`, `aggregation.py`, `sessions.py`, `settings.py`, `footprint_store.py`, `reconcile.py`
- `tests/test_processing.py`
- `docs/PROCESSING_V1.md`, `docs/PHASE5_COMPLETION_REPORT.md`
- `data/reports/processing_reconciliation_1m.{md,json}` (from the full six-month run)

**Modified**
- `goldflow/replay.py`, three small changes. None of them changes the delivered event sequence; the digest is unchanged, see §5–6:
  1. The run context passes `filtered` and `quality_before_start` to consumers.
  2. A contiguous day-window selection uses a zero-copy `slice` instead of `take` (same rows, same order).
  3. **Defect fix:** a replay stopped by `--until`/`--budget` inside the *last* day was reported as `completed`. It now reports `stopped` and is resumable. This mattered for Phase 5, which treats completion as "the clock reached the end".
- `goldflow/cli.py`: `process` and `reconcile` commands (alongside Phase 1's `status`/`providers`).
- `goldflow/settings.py` (Phase 1): validates the Phase 5 config blocks at load time.
- `pyproject.toml`: `goldflow.processing` added to the strict mypy targets.
- `config/binance_xauusdt.json`: new `processing` and `sessions` blocks. Existing blocks are untouched, so Phase 3/4 hashes are unaffected.
- `tests/test_replay.py`: regression test for the status defect.
- `tests/test_processing.py` also checks that an invalid `sessions` block fails at `Config.load`.
- `README.md`, `docs/BUILD_GUIDE.md`.

**Deleted:** none.

## 3. Architecture and data flow
```
ReplayEngine (Phase 4) ── EventChunk ──► TradeProcessor.on_chunk
    quality events ─► DQ intervals (labels only: suppress, never trigger)
    trades ─► columns (cached per day table) ─► dedup ─► classify ─► seq
           ─► G = Rows(1s bucket × 1-tick level).reduce()
           ─► per interval: rows built from the next-finer interval ─► merge into developing rows
           ─► open windows (calendar instances) ─► LevelArrays.add_rows
    clock  ─► finalise bars/windows with end + shift ≤ clock ─► sinks.on_bars / on_window
sinks: FootprintWriter (Parquet + manifest, staged at checkpoints), Phase 6–10 analytics (ProcessorSink)
checkpoint: replay cursor + processor state + sink state, one transaction (Phase 4 store)
```

## 4. Commands
```bash
pip install -r requirements.txt pytest
python -m goldflow.cli process --name p1                 # full dataset, writes footprint and reconciles
python -m goldflow.cli process --name p1 --resume        # after --until / --budget / Ctrl-C
python -m goldflow.cli reconcile --interval 1m
python -m pytest -q tests
# On your PC (restricted mount):
scripts/pc_run.sh 165 process --name pc-p5 --budget 130 ; scripts/pc_run.sh 165 process --name pc-p5 --resume --budget 130   # repeat --resume until "completed"
```

## 5. Tests
**85 passed, 0 failed** (cloud container; `scripts/check.sh`: ruff, ruff format, mypy, pytest all pass). That is 56 existing (Phases 1–4), 1 new Phase 4 regression test and 28 Phase 5 tests (counting each of the 5 invalid-settings cases separately).

The Phase 5 tests cover:
- classification semantics and the UNKNOWN case;
- tick-rule chunk independence;
- `Rows.reduce`/`merge_sorted` chunk independence;
- hand-computed OHLC and side totals;
- `LevelArrays` growth, rebin and round-trip;
- session bounds across all four 2026 UK/US DST transitions (including 23 h and 25 h trading days and the UK≠US week);
- refusal of DST-gap times;
- the weekend schedule;
- calendar instance kinds;
- five invalid-settings cases;
- per-day totals equal partitions, with UNKNOWN never reassigned;
- the footprint store plus reconciliation, and detection of a tampered bar;
- no look-ahead (developing vs final, emission clock checks);
- idempotency: double delivery, late id, unverifiable id;
- snapshot/restore at 4 cut points equals uninterrupted (state digest and file digests);
- chunk-size independence;
- live-style chunks equal replay;
- DST-correct session assignment in the processor;
- weekend and DQ labels on bars;
- inferred side kept separate and off by default;
- resume refused after a settings change;
- markets never merged;
- seek behaviour.

Phase 4 regression: the replay sequence digest of the six-day sample is `915356de…95a0` both before and after the `replay.py` changes. The six-month digest is in §6.

## 6. Real-data evidence

### 6.1 Six-day sample (22–27 Oct 2026; includes a weekend and the UK DST change)
- **Data:** re-downloaded from data.binance.vision in this session and normalised by Phase 3: 6,179,974 trades, 0 quarantined, all six days COMPLETE.
- **Same data as your PC:** all 30 partitions equal the committed `dataset_manifest.json` logical digests once the per-catalog `raw_file_id` reference is aligned.

| Check | Result |
|---|---|
| Daily footprint lots = partition lots, and buy/sell/unknown/trade count/first-last id equal | 6/6 days exact |
| Per-minute volume, trade count, **taker-buy volume vs buy_lots**, OHLC vs Binance 1m klines | 8,640/8,640 minutes, 0 mismatches |
| Side cross-check vs Phase 3 `aggressor_side` | 0 mismatches; 0 UNKNOWN trades |
| Every chunk delivered twice | 6,179,974 duplicates ignored; bar outputs and window totals identical |
| Stopped twice (`--until` mid-Saturday and Tuesday 09:00), resumed | Replay digest `915356de…`, processor state `ca180413…`, footprint files identical to the uninterrupted run |
| 65,536- vs 4,999-event chunks; repeat run | Same state digest `ca180413…` and identical bar outputs |
| London session open | 06:00 UTC Fri 23 Oct (BST) → 07:00 UTC Mon 26 Oct (GMT); New York 12:20 UTC both days (US DST ends 1 Nov) |
| Weekend flag | Fri from 21:00 UTC (180 1m bars), all of Sat, Sun until 22:00 UTC (1,320 bars) |
| Throughput (writer and checkpoints on) | 571–657 k events/s; peak RSS ≈ 1.0 GB |

### 6.2 Full six-month dataset
**Data.** The full dataset was rebuilt from scratch in this session from the frozen range in `data/state/range.json`: 932 archive files, ≈4.0 GB, SHA-256-verified. The June monthly contamination was detected again and replaced by daily files. Phase 3 then reproduced exactly the documented figures: 912 partitions, **149,515,816 trades**, 0 quarantined, trades days 181 COMPLETE / 1 UNVERIFIED (28 Oct) / 1 PARTIAL (29 Oct).

**The replay sequence digest is `eeacba10…2d50`, identical to the Phase 4 reference.** This proves both that the dataset is the same as on your PC and that the `replay.py` changes left the event sequence unchanged. (The Phase 3 *result* digest, `3860107a…` here versus `7eb02d13…` on the PC, also covers catalog-specific `raw_file_id` references. On the sample, the content was shown identical once those were aligned.)

| Measure | Value |
|---|---|
| Events replayed through the processor | 155,997,644 (trades 149,515,816; bookDepth 6,165,348; klines 262,079; metrics 52,704; quality 1,697) |
| Wall time / throughput | 237 s / **657,749 events/s** (4 vCPU cloud container; all 6 intervals, all windows, Parquet writer and checkpoints on). After rebasing onto Phase 1: 630,790 events/s, with identical digests and footprint files |
| Peak RSS | 1,245 MB |
| Trades processed / duplicates / late / unverifiable / side mismatches vs Phase 3 | 149,515,816 / 0 / 0 / 0 / 0 |
| Recovered (aggTrades) trades included | 4,768 |
| Lots: total = buy + sell + unknown | 55,245,785,609 = 27,323,749,719 + 27,922,035,890 + 0 |
| Bars finalised | 1s 9,830,113 · 5s 2,886,352 · 1m 263,518 · 5m 52,704 · 15m 17,568 · 1h 4,392 (late-after-final: 0 for every interval) |
| Windows finalised / still developing at end | 785 / 3 (Asia 30 Oct, trading day 30 Oct, trading week 26 Oct: all extend past the data, correctly not finalised) |
| Processor state digest | `ad595214596f8399…` |
| 1m footprint written | 183 dates, 263,518 bars, 23,206,132 level rows, ≈512 MB |
| **Reconciliation** (`data/reports/processing_reconciliation_1m.md`) | **183/183 dates PASS.** All 181 COMPLETE days: footprint = partition exactly, and every one of 260,638 traded minutes equals Binance's kline in volume, trade count, taker-buy volume and OHLC (0 mismatches). |
| PARTIAL 29 Oct | Footprint = partition exactly. 123 minutes differ from klines, all in 21:49–23:59 UTC, totalling 18,524,995 lots: the known upstream gap, reported and not filled. |
| UNVERIFIED 28 Oct | Footprint = partition exactly. No klines exist that day to compare against. |

The 1m bar count is 2 fewer than 183 × 1,440: two minutes had no trades, and the kline check confirms every *traded* minute has a bar.

**Six-month stop/resume.** A second run was stopped with `--until 2026-07-15T12:34:56.789Z`, inside the London session, after 44,022,265 events, then resumed with `--resume`. The resumed run delivered the remaining 111,975,379 events. It reproduced replay digest `eeacba10…`, processor state digest `ad595214…`, **identical footprint files for all 183 dates** (bars and levels logical digests), 183/183 reconciliation PASS and 0 integrity violations.

## 7. Known limitations, missing capabilities, unresolved decisions
- **D4 session definitions:** open. The defaults are flagged `PROPOSED` in config. Any change alters the settings hash, which forces a fresh `process` run (resume is refused, by design).
- **Weekend schedule:** taken from Binance's documented COMMODITY REGULAR schedule; still to be re-verified via `tradingSchedule` from a non-US host.
- **DQ labels** come from Phase 3 offline annotations. They label bars and windows, and downstream may only use them to suppress work.
- **Late trades** (relevant live only) are counted and not merged into already-final bars. A live amendment policy is a Phase 14 decision.
- **Inferred side** is implemented (tick rule) but has no effect on this dataset, because Binance reports a side on every trade.
- **Formatting:** the Phase 5 modules pass `ruff check` and `mypy --strict`, but are not yet in `scripts/check.sh`'s `ruff format` list (it covers Phase 1 modules only).

## 8. Security and operational considerations
- No credentials were used or stored. The derived data is git-ignored like all market data; only the reconciliation report is committed.
- The processor checkpoint is stored with the replay checkpoint in the existing SQLite state DB. Footprint writes are atomic (`.tmp` + `os.replace`). Staging files are removed with `safe_remove`, which moves rather than deletes on your restricted mount.
- Disk: the six-month 1m footprint takes ≈512 MB (versus ≈3.3 GB of normalised partitions). Back it up with the rest of `Documents\python\goldflow`.

## 9. Acceptance criteria
| Criterion | Result | Evidence |
|---|---|---|
| Every day: Σ bar volume = Σ partition `quantity_lots`; Σ buy + Σ sell + Σ unknown = total, exactly in integers | **PASS** | 183/183 days exact (§6.2); unit tests |
| Per-minute volume equals Binance klines on all COMPLETE days | **PASS** | All 181 COMPLETE days, 260,638 minutes, 0 mismatches (also count, taker-buy and OHLC) |
| Unknown-side volume stays unknown | **PASS** | `unknown_lots` column; test with 1 % null sides; inferred kept separate |
| Delivering the same events twice doesn't change totals | **PASS** | Real-data double delivery; unit test |
| Snapshot/restore equals an uninterrupted run (state digest) | **PASS** | Real-data stop/resume ×2; 4 cut points in tests |
| Replay and (future) live call the same processor | **PASS** | One `on_chunk`/`ingest` path; live-style chunk test equals replay |
| DST transitions tested (late March and late October) | **PASS** | UK and US March/Oct–Nov 2026 tests; real 26 Oct London open |
| Full replay stays above 500 k events/s with aggregation on | **PASS** | 630–658 k events/s over all 156.0 M events (cloud). On your PC, expect it to scale like replay did in Phase 4; measure with the `pc_run.sh` command in §4 |

## 10. Recommended next phase
**Phase 6: Volume Profile, POC, Value Area, HVN/LVN and VWAP.** It builds on the window `LevelArrays` (profiles at any bin) and on the trades themselves (exact VWAP from Σq, Σqp, Σqp² in integers). It plugs in as a `ProcessorSink`. Not started.

Before Phase 10, please confirm **D4** (sessions and the IB window). Phase 1's remaining foundation work (pyproject, structured logging, lint/type-check in CI) can be done any time and should keep the digests above unchanged.
