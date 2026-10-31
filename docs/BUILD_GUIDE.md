# GoldXFlow: Complete Build Guide

**Gold Orderflow Automation Engine. Every phase from research to production, with what to build, how to build it, and how to prove it is done.**

Version 1.0 · 31 October 2026 · Repository: `HoN3YP0T/GoldXFlow` · Applies to the `goldflow` Python package in this repository.

This guide combines four sources:
- the project goal (the project description);
- the phase specification in `Gold_Orderflow_Automation_Engine_Complete_Project_Build_Guide.pdf`;
- the code already built in Phases 2–4;
- the findings made while building it.

Where the PDF is generic, this guide is specific to our instrument, our data and our code. Where a decision is still open, it says so and names who must decide.

---

## Contents
1. How to use this guide
2. The goal and how we will know we reached it
3. Non-negotiable engineering principles
4. The instrument and the data we actually have
5. Architecture
6. Phase map and current status
7. Goal traceability: every goal mapped to the phases that deliver it
8. Phase-by-phase build instructions (Phases 0–17)
9. Cross-cutting systems: data integrity, no look-ahead, determinism and parity, risk, testing, operations, security
10. Milestone gates (M1–M8)
11. Open decisions that block later phases
12. Phase completion report template and standing instruction
13. Glossary

---

## 1. How to use this guide

- Phases are built **in order, one at a time**. Each phase ends with a completion report (template in §12), then stops for your approval before the next phase starts.
- Before a phase begins, its contents are summarised for approval ("what's in this phase").
- Every phase lists:
  - **Objective**
  - **Depends on**
  - **What to build**
  - **How to build it** (design, formulas, file locations)
  - **Deliverables**
  - **Acceptance criteria**
  - **Pitfalls**
  - **Status**
- "Done" means the acceptance criteria pass with evidence (counts, digests, test results), not that code exists.
- A phase may expose a defect in an earlier one. The earlier phase is then fixed and re-verified first. Phase 3, for example, exposed contaminated monthly archive files from Phase 2.

---

## 2. The goal and how we will know we reached it

**Goal.** Build a professional-grade, fully automated Gold Orderflow Trading Engine that:
- acquires market data;
- processes orderflow (volume, delta, liquidity, auction behaviour);
- identifies statistically validated trading opportunities;
- executes them through MetaTrader 5.

Every decision must be traceable to verifiable market data. It is **not** an indicator EA. It is orderflow intelligence plus execution infrastructure.

**Success criteria** (from the project description; each one is traced to phases in §7):

| # | Success criterion |
|---|---|
| S1 | Reliable, continuous acquisition of relevant historical and real-time gold market data |
| S2 | No undetected data gaps, duplicated events or silent calculation failures |
| S3 | Accurate and independently verifiable orderflow calculations |
| S4 | Identical core analytical and strategy logic across backtesting, replay and live trading |
| S5 | Reliable signal transmission and confirmed trade execution through MT5 |
| S6 | Comprehensive backtesting, out-of-sample validation and live performance reporting |
| S7 | Strict risk controls and automatic protection against unreliable data or abnormal execution |
| S8 | Consistent performance on unseen market data rather than overfitting to history |

**What success is not.** A backtest that looks profitable. Six months of one venue is a research dataset, not proof. Profitability is never claimed from in-sample results, and S8 is judged only on data the strategy never saw during development.

---

## 3. Non-negotiable engineering principles

1. **Data first.** Acquisition, validation, storage and deterministic replay come before analytics, strategies and MT5.
2. **Zero silent data loss.** Detect, record, quarantine, recover where possible, and block dependent calculations or trading when integrity cannot be established. Never substitute unverified values.
3. **Never assume.** Instruments, specs and capabilities are discovered and verified. They are not hard-coded from memory.
4. **Provenance on every event:** provider, venue, market type, instrument, raw source reference, schema version.
5. **No merged markets.** Never combine unrelated order books or volume as if they were one market.
6. **One code path.** The same calculation and strategy code serves replay, backtest and live.
7. **No look-ahead.** At simulated time T, only information available at T is visible.
8. **Documented metrics.** Every metric has a definition, inputs, parameters, tests and limitations.
9. **No assumed fills.** Nothing counts as executed until MT5 confirms it broker-side.
10. **Live trading is off by default.** It stays off until data, backtest, parity, risk, recovery and demo tests pass.
11. **Pushback is part of the process.** If an instruction would harm results, say so before proceeding.

---

## 4. The instrument and the data we actually have

### 4.1 Instrument
**Binance USDⓈ-M `XAUUSDT`**:
- contract type `TRADIFI_PERPETUAL`, underlying `COMMODITY` (spot gold);
- 1 contract = 1 troy ounce, quoted in USDT;
- tick 0.01, step 0.001 oz, minimum notional 5 USDT;
- listed 2025-12-11; status TRADING.

The spec was verified live from the trading PC via `exchangeInfo` on 2026-10-30. Token perpetuals (`PAXGUSDT`, `XAUTUSDT`) track gold tokens, not XAU, and are excluded.

**Important limitation.** This is Binance's own gold perpetual. It is **not** COMEX futures orderflow and **not** the OTC XAUUSD market your MT5 broker quotes. It is the research and signal venue. Execution happens at a broker on a different price feed (see Phases 12, 15, 16 and §11).

### 4.2 Data categories and what each can prove

| Data | We have it? | Supports | Cannot prove |
|---|---|---|---|
| Individual trades (archive) | **Yes**, 2026-04-30 → 2026-10-29, 149.5 M trades | Price-level volume, aggressor side, delta, CVD, trade size, footprint | Resting liquidity, cancellations |
| Aggregate trades | Yes (incomplete on 39 days) | Recovery of missing trades; secondary volume | Individual executions |
| 1-minute klines | Yes | Independent completeness reference (count, volume per minute), price plausibility | Tick sequence |
| bookDepth (±0.2 %, ±1–5 % cumulative bands, ~30 s) | Yes, coarse | Rough liquidity context | Price-level book, queue behaviour |
| Metrics (5-min open interest, long/short ratios) | Yes | Positioning context | Intraday flow |
| Historical L2 order book | **No** (not published free) | – | Order-book metrics cannot be backtested on this period |
| Historical bid/ask quotes | **No** (bookTicker not published for XAUUSDT) | – | Spread history on Binance |
| Broker MT5 ticks | **Not yet acquired** | Realistic broker fills and costs | – (required for Phase 12) |

### 4.3 Dataset status (end of Phase 4)

| Item | Value |
|---|---|
| Acquisition range | `[2026-04-30T08:42:51.101Z, 2026-10-30T08:42:51.101Z)`, frozen in `data/state/range.json` |
| Normalised coverage | 2026-04-30 00:00 → 2026-10-29 23:59:59.381 (183 days × 5 datasets = 912 Parquet partitions) |
| Trades | 149,515,816 (149,511,048 archive + 4,768 recovered from aggTrades); 0 quarantined |
| trades day status | 181 COMPLETE (per-minute kline count and volume match exactly); 2026-10-28 UNVERIFIED; 2026-10-29 PARTIAL for 21:00–24:00 UTC (31,747 trades missing upstream); 2026-10-30 MISSING (tail recovered as 223,132 aggTrades, stored raw) |
| Upstream defects found | June 2026 **monthly** trades file contains 3,468 rows from another instrument plus 76 duplicate ids, and lacks 3,582 trades. Daily files are canonical. |
| Determinism | Phase 3 result digest `7eb02d13…f704`; replay sequence digest `eeacba10…2d50`. Identical in the cloud and on the PC. |
| Where the data lives | Trading PC: `Documents\python\goldflow` (≈7.1 GB). Not in git. No backup yet. |

### 4.4 Capability flags (enforced from Phase 11)
These flags live in `goldflow/schemas.py: CAPABILITIES`:

| Flag | Value |
|---|---|
| `historical_trades` | AVAILABLE |
| `aggressor_side` | EXCHANGE_REPORTED |
| `historical_l2_depth` | UNAVAILABLE |
| `historical_depth_bands` | AVAILABLE_COARSE |
| `historical_bid_ask_quotes` | UNAVAILABLE |
| `trade_sequence_numbers` | IDS_NOT_CONTIGUOUS |

A strategy that needs an unavailable capability must refuse to run on that interval.

---

## 5. Architecture

### 5.1 Data flow
```
            ┌────────────────────────── providers/ (adapters declare capabilities) ─────────────────────────┐
            │ Binance archive + REST (built)   Binance WebSocket (Ph14)   Bitget/OKX/COMEX/broker (Ph15)       │
            └──────────────┬──────────────────────────────┬──────────────────────────────────────────────────┘
                           │ raw, immutable                │ raw payloads captured before transformation
                   data/raw/…  (Ph2)                 live capture log (Ph14)
                           │                               │
                 validation + normalisation (Ph3) ◄────────┘  same schema, same validator
                           │  canonical events (Parquet) + catalog + quarantine
                 deterministic replay / live event bus (Ph4 / Ph14)  ── one event interface ──
                           │
          trade processor & price-level aggregation (Ph5)  ── shared by replay, backtest, live ──
                           │
     profile/VWAP (Ph6) · delta/CVD/imbalance/big trades/absorption (Ph7) · order book (Ph8)
     auction/failed auction (Ph9) · sessions/IB/IVB (Ph10)
                           │  analytical state + data-quality flags
              strategy framework & signal engine (Ph11)
                           │  TradingSignal (explainable, capability-checked)
         ┌─────────────────┴─────────────────┐
  backtester + order simulator (Ph12)   risk engine + MT5 bridge (Ph16) ──► MetaTrader 5 (broker)
         │                                        │ broker confirmations, reconciliation
  research/walk-forward (Ph13)            monitoring, dashboard, ops (Ph17)
```

### 5.2 Technology decisions (recorded in ADRs under `docs/`)
- **Python 3.10+** for adapters, data engineering, analytics, strategies, replay and backtesting.
- **pyarrow and NumPy** for vectorised processing. Pandas is for research only.
- **Partitioned Parquet** (zstd) for events, one file per provider/market/instrument/data type/UTC date (ADR-0001).
- **SQLite** (WAL; PERSIST on restricted mounts) for the catalog, checkpoints and run records. Migrate to PostgreSQL/TimescaleDB when concurrent live writers or a multi-process dashboard arrive (Phase 14/17).
- **MQL5 EA** for broker-side execution, with a documented local bridge (Phase 16).
- **Pytest** for all tests. Ruff and mypy are added in Phase 1.

### 5.3 Repository layout (current → target)
```
goldflow/                     current package
  config.py  timeutil.py  retry.py  fsutil.py
  rest_client.py  vision_archive.py  checkpoint.py  acquire.py  integrity.py  report.py   (Phase 2)
  schemas.py  normalize.py  catalog.py  dataset_report.py                                  (Phase 3)
  replay.py                                                                                (Phase 4)
  settings.py  profiles.py  observability.py  migrations.py  providers/{base.py, registry.py, binance/, dummy/}  (Phase 1)
  processing/{trade_processor.py, classify.py, aggregation.py, sessions.py, settings.py,
              footprint_store.py, reconcile.py}                                            (Phase 5)
  orderflow/{volume_profile.py, vwap.py, engine.py, store.py, rebuild.py, verify.py, export.py}            (Phase 6)
  orderflow/{delta.py, cvd.py, imbalance.py, big_trades.py, absorption.py, exhaustion.py, divergence.py,
             events.py, flow_engine.py, flow_store.py, flow_verify.py, flow_settings.py}                  (Phase 7)
  liquidity/{book.py, recorder.py, capture.py, dataset.py, metrics.py, coverage.py, registry.py, ...}     (Phase 8)
  cli.py      discover | acquire | rest | manifest | normalize | validate | report | replay | process | reconcile |
              vprofile[-verify|-export] | orderflow[-verify] | auction[-verify|-review] | ib[-verify] | signals[-verify] | broker-import | backtest | perf-report | study | ledger-verify | depth-record | depth-replay | depth-normalize
  orderflow/{auction.py, failed_auction.py, auction_engine.py, auction_verify.py, auction_review.py, auction_settings.py}  (Phase 9)
  orderflow/{initial_balance.py, ib_verify.py, ivb.py}  processing/holidays.py                 (Phase 10)
  — to add —
  strategies/{signal.py, base.py, regime.py, library.py, engine.py, verify.py}             (Phase 11, built)
  backtesting/{settings.py, broker.py, basis.py, simulator.py, engine.py}                  (Phase 12, built)
  research/{settings.py, metrics.py, ledger.py, runner.py, walkforward.py, study.py}         (Phase 13, built)
  live/{trades.py, bus.py, runner.py, parity.py}                                           (Phase 14, built)
  venues/{mapping.py, archives.py, ingest.py, run.py, crossvenue.py, report.py}; providers/{okx,bybit,bitget}  (Phase 15, built)
  execution/{protocol, settings, orders, risk, symbol, reconcile, audit, transport, bridge, gate, sim}.py; mt5/GoldflowExecutor.mq5  (Phase 16, built)
  execution/{bridge.py, order_manager.py, positions.py}  risk/                             (Phase 16)
  monitoring/  dashboard/                                                                  (Phase 17)
config/  docs/  scripts/  tests/  data/ (git-ignored except small reports, manifest and frozen range)
mql5/   (Phase 16: EA executor source)
```

---

## 6. Phase map and current status

| Phase | Name | Status | Milestone |
|---|---|---|---|
| 0 | Research, requirements and feasibility | **Done** (separate session; Phase 0 report) | – |
| 1 | Project foundation and modular architecture | **Done** (2026-10-31; see `docs/PHASE1_COMPLETION_REPORT.md`) | – |
| 2 | Binance adapter and six-month acquisition | **Done** | M1 |
| 3 | Validation, normalisation and storage | **Done** | M1 |
| 4 | Deterministic historical replay | **Done** | M2 |
| 5 | Trade processing, classification, price-level aggregation | **Done** (session times still PROPOSED, D4) | M3 (begins) |
| 6 | Volume Profile, POC, Value Area, HVN/LVN, VWAP | **Done** (2026-10-31; see `docs/PHASE6_COMPLETION_REPORT.md`; v1 node/VA parameters PROPOSED) | M3 |
| 7 | Delta, CVD, imbalance, Big Trades, absorption, exhaustion | **Done** (2026-10-31; see `docs/PHASE7_COMPLETION_REPORT.md`; detector parameters PROPOSED, D11) | M3 |
| 8 | Order-book reconstruction and liquidity | **Done** (2026-10-31; live-only, see `docs/PHASE8_COMPLETION_REPORT.md`). The recorder must run on the PC or a non-US VPS. | M3 (live part) |
| 9 | Auction Market Theory and failed auctions | **Done** (2026-10-31; see `docs/PHASE9_COMPLETION_REPORT.md`; thresholds PROPOSED, D12; IB levels arrive with Phase 10) | M3 |
| 10 | Sessions, Initial Balance, IVB | **Done** (2026-10-31; see `docs/PHASE10_COMPLETION_REPORT.md`). IVB disabled until D1; holiday calendar PROPOSED | M3 |
| 11 | Strategy interface and signal generation | **Done** (2026-10-31; see `docs/PHASE11_COMPLETION_REPORT.md`; example strategy parameters PROPOSED, D13) | – |
| 12 | Event-driven backtesting and execution simulation | **Built** (2026-10-31; see `docs/PHASE12_COMPLETION_REPORT.md`). Real backtests **blocked on D2** (broker spec + tick history) | M4 |
| 13 | Reports, validation, walk-forward, research | **Built** (2026-10-31; see `docs/PHASE13_COMPLETION_REPORT.md`). Real studies blocked on D2; thresholds D8 | M5 |
| 14 | Live Binance ingestion and historical/live parity | **Built** (2026-10-31; see `docs/PHASE14_COMPLETION_REPORT.md`). Parity exact on 4 real days; live REST needs a non-US host (D6) | M6 |
| 15 | Additional providers and cross-market mapping | **Done** (2026-10-31; see `docs/PHASE15_COMPLETION_REPORT.md`). OKX + Bybit history imported and run through the unchanged stack; Bitget discovery only | – |
| 16 | MT5 execution bridge and risk controls | **Built** (2026-10-31; see `docs/PHASE16_COMPLETION_REPORT.md`). EA not compiled here; demo run and M7 gate need D2/D6 | M7 |
| 17 | Dashboard, observability, deployment, hardening | Not started | M8 |

---

## 7. Goal traceability

Every project requirement is mapped to the phases that deliver it and the evidence that proves it. A goal "can't be missed" only if it appears here with acceptance evidence.

| Project requirement | Delivered by | Evidence of done |
|---|---|---|
| S1 historical acquisition | Ph2, Ph3 | Manifest; 927 files SHA-256 verified; coverage report |
| S1 real-time acquisition | Ph14 | Live feed health metrics; gap recovery log; stale-feed blocking test |
| S2 no undetected gaps/duplicates | Ph2, Ph3, Ph4, Ph14 | Kline count/volume reconciliation; quarantine report; replay integrity checker; live sequence-gap tests |
| S2 no silent calculation failures | Ph5–Ph10 | Reconciliation tests against source totals; DQ gating; state-invalidation tests |
| S3 accurate, verifiable orderflow | Ph5–Ph10 | Formula docs; synthetic and real reference tests; independent recomputation |
| S4 identical logic across backtest/replay/live | Ph4, Ph5, Ph11, Ph12, Ph14 | Single code path; parity harness report |
| S5 reliable MT5 execution | Ph16 | Demo report; broker-confirmed lifecycle logs; duplicate-prevention tests |
| S6 backtesting and out-of-sample validation | Ph12, Ph13 | Ledger-reconciled reports; walk-forward results; untouched test set |
| S7 risk controls and protection | Ph11 (DQ gating), Ph12 (risk simulation), Ph14 (stale feed), Ph16 (risk engine), Ph17 (emergency stop) | Risk-limit tests; data-gap suspension tests; emergency-stop drill |
| S8 performance on unseen data | Ph13 | Out-of-sample and walk-forward metrics; research ledger; approval criteria |
| §2 multi-source, configurable providers | Ph1 (contract), Ph14, Ph15 | Provider added without analytics changes |
| §2 never merge unrelated books | Ph8, Ph15 | One book per venue; tests reject merged inputs |
| §3 volume and market profile | Ph6 | `docs/METRICS_PROFILE_V1.md`; hand-computed POC/VA/prominence tests; six-month from-scratch parity (`data/reports/profile_verification.md`) |
| §3 VWAP and volume analytics | Ph6 | Exact integer VWAP sums; float64 recomputation from decimal prices within 1e-9 relative |
| §3 trade and orderflow analysis | Ph5, Ph7, Ph8 | `docs/METRICS_ORDERFLOW_V1.md`; hand-computed imbalance/quantile/event tests; `orderflow-verify` exact recomputation from trades (bars, CVD, imbalances, every big trade and fill id, event evidence and resolutions) |
| §3 Auction Market Theory | Ph9 | `docs/METRICS_AUCTION_V1.md`; hand-computed episode/failed-auction/shape tests; replay clock check; `auction-verify` exact re-derivation from trades; `auction-review` charts |
| §3 Initial Balance and IVB | Ph10 | `docs/METRICS_IB_V1.md`; DST-week tests; `ib-verify` exact recomputation; IVB refused at startup until its formula is approved (D1) |
| §4 signal generation with evidence, SL/TP, R:R, score, reason | Ph11 | `docs/SIGNALS_V1.md`; `signals-verify` re-derives every signal from its evidence; gating and look-ahead tests; reproducibility tests |
| §5 MT5 lifecycle, risk, reconciliation, recovery | Ph16 | – |
| §6 backtest/live parity, costs, walk-forward, reports | Ph12, Ph13, Ph14 | – |
| §7 zero-silent-loss architecture | Ph2–Ph4 (built), Ph14 (live) | Layered integrity (§9.1) |
| §8 modular, recoverable, monitored | Ph1, Ph17 | Restart/recovery tests; resource metrics |
| §9 dashboard and logs | Ph17 | Dashboard bound to real engine state; no fabricated values |


---

## 8. Phase-by-phase build instructions

### Phase 0: Research, requirements and technical feasibility. **Status: Done**
**Objective.** Verify the instrument, API capabilities, historical coverage and constraints before writing the pipeline.

**What was done.**
- Venue survey: Binance, OKX, Bitget and Bybit gold perpetuals.
- Selected Binance `XAUUSDT` (about 94 % of Binance gold-linked turnover).
- Order-book depth per venue.
- Archive coverage.
- Binance geo-block (HTTP 451 from US cloud).
- RPI behaviour: per the Phase 0 notes, the aggTrade stream excludes RPI trades since 2025-12-31.
- Relationship of the perpetual to spot gold: correlation, weekend behaviour.

**Deliverables (exist).** Phase 0 report (project artifact), `claude/data-sources-binance.md`.

**Carry-forward rule.** Any Binance capability used later must be re-verified against the live API or archive at the time it is used. Phase 2 re-verified the spec, archive listings and REST limits.

---

### Phase 1: Project foundation and modular architecture. **Status: Done (2026-10-31)**
**Objective.** A clean foundation every later module plugs into: validated configuration, provider contract, storage interfaces, structured logging and quality tooling.

**Depends on.** Nothing. Must not change Phase 2–4 behaviour or digests.

**What already exists.** JSON config, Python logging, SQLite checkpoints and catalog, a Parquet partition layout, pytest (34 tests), `.env.example` with no secrets in the repo, and a restricted-mount runner.

**What to build.**
1. **`pyproject.toml`** with pinned compatible ranges (pyarrow, numpy, requests, pandas, pytest, ruff, mypy, pydantic). Add `pip install -e .[dev]`.
2. **Validated settings** (`goldflow/settings.py`): pydantic models for the providers, instrument, range, datasets, replay, sessions, storage, risk and execution blocks.
   - Fail at startup on invalid or conflicting values, for example: a negative latency, duplicate ranks, a tick size that doesn't match exchangeInfo, or live trading enabled without the demo gate passed.
   - Expose `settings.config_hash`.
3. **Provider contract** (`goldflow/providers/base.py`): an abstract `Provider` with:
   - `capabilities() -> CapabilitySet`;
   - `discover_instrument(symbol) -> InstrumentSpec`;
   - `historical_trades(range)`, `historical_depth(range)` (may raise `CapabilityUnavailable`);
   - `subscribe(streams) -> AsyncIterator[RawEvent]`;
   - `health()` and `close()`.

   `registry.py` maps provider ids to classes from config. Refactor the existing Binance code behind `providers/binance/` without changing its behaviour.
4. **Dummy test adapter** (`providers/dummy/`): deterministic synthetic events, flagged `synthetic=True`. The registry refuses it in any non-test profile.
5. **Structured logging** (`goldflow/logging.py`): JSON lines with UTC timestamp, component, severity, event_id, instrument, provider and correlation_id (the run id). The existing log calls route through it.
6. **Metrics interface:** counters and gauges for connection state, event rate, queue depth, latency and errors. In-process for now; exported in Phase 17.
7. **Schema migration policy:** add a `schema_migrations` table and versioned SQL. Bumping a Parquet schema version requires a migration note and a regeneration command.
8. **Tooling:** `ruff check`, `ruff format`, `mypy --strict` on new modules, and a `make check` / `scripts/check.sh` that runs all three plus pytest.

**How.** Keep the CLI commands identical. Add `goldflow --profile {research,test,live}`; live refuses to start until the Phase 16 gates pass.

**Deliverables.** The items above plus `docs/ADR-0002-provider-contract.md`.

**Acceptance criteria.**
- A fresh setup from the README succeeds.
- The app validates config and starts in non-trading mode.
- Unit tests pass; lint and type checks pass.
- No secrets are in the repo.
- The dummy adapter exercises the contract and is refused outside tests.
- Phase 3 `validate` still produces digest `7eb02d13…` and Phase 4 replay still produces `eeacba10…`. This proves the refactor changed no results.

**Pitfalls.** Refactoring the Binance code can silently change results, so rerun both digests. Don't add technologies "because the list says so".

---

### Phase 2: Binance adapter and six-month historical acquisition. **Status: Done (M1)**
**Objective.** Download at least six months of verified historical data.

**How it was built** (`goldflow/acquire.py`, `vision_archive.py`, `rest_client.py`, `checkpoint.py`):
- **Archive-first:** files on data.binance.vision are discovered through the S3 listing, never assumed.
- **Units:** monthly plus daily per dataset. With `prefer_daily`, daily files are canonical and monthly files are cross-checks.
- **Download:** Range-resume to `.part`, atomic rename, SHA-256 checked against Binance `.CHECKSUM`.
- **ETag tracking:** a republished file moves the old copy to `superseded/`.
- **Retry:** bounded concurrency, exponential backoff with jitter, 429/418 Retry-After.
- **REST:** weight-budgeted; HTTP 451 is recorded as a geo-block and never worked around.
- **REST resume:** position comes from file content, so a page can never be duplicated.
- **Records:** every missing file or interval is a row with its reason. The manifest is regenerated after each run.

**Evidence.**
- 927 files (≈4 GB) verified.
- The 30 Oct tail was recovered via REST from the PC.
- 28 Oct aggTrades are permanently unavailable (never archived, and outside Binance's 2-day REST window).
- Spec verified live from the PC.

**Commands.** `python -m goldflow.cli discover | acquire --months 6 | rest | manifest`. On the PC, every command runs through `scripts/pc_run.sh`.

**Maintenance.**
- Re-run `acquire` after Binance republishes the 29 Oct file. ETag detection triggers a re-download and the Phase 3 status updates.
- With an API key (`BINANCE_API_KEY`, MARKET_DATA permission), `historicalTrades` can fill 29 Oct 21:00–24:00 within one month of the date.

---

### Phase 3: Historical validation, normalisation and storage. **Status: Done (M1)**
**Objective.** A trustworthy, queryable repository without altering source records.

**How it was built** (`schemas.py`, `normalize.py`, `catalog.py`, `dataset_report.py`; spec in `docs/SCHEMAS_V1.md`):
- **Canonical schemas v1:** TradeEvent, AggTradeEvent, DepthBandSnapshot, MarketMetrics, Kline1m, DepthEvent (L2, defined but with no data). Every field is labelled reported, derived, constant or reference.
- **Validation:** 19 reason codes, first failure wins. They cover tick/step exactness, kline-range price plausibility, timestamp bounds, id/time sequence outliers, and exact versus conflicting duplicates. Invalid records go to quarantine Parquet.
- **Completeness evidence:** per-minute kline count and volume, plus aggTrade quantity per row. Day and hour status: COMPLETE, PARTIAL, UNVERIFIED or MISSING. AggTrades whose fills straddle midnight are resolved across days.
- **Recovery:** single-trade aggTrades absent from the trades file are added as RECOVERED rows.
- **Storage:** atomic Parquet writes; a transactional SQLite catalog; a resumable, budgeted `normalize`; `validate` recomputes into a scratch catalog and compares digests.

**Evidence.**
- 912 partitions.
- 0 quarantined.
- Determinism proven across three runs, and across cloud versus PC.
- Monthly-file contamination found and documented.

**Commands.** `python -m goldflow.cli normalize | validate [--resume] [--budget S] | report`.

---

### Phase 4: Historical event replay engine. **Status: Done (M2)**
**Objective.** Emit historical events exactly when they would have become available, deterministically.

**How it was built** (`goldflow/replay.py`; spec in `docs/REPLAY_V1.md`):
- **Event key:** `(availability_ms, rank, seq)`.
- **Availability:** time column + offset + latency. A candle is available at close + 1 ms.
- **Tie order:** quality, candle, metrics, depth, aggTrades, trades.
- **Processing:** day-window vectorised merge.
- **Controls:** pause, resume, stop, speed, step-event, step-time, seek.
- **Checkpoints:** cursor and consumer state stored atomically; resume is refused if the dataset version or config hash changed.
- **Data-quality events:** DQ_BEGIN and DQ_END from the catalog (suppress-only).
- **Hashing:** a ChainHasher digest independent of chunking and resume boundaries.

**Evidence.**
- 155,997,644 events.
- Same digest across uninterrupted, resumed and different-chunking runs, on two machines.
- 1.8–2.8 M events/s; ≤ 1.26 GB memory.

**Commands.** `python -m goldflow.cli replay --name r1 [--resume] [--start --end --dates --session --speed --until --budget --streams]`.

**Interface for later phases.** Subclass `goldflow.replay.Consumer` and implement `on_chunk(EventChunk)`, `get_state()`, `set_state()` and `on_seek()`. Live ingestion (Phase 14) must deliver the **same** `EventChunk` interface, so analytics never know whether they run on history or live.

---

### Phase 5: Trade processing, classification and price-level aggregation. **Status: Done (M3 begins)**
**Built** (`goldflow/processing/`; spec `docs/PROCESSING_V1.md`; report `docs/PHASE5_COMPLETION_REPORT.md`):
- `TradeProcessor(Consumer)`, the single path for replay and live: dedup (watermark plus id ring), exchange-reported sides (cross-checked against Phase 3), and an optional INFERRED tick rule kept separate.
- Integer footprint bars at 1s/5s/1m/5m/15m/1h, finalised only at `end + shift`.
- Windows: UTC day, trading day, trading week, sessions and custom ranges, using zoneinfo for DST.
- Snapshot/restore inside replay checkpoints, guarded by the settings hash. The 1m footprint is written to Parquet with a manifest, and `reconcile` checks it.

**Evidence (full six months, rebuilt in the cloud):**
- Replay digest `eeacba10…` unchanged; 657,749 events/s; 1.25 GB peak.
- 183/183 days: footprint = partition exactly.
- All 181 COMPLETE days equal Binance klines minute by minute (volume, count, taker-buy, OHLC).
- Processor state digest `ad595214…`, reproduced after stop/resume.
- 85 tests pass (`scripts/check.sh` green, with `goldflow.processing` under `mypy --strict`).

**Commands.** `python -m goldflow.cli process --name p1 [--resume] [--dates …] [--until …] [--budget S]`; `python -m goldflow.cli reconcile`.

**Open:** session times are PROPOSED (D4). No holiday calendar yet (Phase 10).

**Original specification:**
**Objective.** One shared event processor that turns trades into price-level and time-window aggregates. Every analytic in Phases 6–10 is built on its outputs.

**Depends on.** Phase 4 (the Consumer interface) and Phase 1 (settings, logging). The session config is drafted here and finalised in Phase 10.

**What to build** (`goldflow/processing/`):
1. **`TradeProcessor(Consumer)`.**
   - Accepts trade events from replay or live.
   - **Idempotent:** keeps a per-instrument watermark of the highest processed trade id, plus a bounded set of recent ids for out-of-order arrival. An event whose id is ≤ watermark and already in the set is ignored and counted as `duplicates_ignored`.
   - Links every aggregate back to its source trade ids (first/last id per bucket).
2. **Side classification** (`classify.py`): from `buyer_maker_flag`.
   - `false` → aggressor **BUY**; `true` → aggressor **SELL** (Binance semantics); missing → **UNKNOWN**.
   - `side_evidence` = EXCHANGE_REPORTED or UNKNOWN.
   - An inferred side (for example the tick rule) is allowed only as a separately labelled `INFERRED` field, off by default. It is never mixed into exchange-reported volume.
3. **Price-level aggregation** (`aggregation.py`).
   - **Key:** `price_ticks // bin_ticks`. `bin_ticks` is configurable, default 1 tick = $0.01; profile views typically use 10 = $0.10 or 50 = $0.50.
   - **Per level:** `buy_lots`, `sell_lots`, `unknown_lots`, `trade_count`, `buy_count`, `sell_count`, `notional`, `first_ts`, `last_ts`.
   - Integer lots (0.001 oz) and ticks only, so totals are exact.
4. **Time windows:** footprint bars at configurable intervals (1 s, 5 s, 1 m, 5 m, 15 m, 1 h). Each bar has OHLC from trades, volume, buy/sell lots, delta, trade count, and a per-price-level footprint (a sparse map). A bar is **finalised only when the clock passes its end**; until then it is "developing" and labelled so.
5. **Session manager** (`sessions.py`), versioned calendar config.
   - **Named time zones:** `Asia/Tokyo` or `Asia/Singapore` for Asia, `Europe/London`, `America/New_York`, with DST handled through zoneinfo.
   - **Defaults to propose:** Asia 23:00–08:00 UTC; London 07:00–16:00 Europe/London; New York 08:20–17:00 America/New_York (COMEX gold hours); Binance `tradingSchedule` COMMODITY REGULAR Sun 22:00 → Fri 21:00 UTC.
   - **Weekend:** the perpetual trades on weekends while gold is closed. Flag `weekend=True` and let strategies decide.
   - Daily (UTC and session-day), weekly and custom windows.
6. **Snapshot and restore:** `get_state` serialises the aggregates compactly (per-level arrays) for replay checkpoints. Restore followed by continue must equal an uninterrupted run.
7. **Outputs:** per-bar Parquet under `data/derived/…/footprint/interval=1m/date=…`, plus in-memory state for downstream analytics.

**How (performance).** Process chunk-vectorised: `np.bincount` / `np.add.at` on `price_ticks` per bar. The full replay must stay above 500 k events/s with aggregation on.

**Deliverables.** Processor, classifier, aggregation structures, session manager, derived footprint dataset, `docs/PROCESSING_V1.md` (definitions, units, session table).

**Acceptance criteria.**
- For every day, Σ bar volume = Σ partition `quantity_lots`, and Σ buy + Σ sell + Σ unknown = total, exactly in integers.
- Per-minute volume equals Binance klines on all COMPLETE days.
- Unknown-side volume stays unknown.
- Delivering the same events twice doesn't change totals.
- Snapshot/restore equals an uninterrupted run (state digest).
- Replay and (future) live call the same processor.
- DST transitions tested (late March and late October).

**Pitfalls.**
- Floating-point volume: use integer lots.
- Finalising bars before their end time (look-ahead).
- Mixing exchange-reported and inferred sides.
- Treating weekend perp flow like regular-session gold flow.

---

### Phase 6: Volume Profile, POC, Value Area, HVN/LVN and VWAP. **Status: Done (2026-10-31)**
**Objective.** The foundational profile and volume-weighted references, precisely defined and reproducible.

**Depends on.** Phase 5 aggregates and sessions.

**Definitions (to be versioned in `docs/METRICS_PROFILE_V1.md`).**
- **Volume unit:** base quantity in lots (0.001 oz). Notional profiles are optional and labelled separately.
- **Volume Profile `V(p)`:** Σ lots traded at price bin p within a window. Windows: session, daily, weekly, fixed-range (start, end), custom-range, and developing (grows with the clock).
- **POC:** the bin with maximum `V(p)`. **Tie-break:** the bin closest to the window's volume-weighted mean price; if still tied, the lower price. The rule is deterministic and documented.
- **Value Area** (target `va_pct`, default 70 %):
  1. Start at the POC.
  2. Compare the combined volume of the next **two** bins above with the next two bins below. Add the larger pair.
  3. If equal, add the pair on the side closer to the POC distance-weighted mean. If still equal, add both.
  4. Stop as soon as accumulated volume ≥ `va_pct` × total.
  5. Near the profile edge with fewer than two bins left, use what remains.
  6. VAH and VAL are the outermost included bins.
  7. A parameter switch allows the single-bin variant. The pair rule is the default because it is the CBOT-style convention.
- **HVN/LVN:**
  1. Smooth `V(p)` with a Gaussian kernel (σ = `smooth_ticks` bins, default 5).
  2. Find local maxima (HVN) and minima (LVN) on the smoothed profile.
  3. Keep peaks with prominence ≥ `prom_frac` × max(smoothed) (default 0.10).
  4. Keep nodes with volume ≥ `min_vol_frac` × mean (HVN) or ≤ `lvn_frac` × mean (LVN).
  5. Require at least `min_separation` bins between nodes.
  6. Report node price, width (where the curve crosses half-prominence), volume and prominence. There is no visual or arbitrary picking.
- **VWAP:** `Σ p_i q_i / Σ q_i` over trades in the window, using actual trade price and quantity. Variants: session, daily, anchored (anchor = a timestamp or event) and custom-anchored. It resets exactly at the anchor.
- **Bands:** volume-weighted population variance `σ² = Σ q_i (p_i − VWAP)² / Σ q_i`, with bands at VWAP ± k·σ (k ∈ {1, 2, 3}, configurable). It is computed incrementally with Σq, Σqp and Σqp², using integer ticks for exactness. Sample variance and bar-based deviations are not used unless explicitly switched and labelled.
- **Developing state:** profile, POC, VA, nodes and VWAP update incrementally. Time-stamped snapshots are stored at a configurable cadence (default every 1 minute plus at session end) for inspection and charts.

**Deliverables.** `orderflow/volume_profile.py`, `vwap.py`; snapshot dataset `data/derived/profile/…`; chart-ready JSON; reference tests.

**Acceptance criteria.**
- Synthetic profiles with hand-computed POC, VA and nodes match exactly.
- VWAP on real days matches an independent recomputation from raw Parquet within 1e-9 relative.
- Session resets and anchors are correct across DST.
- Parity: replay-built state equals state rebuilt from scratch at the same timestamp.
- Profiles reconcile to Phase 5 totals.

**Pitfalls.**
- VA "stop when ≥ 70 %" ambiguity, and tie handling: write them down and test them.
- Bin size changes POC: store `bin_ticks` with every result.
- Developing VWAP must not use trades after T.

---

### Phase 7: Delta, CVD, imbalance, Big Trades, absorption and exhaustion. **Status: Done (2026-10-31)**
**Built** (`goldflow/orderflow/` Phase 7 modules; spec `docs/METRICS_ORDERFLOW_V1.md`; report `docs/PHASE7_COMPLETION_REPORT.md`): `OrderflowEngine(ProcessorSink)` with flow bars (delta and intra-bar delta extremes), CVD per reset instance, diagonal/horizontal and stacked imbalance, taker-order reconstruction and Big Trades (absolute, notional, rolling and time-of-day quantiles on an exact histogram, every fill id stored), absorption, exhaustion and divergence with a CANDIDATE → CONFIRMED / INVALIDATED / EXPIRED lifecycle tracked strictly after the evidence window. Commands: `orderflow --name o1 [--resume]`, `orderflow-verify`. Evidence (4 real days in the cloud): exact verification 4/4 days, state digest reproduced after a stop/resume through the DST night, 296 k events/s. Six-month run: on the PC. Open: D11 (parameter review).

**Objective.** Transaction-based orderflow analytics with transparent classification and stated limits.

**Depends on.** Phase 5 (sides, footprint) and Phase 6 (levels for context).

**Definitions (`docs/METRICS_ORDERFLOW_V1.md`).**
- **Delta** = buy_lots − sell_lots per bar, level or session. **CVD** = cumulative delta, with resets at the session, day or anchor (configurable). Unknown-side volume is reported separately and never assigned.
- **Footprint imbalance (diagonal, default):** at price p in a bar, compare **buy volume at p** with **sell volume at p − 1 bin**. A *buy imbalance* if `buy(p) ≥ ratio × sell(p−1)` and `buy(p) ≥ min_lots`. A *sell imbalance* is the mirror. Defaults: ratio = 3.0, min_lots configurable.
  - Edge rule: if the comparison bin is empty, apply only with `zero_policy` (default: no imbalance).
  - **Horizontal imbalance** (same price) is available as an option.
  - **Stacked imbalance:** ≥ `stack_n` (default 3) consecutive bins with same-side imbalance in one bar.
- **Big Trades.** Binance splits a large taker order into many fills, so first reconstruct the **taker order**: consecutive trades with the same aggressor side and the same timestamp (or within `cluster_ms`, default 0 ms = same timestamp), over adjacent price bins.
  - **Thresholds (any or all, configurable):** absolute quantity ≥ X oz; notional ≥ Y USDT; rolling quantile ≥ q (default 99.5th) of taker-order size over the last N hours; time-of-day baseline (quantile per 30-min bucket over the trailing 20 sessions).
  - Store every event with its source trade ids.
- **Absorption** (candidate, never "proven"):
  - Within a window W at a level L (± `tol` bins), aggressive volume on one side ≥ `abs_vol` (absolute or quantile)…
  - …while price progresses ≤ `max_progress` bins in the aggressor's direction…
  - …and is **confirmed** only if price then moves ≥ `confirm_bins` against the aggressor within `confirm_window`.
  - Before confirmation it is labelled `ABSORPTION_CANDIDATE`; it becomes `ABSORPTION_CONFIRMED` only after the window, with no back-dating. A single large trade is never labelled absorption.
- **Exhaustion:** a new extreme made on declining aggressive volume (the volume of the last k pushes is falling) **and** a delta divergence at that extreme. Confirmed only after a pullback ≥ `confirm_bins`.
- **Divergence:** price makes a higher high (lower low) by swing definition (`swing_bars` left and right) while CVD or delta makes a lower high (higher low). The swing is confirmed only after the right-side bars close, so confirmation time = pivot + right bars. Invalidation is when price exceeds the pivot extreme by `inval_bins`.

**Deliverables.** `orderflow/delta.py`, `cvd.py`, `imbalance.py`, `big_trades.py`, `absorption.py`; a six-month event catalogue (Parquet) with counts by session; a validation report.

**Acceptance criteria.**
- Side uncertainty is visible in every output.
- No volume is silently assigned to bid or ask.
- Every event traces to its input trade ids.
- Events are reproducible (same digest across runs).
- Confirmation timestamps are always ≥ the evidence window end.

**Pitfalls.**
- Labelling absorption on stalls without a response test.
- Using swing points before they are confirmed (repainting).
- Treating aggTrades as taker orders. They are close but not identical, and they are incomplete on 39 days, so use trades.

---

### Phase 8: Historical and live order-book reconstruction and liquidity. **Status: Done (2026-10-31). Live only.**
**Built** (`goldflow/liquidity/`, `docs/METRICS_LIQUIDITY_V1.md`, `docs/PHASE8_COMPLETION_REPORT.md`): local book with the Binance sync procedure, raw recorder (`goldflow depth-record`), capture replay (`depth-replay`), DepthEvent v1 normalisation (`depth-normalize`), per-interval depth capability, book metrics, and one book per venue with RPI separate. Next step for you: start `depth-record` on the PC or a non-US VPS (D6) so depth history begins to accumulate.
**Objective.** Depth-based analytics **only** where genuine snapshots and incremental updates exist.

**Reality for us.** There is no historical price-level L2 for 2026-04-30 → 2026-10-30. Depth analytics can only be built from our **own live recorder**, so it should be started as early as possible (see Phase 14) to accumulate history. Paid history (for example Tardis.dev `incremental_book_L2`) is the alternative and needs your decision.

**What to build.**
1. **Binance futures local book** (`liquidity/book.py`), following the documented procedure:
   1. Subscribe to `<symbol>@depth@100ms`.
   2. Buffer events.
   3. Fetch the REST snapshot `/fapi/v1/depth?limit=1000` (`lastUpdateId`).
   4. Drop buffered events with `u < lastUpdateId`.
   5. The first applied event must satisfy `U ≤ lastUpdateId ≤ u`.
   6. Every next event must have `pu == previous u`. Otherwise the book is **invalid**: discard it, resnapshot and rebuild.
   7. A quantity of 0 removes the level.
2. **Recorder:** persist raw depth messages and snapshots (gzip JSONL, then Parquet DepthEvent v1) with sequence validity per interval. About 120 MB/day compressed (Phase 0 estimate).
3. **Book state and metrics:** best bid/ask, spread, depth within ±N ticks and ±x %, book imbalance `(bid_depth − ask_depth)/(sum)` per distance band, level additions and reductions, depletion near the touch, concentration (top-k level share).
   - Cancellations are **not** directly observable from L2 deltas. Label reductions without a matching trade as "reduction (cancel or hidden fill unknown)".
4. **Coverage labelling:** `depth_capability(interval)` is VALID only for intervals with a continuous valid book. Every depth-based metric and signal checks it.
5. **One book per provider, venue, instrument and market.** RPI liquidity (`rpiDepth`) is kept separate from the normal book.

**Acceptance criteria.**
- A simulated sequence gap invalidates and rebuilds the book.
- No depth-based signal is produced from an invalid or absent book.
- Capability is labelled per interval.
- A replay of the recorded stream reproduces the identical book state digest.

**Pitfalls.**
- Merging depth from several venues.
- Treating bookDepth bands as L2.
- Running the recorder from a US cloud host (HTTP 451).

---

### Phase 9: Auction Market Theory and failed-auction analytics. **Status: Done (2026-10-31)**
**Built** (`orderflow/auction*.py`, `failed_auction.py`; spec `docs/METRICS_AUCTION_V1.md`; report `docs/PHASE9_COMPLETION_REPORT.md`): `AuctionEngine(ProcessorSink)` after the `ProfileEngine`. Reference levels from the previous full profile instance (high, low, POC, VAH, VAL, VWAP, bands, HVN/LVN, user levels; exact rationals, never from a partial window); acceptance / return / rejection episodes; failed auctions with weak-participation evidence and a held return; outcome rows at 15/30/60 min; auction state (balance, imbalance, transition); initiative vs responsive activity; excess, poor extremes and a labelled single-print proxy. Commands: `auction`, `auction-verify`, `auction-review` (CSV + SVG charts from raw trades). Evidence (4 real days): exact independent re-derivation 4/4 days, state digest reproduced after stop/resume. Open: D12, IB levels (Phase 10).

**Objective.** Rule-based acceptance, rejection, auction state and failed-auction detection from measurable evidence.

**Depends on.** Phases 5–7 (profile, VWAP, delta) and Phase 10 sessions. The Phase 10 session calendar may be built first or in parallel.

**Definitions (`docs/METRICS_AUCTION_V1.md`; every threshold is configurable and versioned).**
- **Reference levels:** previous and current session high/low, POC, VAH/VAL, HVN/LVN, VWAP and bands, IB high/low, and user levels.
- **Acceptance outside a level L:** price spends ≥ `accept_time` (for example 15 min) **or** trades ≥ `accept_vol_frac` of the window volume beyond L, with closes (bar closes at the chosen interval) beyond L.
- **Rejection:** price trades beyond L by ≥ `probe_bins` and returns inside within `reject_time` without meeting the acceptance criteria.
- **Failed auction (high; the low is the mirror):** all must hold.
  1. A new high beyond a reference level, by ≥ `probe_bins`.
  2. Participation at the probe was weak: volume beyond the level ≤ `fa_vol_frac`, **or** responsive selling shows in delta (negative delta at the probe).
  3. Price returns inside the reference within `fa_return_time`.
  4. It is confirmed only after the return has held for `fa_hold` bars.
  - A wick alone is never enough.
  - Record the event time, confirmation time, invalidation level (the probe extreme) and outcome metrics (the move over the next N minutes), stored for review.
- **Balance versus imbalance (auction state):**
  - *Balance:* rotation inside a value area, with ≥ `rot_n` rotations between VAH and VAL and value roughly overlapping the previous value area (overlap ≥ `ov_frac`).
  - *Imbalance / price discovery:* acceptance outside the previous value with value migrating (POC shift ≥ `poc_shift_bins`).
- **Initiative versus responsive:** activity in the direction away from value (initiative) versus back toward value (responsive), measured by delta sign relative to the location versus value and VWAP.
- **Poor high/low:**
  - The extreme bin's volume is ≥ `poor_frac` × the mean volume of the top 3 bins near the extreme (a "flat" top, no tail), **and** no excess.
  - **Excess:** a tail of ≥ `excess_bins` bins at the extreme, each with volume ≤ `tail_frac` × the mean, formed within `excess_time` (a fast rejection).
  - **Single prints:** TPO-style single prints need a time-at-price grid (for example 30-min periods). Mark as *unsupported* if time-period profiles are not built. With trade data, a low-volume "single print" proxy is allowed if it is labelled as such.
- **No hindsight:** every state and event carries its confirmation time and is emitted only then.

**Deliverables.** `orderflow/auction.py`, `failed_auction.py`; an event catalogue with subsequent-outcome statistics per session; a reviewer-friendly CSV with links to the chart snapshots.

**Acceptance criteria.**
- Rules are documented and reproducible.
- No event is emitted before its confirmation time (tested by replaying with a clock check).
- A sample of events is reviewable against charts.

---

### Phase 10: Sessions, Initial Balance and IVB. **Status: Done (2026-10-31). IVB disabled until its definition (D1).**
**Built** (`orderflow/initial_balance.py`, `ib_verify.py`, `ivb.py`, `processing/holidays.py`; spec `docs/METRICS_IB_V1.md`; report `docs/PHASE10_COMPLETION_REPORT.md`): `IBEngine(ProcessorSink)` with per-session IB definitions (offset or local start; DST-checked at startup), developing and final IB with k × range extensions, breakouts (volume/delta filters reported), failed breaks, extension touches and session summaries; IB levels feed the Phase 9 auction engine (acceptance outside the IB); a PROPOSED holiday calendar that labels without changing Phase 5 sessions; IVB refused until a signed-off definition and implementation exist. Commands: `ib`, `ib-verify`. Evidence (4 real days): exact verification, digest reproduced after stop/resume.

**Objective.** Session-aware opening range and initial-session analytics.

**What to build.**
1. **Session calendar** (versioned YAML or JSON): named sessions with zoneinfo time zones, DST, the holiday calendar (CME/COMEX holidays for gold; weekends) and user sessions. Binance perpetual flow during gold closures is flagged.
2. **Initial Balance:** the configurable initial period (default the first 60 min of the selected session, for example New York 08:20–09:20 America/New_York or London 08:00–09:00 Europe/London).
   - Output: IB high, low, mid and range, plus developing values while forming.
   - **Extensions:** IB high + k × range and IB low − k × range for k ∈ {0.5, 1, 1.5, 2, 3}.
3. **IB behaviour:**
   - breakout: a close beyond the IB extreme;
   - acceptance outside: Phase 9 acceptance criteria;
   - failed break: a break followed by a return inside within the window;
   - range expansion.
   - Volume and delta conditions only as documented boolean filters.
4. **IVB: disabled until defined.** "IVB" has no single standard definition (it could mean Initial Value Balance, Initial Volume Balance, or a proprietary indicator). **You must supply:** the exact meaning, formula, inputs, time window and interpretation, ideally with a worked example or a chart. The module ships disabled and is never silently replaced by IB or another metric.

**Acceptance criteria.**
- Session boundaries are identical in replay and live.
- The DST transition weeks are tested (US and UK change on different dates).
- IVB is implemented only after formula sign-off.

---

### Phase 11: Unified strategy interface and signal generation. **Status: Done (2026-10-31)**
**Built** (`goldflow/strategies/`; spec `docs/SIGNALS_V1.md`; report `docs/PHASE11_COMPLETION_REPORT.md`): strategy contract (capabilities, inputs, triggers, typed parameters, warm-up, pure `build` from evidence), `StrategyEngine` after its analytics engines with time-ordered dispatch and suppress-only gating (capability refusal, WARMUP, DQ_OPEN, DQ_EVIDENCE, DUPLICATE_ACTIVE), regime features, grouped uncalibrated score, `TradingSignal` with exact R:R and evidence, three example research strategies with hypotheses. Commands: `signals`, `signals-verify` (re-derives every signal from its stored evidence). Evidence (4 real days): 5 signals, all re-derived; digest reproduced after stop/resume.

**Objective.** Strategies consume validated analytical state and emit explainable, timestamped signals, or emit nothing.

**What to build** (`strategies/`, `signals/`):
1. **Strategy contract:**
   - `required_capabilities` (for example `{historical_trades, aggressor_side}`; a depth-based strategy adds `l2_depth_valid`);
   - `config_schema`, `warmup` (time or events), `on_state(analytics, clock)` → `[TradingSignal]`;
   - `reset()`, `version`, `get_state()` / `set_state()`.
2. **Capability and data-quality gating** (engine-enforced):
   - No signal while a required capability is unavailable or a relevant **DQ interval is open** (from Phase 4 quality events or live feed health).
   - Gating may only **suppress**, never trigger.
3. **Regime features** (optional, documented, no look-ahead): realised volatility (5-min returns, trailing), trend versus rotation (value migration, POC drift), volume regime (volume versus its time-of-day baseline), and session context.
4. **Scoring:**
   - Combine features into a setup score with documented weights.
   - Account for correlation: group correlated evidence (for example delta and CVD) so it counts once.
   - A score is **not** a probability unless calibrated in Phase 13 (reliability curve on validation data).
5. **TradingSignal schema** (from the PDF):
   - signal_id, strategy_id and version, provider, instrument, event_time_utc, direction;
   - entry_reference (price plus type: market/limit), stop_loss, take_profit, R:R, risk_intent;
   - setup_score, evidence[] (each item: metric, value, threshold, timestamp, source ids), expiry or invalidation, required_data_capabilities, data_quality_status.
6. **Example research strategies (not assumed profitable):**
   - (a) failed auction at VAH/VAL with delta confirmation, target POC;
   - (b) absorption at an HVN or VWAP band with a confirmed reversal;
   - (c) IB failed break.
   - Each comes with a written hypothesis.

**Acceptance criteria.**
- The same inputs give the same signals.
- Every signal is explainable from its evidence list.
- Only information available at signal time is used (clock assertions).
- A strategy refuses unsupported inputs.

---

### Phase 12: Event-driven backtesting and execution simulation. **Status: Built (2026-10-31). Real results blocked on D2.**
**Built** (`goldflow/backtesting/`; spec `docs/BACKTEST_V1.md`; report `docs/PHASE12_COMPLETION_REPORT.md`): MT5 tick import (server time zone, quarantine, manifest), basis model (past-only rolling median, exact translation), `Backtester` sink on the replay clock after the `StrategyEngine`, fills on broker bid/ask with latency and slippage, gapped stops, unfavourable gap rule, seeded rejections, stops level, commission and swap, risk sizing and limits, exact cents ledger reconciled before every report, reproducibility record. Refuses to run without a configured broker; synthetic broker data only in tests and labelled NOT A RESULT. Commands: `broker-import`, `backtest`.

**Objective.** Backtest strategies over the dataset with the same analytics and strategy code as live, and with realistic, disclosed execution.

**Critical design point: signal venue ≠ execution venue.** Signals come from Binance XAUUSDT. Orders execute on your **MT5 broker's XAUUSD**, a different price feed with its own spread, swap, commission and hours. So the backtest must fill on **broker prices**.

**Prerequisites.**
1. **Broker tick history:** export XAUUSD ticks (bid/ask) for the same period from MT5. Use `CopyTicksRange` in an MQL5 script, or the MetaTrader5 Python package on Windows. Import it as a new provider dataset (`provider=broker:<name>`) through the Phase 3 validator, with its own capability flags (`bid_ask_quotes: AVAILABLE`).
2. **Basis model:** align the Binance perpetual and broker mid on a common clock. Measure the basis (perp − broker mid) distribution by session; Phase 0 found the perp ≈ +5.6 bps above the index on median. Signals are expressed as levels on Binance; orders are translated to broker prices through the **live basis at signal time**, with no future basis.

**What to build** (`backtesting/`):
- **Engine:** consumes replay events (Binance plus broker ticks merged by availability time) → the same Phase 5–11 consumers → signals → simulator. Warm-up is enforced.
- **Order simulator:**
  - market, limit and stop orders; latency (signal → broker, configurable, for example 150–500 ms) applied on the event clock;
  - **fills on broker bid/ask** (buy at ask, sell at bid), slippage model (fixed plus volatility-scaled), commission and swap from the broker spec;
  - partial fills off by default (retail size), rejections (simulated broker reject rate and stops-level rules).
  - **Stop/target ambiguity:** with tick data, the first touch decides. If ticks are missing for an interval, assume the **unfavourable** outcome (stop first) and count it.
- **Risk simulation:** position sizing (risk % of equity ÷ stop distance in broker contract terms), max exposure, max concurrent positions, daily loss limit, max drawdown stop, max consecutive losses, session filters.
- **Reproducibility record:** run id, dataset version (Phase 3 digest plus the broker dataset digest), config hash, code version, strategy version, execution model, starting capital, seed.
- **Outputs:** trade ledger (every fill with timestamps and source evidence), equity and balance series, run metadata.

**Acceptance criteria.**
- No look-ahead (clock assertions; basis taken only up to T).
- Same inputs give the same results.
- Execution assumptions are printed in every report.
- The ledger reconciles exactly to the performance statistics.

**Pitfalls.**
- Filling on Binance prices while trading at a broker.
- Using the mid price.
- Ignoring broker spread widening at the session open and during news.
- Choosing favourable stop/target ordering.

---

### Phase 13: Reports, validation, walk-forward and strategy research. **Status: Built (2026-10-31). Real studies blocked on D2.**
**Built** (`goldflow/research/`; spec `docs/RESEARCH_V1.md`; report `docs/PHASE13_COMPLETION_REPORT.md`): reconciled performance report with intervals, splits by regime and cost impact; fixed chronological splits and a once-only test-set lock; rolling walk-forward with stitched out-of-sample equity; stability, cost stress (real reruns with a widened spread), sensitivity heatmap and Monte Carlo; hash-chained research ledger with hypotheses first; approval criteria (`max_oos_drawdown` must be set, D8). Commands: `study`, `perf-report`, `ledger-verify`.

**Objective.** Decide whether a strategy is robust, not merely fitted.

**What to build** (`research/`):
1. **Performance report:** net and gross P&L, win rate, profit factor, expectancy (R and money), average win and loss, R distribution, max drawdown and recovery, consecutive losses, trade duration, frequency, splits by session, side and regime, and **cost impact** (gross versus net).
2. **Chronological splits** (proposed, adjustable before any testing):
   - development 2026-04-30 → 2026-08-31;
   - validation 2026-09-01 → 2026-09-30;
   - **untouched test** 2026-10-01 → 2026-10-29, opened **once** for the final candidate.
   - Data accumulating after 2026-10-30 is a second out-of-sample period.
3. **Walk-forward:** rolling optimise-on-past and test-on-next windows (for example 8 weeks in-sample, 2 weeks out-of-sample, step 2 weeks). Record each window's parameters and results. Report the stitched out-of-sample equity.
4. **Robustness:**
   - parameter sensitivity heatmaps (a plateau, not a peak);
   - cost sensitivity (spread × 1.5, slippage × 2);
   - regime splits;
   - trade-sequence Monte Carlo (bootstrap order → drawdown distribution);
   - minimum sample size (≥ 100 trades for any conclusion; state the confidence intervals).
5. **Research ledger:** every hypothesis, parameter set, data window, rejected variant and decision, written **before** results are seen, to limit selection bias.
6. **Approval criteria** (proposed; you set the final numbers):
   - out-of-sample profit factor ≥ 1.2 after costs;
   - positive expectancy in ≥ 70 % of walk-forward windows;
   - max out-of-sample drawdown ≤ your limit;
   - results stable under ±20 % parameter changes and 1.5× costs.

**Acceptance criteria.**
- All metrics reconcile with the ledger.
- The test set is not reused for optimisation (enforced by a lock file recording when it was opened).
- Reports separate in-sample, validation and out-of-sample.

---

### Phase 14: Real-time Binance ingestion and historical/live parity. **Status: Built (2026-10-31). Live REST needs a non-US host (D6).**
**Built** (`goldflow/live/`; spec `docs/LIVE_V1.md`; report `docs/PHASE14_COMPLETION_REPORT.md`): option (a) REST trade poller incl. RPI, normalised by the Phase 3 validator; raw capture first; gap suspicion with `historicalTrades` backfill or a `GAP` interval; `LiveBus` with the same `EventChunk` interface and a clock held at the last successful poll minus a grace; per-stream staleness (`STALE` blocks signals); late trades excluded everywhere and counted; checkpoints with `RECOVERY` on restart; WebSocket monitor (aggTrade, markPrice). Parity harness: **exactly equal on 26 outputs over 3,197,220 real trades**. Found: the legacy `fstream.binance.com/stream` URL is silent; depth is on `/public/stream`, aggTrade/markPrice on `/market/stream` (Phase 8 default fixed). Commands: `live-run`, `parity`.

**Objective.** Bring the pipeline online without a second, inconsistent implementation.

**Host.** It must run where Binance is reachable: the trading PC (India) or a non-US VPS. US cloud hosts get HTTP 451.

**Critical parity issue: trades versus aggTrades live.**
- Historical analytics use **individual trades**, which include RPI trades.
- Per the Phase 0 notes, the WebSocket `aggTrade` stream **excludes RPI trades** since 2025-12-31, and **no individual-trade WebSocket stream is documented**.
- So live and historical flow can differ.

**Decide before building:**
- (a) **Live individual trades by REST polling:** `/fapi/v1/trades` returns the last 1,000 trades and has `isRPITrade`; weight 5, poll about every 0.5–1 s with id-continuity checks; plus `historicalTrades` (API key) to backfill gaps. History and live then share one definition.
- (b) Define the canonical flow as **non-RPI aggTrades** for both history and live. This would require rebuilding the history from aggTrades, which are incomplete on 39 days.
- **Recommendation: (a).** Measure the RPI share of volume on history (from `isRPITrade` on recent REST data) to quantify the difference.

**What to build** (`live/`):
- **WebSocket client:** reconnect with bounded backoff, 24 h reconnection handling, ping/pong, and per-stream sequence checks. Streams: depth (Phase 8), aggTrade (monitoring and cross-check), markPrice, and tradingSession.
- **REST trade poller** (option a), with id-gap detection and automatic backfill. Unrecoverable gaps are recorded and gated.
- **Raw capture first:** every payload is persisted with receive time before transformation, then normalised by the **same** Phase 3 validator.
- **Live event bus** that produces the **same `EventChunk`** interface as Phase 4. Analytics and strategies are unchanged.
- **Feed health:** staleness timers per stream. A stale or invalid feed opens a live DQ interval and blocks dependent signals.
- **Restart recovery:** checkpoints and session state persisted. On restart, backfill from the last durable trade id before resuming signals.
- **Parity harness:** capture N hours of live events, then replay the capture through (1) the live path in simulation and (2) the historical path. Compare analytical state and signals within tolerances (exact for counts and volumes).

**Acceptance criteria.**
- Stale feeds are visible and block signals.
- The book is rebuilt after sequence loss.
- The same captured events give equivalent calculations (parity report).
- The live trade definition matches history (documented RPI policy).

---

### Phase 15: Additional providers and cross-market mapping. **Status: Done (2026-10-31).**
**Built** (`goldflow/venues/`, `goldflow/providers/{okx,bybit,bitget}/`; spec `docs/VENUES_V1.md`; report `docs/PHASE15_COMPLETION_REPORT.md`): per-venue identity, partitions and catalogs; mapping records checked against live discovery; OKX (contiguous ids, provable completeness, UTC+8 archive days) and Bybit (RPI flag; UNVERIFIED) archive import through the Phase 3 validator; the unchanged Phase 4–11 stack on each venue, reconciled; basis series (1 s, as-of strictly before, exact) and a labelled cross-venue ounce aggregate; `providers-report`. Real data, 4 days: OKX 509,207 trades (COMPLETE), Bybit 471,842 (RPI 17.8 % of volume); Binance trades $0.5–1.0 below both. Commands: `venue-import`, `venue-run`, `basis`, `volume-oz`, `providers-report`.

**Objective.** Add sources without rewriting analytics or merging markets.

**Candidates (from Phase 0).**
- **Bitget `XAUUSDT`:** 100-level REST book.
- **OKX `XAU-USDT-SWAP`:** a 5,000-level book, the deepest public gold-perp book.
- **Bybit `XAUUSDT`.**
- For real institutional gold flow: **COMEX GC/MGC** via Rithmic, CQG or dxFeed (paid, needs an entitlement).
- Your **MT5 broker feed** (already needed in Phase 12).

**What to build.**
- One adapter per provider implementing the Phase 1 contract, with honest capability declarations.
- Instrument mapping records: source instrument, execution instrument, contract multiplier, quote currency, tick size, session and price basis.
- A cross-market basis module: time-aligned basis and spread series per venue pair.

**Rules.**
- Venue metrics stay separate.
- Any cross-venue aggregate (for example total gold-perp volume) must be explicitly designed, normalised to ounces, and labelled.
- Order books are never merged.

**Acceptance criteria.**
- A provider is added without changing the analytics interfaces.
- No artificial merged book or profile exists.
- Coverage and capability reports exist per provider.

---

### Phase 16: MT5 execution bridge and risk controls. **Status: Built (2026-10-31). Demo evidence pending (D2, D6).**
**Built** (`goldflow/execution/`, `mt5/GoldflowExecutor.mq5`; spec `docs/EXECUTION_V1.md`; report `docs/PHASE16_COMPLETION_REPORT.md`): HMAC-authenticated loopback protocol (EA connects; Common Files fallback), symbol validation against the broker block, broker-confirmed order lifecycle with idempotency keys in the order comment, pre-trade risk engine (limits from `backtest.risk`, stricter only; DQ gate cannot be disabled; latched emergency flatten), reconciliation (adopt broker changes, block on mismatch), hash-chained audit log, `demo` profile (DEMO accounts only) and `exec-gate` for the M7 record. 13 fault-injection tests against a simulated EA following the MQL5 rules. Live off by default.

**Objective.** Connect validated signals to MetaTrader 5 with broker-side confirmation and strict safety.

**Bridge design (recommended).** An **MQL5 EA executor** running in MT5 on Windows (the trading PC or a Windows VPS) plus a **local authenticated channel** to the Python engine.
- The channel is a localhost TCP socket with a shared-secret HMAC; a file-based queue is the fallback.
- Messages are JSON, schema-validated, carrying a unique `signal_id` and an idempotency key, with acknowledgements, timeouts and a heartbeat.
- Alternative: the MetaTrader5 Python package (Windows only). It is simpler, but the EA keeps execution logic broker-side and survives a Python crash.

**What to build.**
1. **Instrument mapping:** Binance XAUUSDT → broker symbol (for example `XAUUSD`, `XAUUSD.r`, `GOLD`). Validate contract size, tick value, volume step, min/max lot, stops level, trading hours and price basis from `SymbolInfo` at startup.
2. **Order lifecycle states:** CREATED → TRANSMITTED → ACCEPTED → (PARTIALLY_)FILLED → CLOSED, or REJECTED or CANCELLED.
   - Internal state changes **only** on broker confirmation (`OnTradeTransaction` events reported back).
   - The idempotency key goes in the order comment or magic number, so a retry never duplicates an order.
3. **Risk engine (pre-trade, every order):** risk per trade, max daily loss, max drawdown, max positions, max aggregate exposure, max consecutive losses, spread and slippage filters, data-quality gate (no live DQ interval open) and session filter. **Emergency stop** flattens all positions and blocks new orders.
4. **Position management:** SL/TP placement, partials, break-even and trailing as configured. Changes are confirmed broker-side.
5. **Reconciliation:** on start, reconnect and periodically, compare broker positions and orders with internal state. Any mismatch blocks trading and alerts. Unexpected broker-side changes (manual closes, stop-outs) are adopted and logged.
6. **Audit log:** every message, decision and broker response, with timestamps.
7. **Deployment gates:** live mode is **off by default**.
   - Demo first, for at least 2–4 weeks: all lifecycle paths exercised, slippage measured against the Phase 12 model, reconciliation clean.
   - Then explicit operator approval with a recorded switch.

**Acceptance criteria.**
- No order is assumed filled without broker confirmation.
- Duplicate submissions are prevented (fault-injection tests: retries, restarts, network drops).
- Risk limits block trades under invalid data or excess exposure.
- Live mode is off by default.

---

### Phase 17: Dashboard, observability, deployment and production hardening. **Status: Not started**
**Objective.** A complete operational view and controlled continuous operation.

**What to build.**
- **Dashboard** (a FastAPI backend with a React or lightweight frontend), bound to **real engine state only**:
  - **Market:** price, session, feed freshness, data health.
  - **Orderflow:** profile, POC, VAH/VAL, HVN/LVN, VWAP and bands, delta/CVD, imbalances, big trades, absorption, auction events, depth (where valid).
  - **Trading:** signals with evidence, positions, orders, risk usage, P&L, execution status.
  - **Research:** dataset coverage, validation reports, backtests, walk-forward, out-of-sample comparison.
  - **System:** API and WebSocket state, event rate, queue depth, latency, missing intervals, recovery state, database health, errors, MT5 bridge heartbeat.
- **Alerts:** stale feed, DQ interval opened, reconciliation mismatch, risk-limit hit, bridge down, disk low.
- **Deployment:**
  - service supervision (a Windows service or systemd on a VPS);
  - secrets via environment or keystore;
  - log rotation and retention;
  - **backups** of raw data, the catalog and ledgers, with tested restore;
  - PostgreSQL/TimescaleDB migration if concurrency requires it.
- **Stress tests:** high event rates (replay at max speed through the live path), disconnections, rate limits, database failure, process crash mid-order, broker outage.
- **Runbook:** start/stop, emergency stop, recovery after a crash, after a data gap and after a broker outage; and how to add a provider.

**Acceptance criteria.**
- No fabricated dashboard values.
- Critical errors and stale feeds are visible.
- Backup restoration is tested.
- Production readiness is documented, not assumed.

---

## 9. Cross-cutting systems

### 9.1 Data integrity: the eight layers (all phases)
1. **Source validation:** checksums, schema and value rules (built: Phases 2–3).
2. **Event identity and idempotency:** keys scoped by provider, market, instrument and source id (built for history; built for processing in Phase 5: watermark plus recent-id ring; built for live in Phase 14: the same processor behind the REST poller).
3. **Sequence validation where the source provides it:** trade ids are not contiguous on Binance (unused ids), so completeness is proven with **kline counts** rather than id gaps. Depth uses `U/u/pu`.
4. **Durable raw capture before transformation:** archive zips and REST JSONL (built); live capture (built, Phase 14).
5. **Normalised validation and quarantine** (built).
6. **Reconciliation and backfill:** aggTrades, klines, REST, midnight-straddle resolution (built); live backfill via historicalTrades (built, Phase 14).
7. **Capability and DQ gating downstream:** quality events (built); strategy gating (Phase 11); live staleness, gaps, late trades and recovery (built, Phase 14); risk engine (Phase 16).
8. **Audit reporting:** manifests, validation reports, run digests (built); execution audit (Phase 16).

**Rule.** When integrity is uncertain, mark the interval, block only the calculations and signals that depend on it, and keep the unaffected ones running.

### 9.2 No look-ahead rules (all phases from 4)
- Availability time governs everything; see `docs/REPLAY_V1.md`.
- Bars, profiles, swings and auction events are **developing** until their confirmation time, and are emitted with that time.
- Basis, volatility, quantile thresholds and regime features use trailing windows only.
- The test set is locked (Phase 13).

### 9.3 Determinism and parity
- Every dataset, run and backtest records its dataset digest, config hash and code version.
- Digest checks become regression tests. Today: Phase 3 `7eb02d13…` (PC catalog), replay `eeacba10…`, and Phase 5 processor state `ad595214…` (default processing/session settings `2b3dc7ba…`). Any refactor must keep them, or explain and re-baseline them deliberately.
- Live/historical parity is proven by the Phase 14 harness.

### 9.4 Risk (layered)
DQ gating (Ph11) → simulated risk (Ph12) → feed staleness (Ph14) → pre-trade risk engine and reconciliation (Ph16) → emergency stop and alerts (Ph17).

### 9.5 Testing strategy
- **Unit:** every rule and formula, with hand-computed synthetic cases.
- **Reference:** independent recomputation from raw Parquet on real days.
- **Regression:** the digests.
- **Property-based:** idempotency, order-independence of totals, snapshot/restore equality.
- **Data quality:** the gap and duplicate injection already used in Phase 3.
- **Parity:** Phase 14.
- **Execution:** fault injection (Phase 16).
- **Rule:** a phase isn't done unless its tests run in CI and on the PC.

### 9.6 Operations (current)
- The data lives on the PC at `Documents\python\goldflow`. Run steps through `scripts/pc_run.sh` in the Claude desktop VM.
- On that mount, SQLite journals are restricted and file deletion isn't permitted, so the working DB lives on VM local disk and a snapshot is saved to `data/state`.
- **Back up the folder.**
- The code is on GitHub (`HoN3YP0T/GoldXFlow`). Data is never committed.
- Long-term, a Windows VPS near the broker (for MT5) and a non-US host for Binance live feeds.

### 9.7 Security
- No secrets in git. `BINANCE_API_KEY` (read-only MARKET_DATA) comes from the environment.
- The MT5 bridge is authenticated and localhost-only.
- Live trading needs an explicit operator switch.
- The repository is **public**: no keys, account numbers or broker credentials may ever be committed.

---

## 10. Milestone gates

| Gate | Must be true before proceeding | Status |
|---|---|---|
| M1 Historical data | Six months downloaded, or the exact limitation documented; manifest and integrity report | **Passed** |
| M2 Replay foundation | Full dataset replays deterministically with resumable checkpoints | **Passed** |
| M3 Analytics | Every supported metric has a documented formula, unit tests and reference verification | Phases 5–10 |
| M4 Backtesting | Chronological backtests with realistic, disclosed execution | Phase 12 |
| M5 Research | Out-of-sample and walk-forward results; no claim of guaranteed profitability | Phase 13 |
| M6 Live ingestion | Live shares code with replay and passes parity tests | Phase 14 |
| M7 MT5 demo | Broker-side orders confirmed, reconciled and risk-controlled in demo | Phase 16 |
| M8 Production | Recovery, stress, backup/restore, monitoring and emergency stop tested | Phase 17 |

---

## 11. Open decisions that block later phases

| # | Decision | Needed by | Who | Recommendation |
|---|---|---|---|---|
| D1 | **IVB definition** (formula, inputs, window, meaning) | Phase 10 | You | Provide a definition and a worked example; until then IVB stays disabled |
| D2 | **Execution broker and symbol** (name, account type, XAUUSD spec, commission) | Phase 12 | You | Use the broker you will trade live; export its tick history |
| D3 | **Live trade definition (RPI policy)** | Phase 14 (affects parity) | Joint | REST polling of individual trades (option a) |
| D4 | **Session definitions** (which sessions, which IB window) | Phase 5/10 | You | COMEX hours + London + Asia, with New York IB = the first 60 min from 08:20 New York |
| D5 | **Historical L2:** buy (Tardis.dev) or record live only | Phase 8 | You | Start the free live recorder now; decide on purchase after Phase 13 shows depth is needed |
| D6 | **Recorder and live host** (PC or VPS, non-US) | Phase 8/14 | You | Small non-US VPS for 24/7 recording; Windows VPS for MT5 |
| D7 | **Risk limits** (risk per trade, daily loss, max drawdown) | Phase 12/16 | You | Prop-style defaults: 0.5 % per trade, 2 % daily, 6–8 % max drawdown |
| D8 | **Approval criteria numbers** | Phase 13 | You | See Phase 13 proposals |
| D9 | **Data backup location** | Now | You | External drive or cloud storage for `Documents\python\goldflow` |
| D10 | **Repository visibility** | Now | You | Private is recommended for strategy code |
| D12 | **Phase 9 auction thresholds** (probe bins, acceptance time/volume, failed-auction volume share and hold, state rules, shape fractions) | Phase 11/13 | You | Keep the v1 defaults for research; review with Phase 13 out-of-sample outcomes |
| D13 | **Phase 11 example strategy parameters** (stops, targets, minimum R:R, level types) | Phase 13 | You | Research defaults only; judge them out of sample, never by in-sample results |
| D11 | **Phase 7 detector parameters** (imbalance ratio/min lots, Big Trade quantile, absorption/exhaustion/divergence settings) | Phase 11/13 | You | Keep the v1 defaults for research; review them with Phase 13 out-of-sample results, never by in-sample outcome |

---

## 12. Phase completion report template and standing instruction

Every phase ends with a report containing:
1. Scope completed and not completed.
2. Files created, modified and deleted.
3. Architecture and data flow implemented.
4. Commands to install, run and test.
5. Actual test counts and pass/fail results.
6. Actual dataset counts, coverage and integrity findings.
7. Known limitations, missing capabilities, unresolved decisions.
8. Security and operational considerations.
9. Acceptance criteria: passed, failed or blocked, with evidence.
10. Recommended next phase. It is not started without instruction.

**Standing instruction (append to every phase request):**
> Execute only the current phase. First inspect the repository and identify what is already implemented. Do not rewrite working components unnecessarily. Implement real functionality; no placeholder calculations or fabricated market data. Run unit, integration, regression and data-integrity tests (including the digest regression checks). Inspect actual outputs, fix critical errors, update documentation, and provide evidence. If data or credentials are unavailable, document the limitation and stop the dependent task. Summarise the phase contents before starting. Stop after the phase with the completion report.

Completed reports: `docs/PHASE2_ACQUISITION_REPORT.md`, `docs/PHASE3_COMPLETION_REPORT.md`, `docs/PHASE4_COMPLETION_REPORT.md`, `docs/PHASE5_COMPLETION_REPORT.md`, `docs/PHASE7_COMPLETION_REPORT.md`, `docs/PHASE8_COMPLETION_REPORT.md`, `docs/PHASE9_COMPLETION_REPORT.md`, `docs/PHASE10_COMPLETION_REPORT.md`, `docs/PHASE11_COMPLETION_REPORT.md`, `docs/PHASE12_COMPLETION_REPORT.md`, `docs/PHASE13_COMPLETION_REPORT.md`.

---

## 13. Glossary

| Term | Meaning |
|---|---|
| **Aggressor / taker** | The side that crossed the spread. On Binance, `is_buyer_maker=true` means the seller was the aggressor. |
| **aggTrade** | Binance's aggregate of fills with the same price, side and taker order. Not individual executions. |
| **Availability time** | The moment an event would have been known live. Replay delivers events at this time. |
| **Capability flag** | A declaration of what a dataset or provider can support (for example historical L2: unavailable). |
| **CVD** | Cumulative volume delta: the running sum of buy − sell aggressive volume. |
| **DQ interval** | A data-quality annotation: a period whose data is partial, unverified or missing. |
| **Digest** | A SHA-256 fingerprint of a dataset's contents or of a replay's event sequence, used to prove reproducibility. |
| **Footprint** | Per-bar, per-price breakdown of buy and sell aggressive volume. |
| **HVN / LVN** | High and low volume nodes: peaks and troughs of the smoothed volume profile. |
| **IB** | Initial Balance: the high/low range of the first period of a session. |
| **POC / VAH / VAL** | Point of control (the price with most volume); value area high and low (bounds of the 70 % volume area). |
| **RPI** | Retail Price Improvement orders on Binance. Excluded from the aggTrade stream since 2025-12-31 (Phase 0 notes). |
| **Lots** | Integer quantity units of 0.001 oz, used so all volume arithmetic is exact. |
| **Ticks** | Integer price units of 0.01 USDT. |
