# goldflow: Binance XAUUSDT gold orderflow engine (Phases 1–6, 8)

Downloads and verifies historical market data for **Binance USDⓈ-M `XAUUSDT`** (TradFi perpetual on spot gold). Raw data is kept exactly as Binance published it; Phase 3 normalises it into validated Parquet partitions, Phase 4 replays it deterministically, Phase 5 turns the replayed trades into exact price-level and footprint aggregates, Phase 6 builds volume profiles (POC, value area, HVN/LVN) and VWAP with bands on top of them, Phase 7 adds delta, CVD, footprint imbalances, Big Trades (rebuilt taker orders) and absorption / exhaustion / divergence events, Phase 9 adds Auction Market Theory: acceptance, rejection and failed auctions at reference levels, auction state, activity and profile shape. Phase 10 adds the Initial Balance (with extensions, breakouts and failed breaks) and a holiday calendar; IVB stays disabled until it is defined. Phase 11 adds the strategy interface: research strategies turn that evidence into explainable, gated trading signals (proposals, not orders). Phase 12 adds the event-driven backtester that fills those signals on your broker's XAUUSD bid/ask (it waits for the broker specification and tick history, decision D2). Phase 13 adds the research layer: walk-forward, robustness checks, a once-only test set, a hash-chained research ledger and approval criteria. Phase 14 brings the same pipeline online in paper mode: live individual trades (REST polling, RPI included, as in the history), feed health that blocks signals, restart recovery, and a parity harness that proves the live path gives exactly the replay's results. Phase 15 adds other gold perpetual venues (OKX, Bybit; Bitget discovery) as separate markets with their own partitions and catalogs, the same analytics on each, cross-venue basis series and a labelled ounce volume aggregate. Phase 16 adds the MT5 execution bridge: an MQL5 executor EA, an authenticated local protocol, broker-confirmed order states, idempotent retries, a pre-trade risk engine with an emergency stop, reconciliation and an audit log. Live trading is off by default, and demo routing reaches demo accounts only.

**No market data is committed to this repository.** Rebuild it with `python -m goldflow.cli acquire` then `python -m goldflow.cli normalize`. The frozen range in `data/state/range.json` reproduces the exact dataset, and the partition digests in `data/manifests/dataset_manifest.json` let you verify it.

## Build guide
**[docs/BUILD_GUIDE.md](docs/BUILD_GUIDE.md)** is the complete phase-by-phase plan (Phases 0–17): what to build, how to build it, acceptance criteria, current status, goal traceability, milestone gates and open decisions.

## Setup and foundation (Phase 1)
```bash
pip install -e ".[dev]"                     # pinned compatible ranges from pyproject.toml (Python >= 3.10)
goldflow status                             # validates config; prints profile, trading mode (DISABLED), provider health, capabilities
goldflow providers                          # registered data providers and their declared capabilities
goldflow status --profile live              # refused (exit 2) until execution.live_trading_enabled AND the Phase 16 M7 gate
scripts/check.sh                            # ruff + format + mypy --strict (foundation) + pytest
```
- **Profiles:** `research` (default), `test` (synthetic providers allowed) and `live` (locked). Select with `--profile` or `GOLDFLOW_PROFILE`.
- **Configuration:** validated at startup (`goldflow/settings.py`). Every problem is listed and the CLI exits with code 2.
- **Logs:** JSON lines in `data/state/goldflow.jsonl` (with run, event and correlation ids). The console stays human-readable.
- **Providers:** implement `goldflow.providers.base.Provider` and declare capabilities honestly; see `docs/ADR-0002-foundation.md`.

## Layout
```
config/binance_xauusdt.json   symbol, datasets, range, concurrency, retry, REST limits
goldflow/
  rest_client.py    Binance futures REST: exchangeInfo/spec, aggTrades, trades, historicalTrades;
                    weight-based rate limiting (X-MBX-USED-WEIGHT-1M), 429/418 Retry-After, 451 = GeoBlocked
  vision_archive.py data.binance.vision: S3 listing (paginated), Range-resume download, SHA-256 .CHECKSUM check
  checkpoint.py     SQLite (WAL, synchronous=FULL): units, gaps, REST cursors, events, runs
  acquire.py        planner (monthly where whole month, else daily; missing -> explicit record + REST unit),
                    thread pool downloader, progress.json, republish detection, monthly->daily fallback
  integrity.py      streaming pyarrow verification (bounded memory): ids, gaps, dups, time order, values
  report.py         coverage of [start,end), cross-file id continuity, trades-vs-1m-klines cross-check, manifest
  replay.py         Phase 4 deterministic replay (EventChunk / Consumer interface)
  processing/       Phase 5: trade_processor.py (TradeProcessor consumer), classify.py, aggregation.py,
                    sessions.py (zoneinfo calendar), settings.py, footprint_store.py, reconcile.py
  orderflow/        Phase 6: volume_profile.py (POC, value area, HVN/LVN), vwap.py, engine.py (ProfileEngine sink),
                    store.py, rebuild.py (from-scratch rebuild), verify.py, export.py (chart JSON), settings.py
                    Phase 7: delta.py, cvd.py, imbalance.py, big_trades.py, absorption.py, exhaustion.py, divergence.py,
                    events.py, flow_engine.py (OrderflowEngine sink), flow_store.py, flow_verify.py, flow_settings.py
                    Phase 9: auction.py, failed_auction.py, auction_engine.py (AuctionEngine sink), auction_verify.py,
                    auction_review.py (CSV + SVG charts), auction_settings.py
                    Phase 10: initial_balance.py (IBEngine sink), ib_verify.py, ivb.py (disabled); processing/holidays.py
  strategies/       Phase 11: signal.py (TradingSignal), base.py (Strategy contract), regime.py, library.py (examples),
                    engine.py (StrategyEngine sink, gating), verify.py
  backtesting/      Phase 12: settings.py (broker spec, execution model), broker.py (MT5 tick import), basis.py,
                    simulator.py (fills, costs, risk, cents ledger), engine.py (Backtester sink, report)
  research/         Phase 13: metrics.py, ledger.py (research ledger, test lock), runner.py, walkforward.py, study.py
  live/             Phase 14: trades.py (REST trades -> TradeEvent), bus.py (LiveBus, FeedHealth), runner.py (LiveRunner), parity.py
  execution/        Phase 16: protocol.py (HMAC lines), orders.py, risk.py, reconcile.py, audit.py, bridge.py, gate.py, sim.py (simulated EA for tests)
  venues/           Phase 15: mapping.py (instrument mapping, venue configs), archives.py, ingest.py, run.py, crossvenue.py (basis, oz aggregate), report.py
  cli.py            discover | acquire | rest | manifest | normalize | validate | report | replay | process | reconcile |
                    vprofile | vprofile-verify | vprofile-export | orderflow | orderflow-verify | auction | auction-verify | auction-review | ib | ib-verify | signals | signals-verify | broker-import | backtest | perf-report | study | ledger-verify | live-run | parity | venue-import | venue-run | basis | volume-oz | providers-report | bridge-run | bridge-status | emergency-stop | emergency-reset | exec-gate | bridge-secret | audit-verify | ea-vector
docs/L2_ORDERBOOK_AVAILABILITY.md, SCHEMAS_V1.md, REPLAY_V1.md, PROCESSING_V1.md, METRICS_PROFILE_V1.md, METRICS_ORDERFLOW_V1.md, METRICS_AUCTION_V1.md, METRICS_IB_V1.md, SIGNALS_V1.md, BACKTEST_V1.md, RESEARCH_V1.md, LIVE_V1.md, VENUES_V1.md, EXECUTION_V1.md, PHASE*_REPORT.md
tests/test_core.py, test_phase3.py, test_replay.py, test_processing.py, test_profile.py, test_orderflow.py, test_auction.py, test_ib.py, test_strategies.py, test_backtest.py, test_research.py, test_live.py, test_venues.py, test_execution.py
data/raw/binance/futures_um/XAUUSDT/<dataset>/<daily|monthly|rest>/   raw zips + .CHECKSUM (+ _verify sidecars)
data/state/   acquisition.sqlite, range.json, plan-*.json, progress.json, acquisition.log, instrument_discovery.json
data/manifest.json
```

## Usage
```bash
pip install -r requirements.txt pytest
python -m goldflow.cli discover                 # instrument spec (REST) + archive availability
python -m goldflow.cli acquire --months 6       # first run freezes [start,end) in data/state/range.json
python -m goldflow.cli acquire                  # resume: verified files are skipped (size + ETag + status)
python -m goldflow.cli rest                     # REST-only recovery units (run where fapi.binance.com is allowed)
python -m goldflow.cli manifest                 # rebuild manifest.json from the checkpoint DB
```
Exit code 1 means a unit failed or the primary `trades` dataset has an uncovered interval. Nothing is skipped silently. Every missing file, id gap and uncovered interval is a row in the `gaps`/`units` tables and appears in the manifest with its reason.

## Rules enforced
- Range is half-open `[start, end)` in UTC ms. `end` is the acquisition timestamp, frozen on the first run.
- A file counts only when its SHA-256 matches Binance's `.CHECKSUM` **and** it passes streaming verification.
- If an upstream file is republished (ETag change), the old copy moves to `superseded/` and the change is logged.
- A monthly file that fails verification is kept, marked `rejected`, and its month is re-acquired from daily files.
- REST access returning HTTP 451 is recorded as a geo-block. The client never switches to an alternative host to get around it.

## Phase 3: validation, normalisation and storage
```bash
python -m goldflow.cli normalize            # validate + quarantine + write Parquet partitions + catalog + reports
python -m goldflow.cli validate             # recompute everything from raw, write nothing, compare result digest
python -m goldflow.cli normalize --dates 2026-10-29   # single days (midnight straddles at the edges stay PARTIAL)
python -m pytest -q tests
```
Outputs:
```
data/normalized/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/data_type=<trades|aggTrades|klines_1m|bookDepth|metrics>/date=YYYY-MM-DD/part-000.parquet
data/quarantine/...same partitioning...      invalid records with reason codes (created only when there are any)
data/manifests/dataset_manifest.json         coverage, partitions (sha256 + content digest), raw files by role, limitations
data/reports/validation_report.{md,json}     statuses, non-complete days/hours, monthly cross-check, run comparison
data/reports/quarantine_report.md, coverage_daily.csv, coverage_monthly.csv
```
Catalog tables (same SQLite DB): `raw_files`, `partitions`, `day_status`, `hour_status`, `quarantine_counts`, `raw_day_stats`, `crosscheck`, `validation_runs`.
See `docs/SCHEMAS_V1.md` for field definitions and reason codes, and `docs/ADR-0001-phase3-storage.md` for the storage decisions.

## Phase 4: deterministic replay
```bash
python -m goldflow.cli replay --name r1                         # full dataset at maximum speed
python -m goldflow.cli replay --name r1 --resume                # continue exactly after the last delivered event
python -m goldflow.cli replay --start 2026-10-29T13:00:00Z --end 2026-10-29T21:00:00Z --speed 60 --name ny
python -m goldflow.cli replay --dates 2026-07-09 --session 13:00-21:00 --streams trades klines_1m --name x
```
Prints the event counts per stream, the final simulation clock, throughput, peak memory, the sequence digest and integrity violations. See `docs/REPLAY_V1.md` for availability rules, tie order, controls and checkpoint semantics. Python API: `goldflow.replay.ReplayEngine` with `Consumer` subclasses.

## Phase 5: trade processing and price-level aggregation
```bash
python -m goldflow.cli process --name p1                  # replay -> TradeProcessor -> footprint Parquet -> reconciliation
python -m goldflow.cli process --name p1 --resume         # continue after --until / --budget / Ctrl-C (state is in the checkpoint)
python -m goldflow.cli process --name p1 --dates 2026-10-26
python -m goldflow.cli reconcile --interval 1m            # re-check the written footprint against partitions and 1m klines
```
Outputs:
```
data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/footprint/interval=1m/date=YYYY-MM-DD/{bars,levels}.parquet
data/derived/.../footprint/interval=1m/_manifest.json      per-date totals, digests, settings hash, source dataset version
data/reports/processing_reconciliation_1m.{md,json}       exact checks vs trades partitions and Binance 1m klines
```
Exchange-reported aggressor sides only (UNKNOWN is never reassigned), integer lots and ticks, duplicate-safe, bars finalised only after their end (no look-ahead), sessions in named time zones with DST, and snapshot/restore equal to an uninterrupted run. Settings live in the `processing` and `sessions` config blocks; the session times are **proposed** defaults (decision D4). See `docs/PROCESSING_V1.md`. Python API: `goldflow.processing.TradeProcessor` with `ProcessorSink` subclasses for downstream analytics.

## Phase 6: Volume Profile, POC, Value Area, HVN/LVN and VWAP
```bash
python -m goldflow.cli vprofile --name v1                 # replay -> TradeProcessor -> ProfileEngine -> profile Parquet -> verification
python -m goldflow.cli vprofile --name v1 --resume        # continue after --until / --budget / Ctrl-C
python -m goldflow.cli vprofile --name v1 --footprint     # also (re)write the Phase 5 footprint in the same pass
python -m goldflow.cli vprofile-verify --sample-hours 6   # rebuild snapshots from raw trades: exact parity, VWAP vs float64, Phase 5 totals
python -m goldflow.cli vprofile-export --window session:london:2026-10-26 --bin 10 [--as-of 2026-10-26T09:00:00Z]   # chart JSON
```
Outputs:
```
data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/profile/date=YYYY-MM-DD/{snapshots,nodes,levels}.parquet
data/derived/.../profile/_manifest.json          per-date rows and digests, settings hashes, smoothing kernel, source dataset version
data/derived/.../profile/charts/*.json           vprofile-export output
data/reports/profile_verification.{md,json}     parity and VWAP-tolerance report
```
- **Windows:** profiles and VWAP for every UTC day, trading day, trading week, session, custom range and anchor.
- **Snapshots:** developing snapshots every minute contain exactly the trades before `as_of`, so there is no look-ahead. A final snapshot is taken at each window end.
- **Exactness:** POC, value area (CBOT pair rule) and HVN/LVN are decided in exact integer or rational arithmetic. VWAP and the population-σ bands come from exact integer sums.
- **Settings:** the `volume_profile` config block. The defaults are **proposed** v1 parameters.

See `docs/METRICS_PROFILE_V1.md`. Python API: `goldflow.orderflow.ProfileEngine` (a `ProcessorSink`; `listeners=[...]` receive every snapshot, `current(kind, name)` returns the latest developing one).

## Phase 7: Delta, CVD, imbalance, Big Trades, absorption, exhaustion, divergence
```bash
python -m goldflow.cli orderflow --name o1                # replay -> TradeProcessor -> OrderflowEngine -> orderflow Parquet -> verification
python -m goldflow.cli orderflow --name o1 --resume       # continue after --until / --budget / Ctrl-C
python -m goldflow.cli orderflow-verify [--dates D ...]   # independent exact recomputation from the trades partitions
```
Outputs:
```
data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/orderflow/date=YYYY-MM-DD/
    {bars,cvd,imbalances,stacked,big_trades,events}.parquet
data/derived/.../orderflow/_manifest.json         per-date rows and digests, settings hashes, source dataset version
data/reports/orderflow_verification.{md,json}     exact checks + event catalogue by session
```
- **Sides:** exchange-reported only. `unknown_lots` is shown on every output and never assigned; bars with too much unknown-side volume are not used by the detectors.
- **Big Trades:** fills are first rebuilt into taker orders (same side, same millisecond, price moving with the aggressor). An order is big if it beats the configured thresholds (default: the 99.5 % rolling quantile of the previous 24 h). Every fill id is stored.
- **Events:** absorption, exhaustion and divergence are emitted when their evidence is complete, then tracked strictly afterwards to CONFIRMED, INVALIDATED or EXPIRED. Nothing is back-dated, and a single large trade is never absorption.
- **Settings:** the `orderflow` config block. All detector parameters are **proposed** v1 defaults (decision D11).

See `docs/METRICS_ORDERFLOW_V1.md` and `docs/PHASE7_COMPLETION_REPORT.md`. Python API: `goldflow.orderflow.OrderflowEngine` (a `ProcessorSink`; `listeners=[...]` receive `(table, rows)` as rows become available).

## Phase 8: live order book (L2) recorder and liquidity metrics
There is no historical price-level book for this dataset, so depth history starts the day the recorder starts. Run it where `fapi.binance.com` REST is reachable (the trading PC or a non-US VPS). A US host gets HTTP 451 on the snapshot, and the recorder stops with exit code 4.
```bash
pip install -e ".[live]"                         # adds websockets
python -m goldflow.cli depth-record              # run until Ctrl-C (or --duration S); every payload is captured raw first
python -m goldflow.cli depth-replay              # rebuild the book from the capture: digests, validity intervals, counters
python -m goldflow.cli depth-normalize           # DepthEvent v1 Parquet + validity intervals + data/reports/depth_coverage.{json,md}
python scripts/verify_depth_reference.py --seconds 120   # real-data check: diff-built book vs Binance's own top-20 view
```
The book follows Binance's documented snapshot + diff procedure (`U ≤ lastUpdateId ≤ u`, then `pu ==` previous `u`). Any gap, crossed book, bad payload or lost connection invalidates it until a new snapshot bridges. Depth metrics and signals refuse to run unless one continuous valid book covers their window. See `docs/METRICS_LIQUIDITY_V1.md`.

## Running on a restricted Windows-shared folder (Claude desktop Linux VM)
On mounts where SQLite journals fail and files can't be deleted, run every step through the wrapper:
```bash
GOLDFLOW_SNAPSHOT=0 scripts/pc_run.sh 165 acquire --no-rest --skip-reconcile   # repeat until "run result"
scripts/pc_run.sh 165 rest --skip-reconcile                                     # REST recovery (needs a non-US host)
GOLDFLOW_SNAPSHOT=0 scripts/pc_run.sh 165 normalize --budget 95 --no-report     # repeat until finalized
scripts/pc_run.sh 165 validate --budget 95 ; scripts/pc_run.sh 165 validate --resume --budget 95 ...
scripts/pc_run.sh 165 report --budget 95                                        # resumable monthly cross-check + reports
scripts/pc_run.sh 165 process --name pc-p5 --budget 130 ; scripts/pc_run.sh 165 process --name pc-p5 --resume --budget 130 ...
```
The working DB lives on local disk (`$HOME/gf_state`) and a consistent snapshot is saved to `data/state/acquisition.sqlite`. Files that would be deleted are moved to `data/_to_delete/`. Code updates are applied with `python3 scripts/apply_update.py update.tgz`.

## Phase 9: Auction Market Theory and failed auctions
```bash
python -m goldflow.cli auction --name a1                 # replay -> TradeProcessor -> ProfileEngine -> AuctionEngine -> auction Parquet -> verification
python -m goldflow.cli auction --name a1 --resume
python -m goldflow.cli auction-verify [--dates D ...]    # exact re-derivation from trades + catalogue with outcome statistics by session
python -m goldflow.cli auction-review --limit 120        # data/reports/auction_review.csv + SVG charts drawn from raw trades
```
Outputs: `data/derived/.../auction/date=YYYY-MM-DD/{levels,events,outcomes,states,activity}.parquet`, `.../auction/charts/*.svg`, `data/reports/auction_verification.{md,json}`, `data/reports/auction_review.csv`.
- **Levels:** the previous full session and trading-day high, low, POC, VAH, VAL and VWAP (optionally bands and HVN/LVN), plus user levels. Prices are exact. A level is used only after its window has closed.
- **Events:**
  - acceptance (time or volume with closes beyond), return and rejection;
  - failed auctions: a probe, weak participation or responsive delta, a quick return, and a held return;
  - outcome rows at 15/30/60 min, for review only.
- **Context:** balance / imbalance / transition state every 15 minutes, initiative vs responsive activity per bar, excess, poor highs/lows and a labelled single-print proxy.
- **Settings:** the `auction` config block; thresholds are **proposed** (decision D12). See `docs/METRICS_AUCTION_V1.md`.

## Phase 10: Sessions, Initial Balance and IVB
```bash
python -m goldflow.cli ib --name i1 [--resume]           # replay -> TradeProcessor -> IBEngine -> initial_balance Parquet -> verification
python -m goldflow.cli ib-verify                         # exact recomputation from trades
```
- **IB:** New York 08:20–09:20 New York time and London 08:00–09:00 London time by default (PROPOSED, D4). DST-safe. Developing snapshots every minute, then the final high, low, mid and range with 0.5–3 × range extensions.
- **IB events:** breakouts (volume and delta filters are reported, not applied), failed breaks, extension touches and a session summary.
- **Auction link:** IB high, low and mid are Phase 9 reference levels. Acceptance outside the IB is Phase 9's acceptance on them.
- **Holidays:** the `holidays` block labels IB rows and skips the IB on CLOSED dates. It is **PROPOSED**: verify it against CME's published schedule.
- **IVB:** **disabled**. `ivb.enabled: true` is refused until its definition is supplied and implemented (decision D1). See `docs/METRICS_IB_V1.md`.

## Phase 11: Strategy interface and signals
```bash
python -m goldflow.cli signals --name s1 [--resume]      # full analytics pipeline in memory -> StrategyEngine -> signals Parquet -> verification
python -m goldflow.cli signals-verify                    # re-derives every signal from its stored evidence
```
- **Signals:** each signal has an entry, stop, target, exact R:R, an uncalibrated score, an expiry and an evidence list (every item with its timestamp and source ids). They are in Binance ticks and are proposals, not orders.
- **Gating:** suppress-only. Unsupported capabilities are refused at start; signals are suppressed during warm-up, open data-quality intervals, non-OK evidence and duplicate active setups.
- **Example research strategies** (not assumed profitable): failed auction at VAH/VAL, absorption at an HVN or VWAP band, and IB failed break. Parameters are **proposed** (D13). See `docs/SIGNALS_V1.md`.

## Phase 12: Backtesting on broker prices
```bash
# 1. fill in the "broker" block of config/binance_xauusdt.json from MT5 (Symbols -> XAUUSD -> Specification), incl. server_tz
# 2. export XAUUSD ticks from MT5 (Symbols -> Ticks -> Export, or a CopyTicksRange script) and import them:
python -m goldflow.cli broker-import --file XAUUSD_ticks.tsv --format mt5
# 3. run the backtest (same analytics and strategy code as research/live):
python -m goldflow.cli backtest --name b1 [--resume]     # -> data/backtests/b1/{orders,fills,trades,cash,equity}.parquet, run.json, report.md
```
- **Prices:** fills on broker **bid/ask** only (never Binance). Signal levels are translated with the Binance-to-broker price gap (basis) known at signal time.
- **Execution model:** latency, slippage, gapped stops, stop-first on ambiguous gaps, commission, swap and broker rejections. It is printed at the top of every report.
- **Risk:** sizing and limits (decision D7), and every result reconciled to the cent against the cash ledger.
- **Without a broker:** until the `broker` block is filled in (decision D2), `backtest` refuses to run. See `docs/BACKTEST_V1.md`.

## Phase 13: Research, walk-forward and validation
```bash
python -m goldflow.cli perf-report --name b1             # full performance report (intervals, splits, Monte Carlo) of a backtest
python -m goldflow.cli study --plan plan.json --name s1  # hypothesis -> walk-forward -> robustness -> (once) test -> approval -> report
python -m goldflow.cli ledger-verify                     # research ledger hash chain
```
- **Splits:** development 30 Apr–31 Aug, validation September, test October. The **test set opens once**, for one final candidate (`data/research/test_lock.json`).
- **Validation:** walk-forward (8 weeks in-sample / 2 weeks out-of-sample), ±20 % parameter stability, costs ×1.5, sensitivity heatmap, Monte Carlo.
- **Research ledger:** hypotheses are recorded before any results.
- **Approval:** thresholds are proposed (D8). `research.approval.max_oos_drawdown` must be set before anything can pass. See `docs/RESEARCH_V1.md`.


## Phase 14: Live ingestion (paper mode) and historical/live parity
```bash
python -m goldflow.cli live-run --name L1 [--duration 3600]   # REST trade poller + WebSocket monitor -> the same analytics and strategies (signals only)
python -m goldflow.cli parity --start 2026-10-23T00:00:00Z --end 2026-10-27T00:00:00Z   # live path vs replay, exact comparison
python -m goldflow.cli parity --start ... --end ... --from-capture                      # the same, on trades recorded by live-run
```
- **Host:** Binance REST answers HTTP 451 to US hosts. Run `live-run` on the trading PC or a non-US VPS (D6). An API key (MARKET_DATA) enables gap backfill.
- **Trade definition:** individual trades incl. RPI, as in the history (D3 option a). `aggTrade` is monitoring only.
- **Safety:** stale feeds, gaps, late trades and restarts open data-quality intervals that block signals until they close.
- **WebSocket endpoints:** depth on `wss://fstream.binance.com/public/stream`, aggTrade/markPrice on `/market/stream` (the old `/stream` path is silent). See `docs/LIVE_V1.md`.


## Phase 15: Other venues (separate markets) and cross-market series
```bash
python -m goldflow.cli venue-import --provider okx --dates 2026-10-23 2026-10-24     # archive -> raw kept -> Phase 3 validator -> provider=okx partitions + catalog
python -m goldflow.cli venue-import --provider bybit --start 2026-10-23 --end 2026-10-27
python -m goldflow.cli venue-run --provider okx --name v1                            # the unchanged analytics/strategy stack on OKX's data, reconciled
python -m goldflow.cli basis --pair binance:okx --dates 2026-10-23 2026-10-26        # 1 s basis series (last trade strictly before; 5 s staleness)
python -m goldflow.cli volume-oz --venues binance okx bybit --dates 2026-10-23       # labelled CROSS_VENUE_AGGREGATE in troy ounces
python -m goldflow.cli providers-report                                              # capabilities, reachability, spec check, coverage per provider
```
- **Never merged:** each venue has its own instrument id, partitions, catalog and processor. Cross-venue outputs are separate and labelled.
- **Completeness:** OKX ids are contiguous, so its days are COMPLETE or PARTIAL. Bybit cannot be proven, so its days are UNVERIFIED and its signals gated. Bitget history is not available from this host. See `docs/VENUES_V1.md`.


## Phase 16: MT5 execution bridge (demo first; live off by default)
```bash
python -m goldflow.cli bridge-secret --out "%APPDATA%\MetaQuotes\Terminal\Common\Files\goldflow\bridge.secret"   # then set bridge.secret_file
python -m goldflow.cli bridge-run --profile demo --name demo1      # live pipeline + bridge; the EA (mt5/GoldflowExecutor.mq5) connects
python -m goldflow.cli emergency-stop --name demo1 --reason "..."  # flatten + block; emergency-reset --name demo1 --operator NAME
python -m goldflow.cli exec-gate --name demo1 --operator NAME      # M7: reads the demo audit log; writes data/state/live_gates.json only if it passes
```
- **Setup:** fill in the `broker` block (D2), then compile the EA in MetaEditor, add `127.0.0.1` to the allowed URLs, and attach it to an XAUUSD chart of a **demo** account.
- **Safety:**
  - orders change state only on broker confirmation;
  - retries reuse one idempotency key;
  - the risk engine blocks on data quality, spread, loss limits, exposure and invalid stops;
  - reconciliation blocks on any unexplained difference;
  - everything is in a hash-chained audit log.
- See `docs/EXECUTION_V1.md`.
