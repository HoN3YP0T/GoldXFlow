# Phase 7 completion report: delta, CVD, imbalance, Big Trades, absorption and exhaustion

Date: 2026-10-31. Instrument: Binance USDⓈ-M XAUUSDT. Input: the Phase 5 trade processor on the Phase 4 replay of the Phase 3 dataset. Specification: `docs/METRICS_ORDERFLOW_V1.md`.

**Order of phases.** Phase 7 was built before Phase 6, as the owner requested. The guide lists Phase 6 only as "levels for context". No Phase 7 definition needs POC, value area, HVN or VWAP: absorption works at any price level. Phase 6 levels can be attached later as extra context fields without changing these results.

## 1. Scope completed / not completed
**Completed** (every definition in the guide's Phase 7 section):
- **Delta and CVD.** Delta per bar, level and window. CVD with `utc_day`, `trading_day`, `trading_week`, session and anchor resets. Unknown-side volume is carried separately everywhere and never assigned.
- **Footprint imbalance.** Diagonal (default) and horizontal modes, a zero-volume policy, stacked imbalance, and exact rational ratio comparison.
- **Big Trades.** Taker-order reconstruction from consecutive trades, not aggTrades. Thresholds: absolute quantity, notional, a trailing nearest-rank quantile and a time-of-day baseline, combined with any/all. Each event stores its full source trade ids and the evidence for every criterion.
- **Absorption.** Effort with no result at a level, from at least 3 distinct taker orders, so a single trade is never absorption. Labelled `CANDIDATE`, then `CONFIRMED`, `FAILED`, `AMBIGUOUS` or `EXPIRED` at its own later time, never back-dated.
- **Exhaustion.** A new extreme on declining aggressive volume with delta divergence. Confirmed only after a pullback.
- **Divergence.** Price against CVD and price against delta, on swings confirmed only after the right-hand bars close (no repainting). Invalidation is tracked.
- **Six-month event catalogue** (Parquet plus manifest), counts by session, a validation report, and an independent real-data validator.
- **CLI:** `orderflow` (resumable) and `orderflow-validate`. 22 new tests.

**Not completed / by design**
- **Phase 6 context levels:** to be added when Phase 6 exists.
- **Thresholds are research defaults, not tuned.** Phase 13 decides whether any label carries information. Nothing here claims profitability.
- **No resting-liquidity evidence.** Absorption and exhaustion are inferred from trades and price only; there is no L2 history (Phase 8 now records it live).

## 2. Files
**Created**
- `goldflow/orderflow/__init__.py`, `settings.py`, `rolling.py`, `delta.py`, `cvd.py`, `imbalance.py`, `big_trades.py`, `absorption.py`, `exhaustion.py`, `divergence.py`, `store.py`, `engine.py`, `report.py`, `validate.py`
- `tests/test_orderflow.py` (22 tests)
- `docs/METRICS_ORDERFLOW_V1.md`, `docs/PHASE7_COMPLETION_REPORT.md`
- `data/reports/orderflow_catalogue.{md,json}`, `data/reports/orderflow_validation.{md,json}` (from the six-month run)

**Modified**
- `goldflow/processing/trade_processor.py`: two additive sink hooks, `on_trades` (accepted trades after dedup) and `on_advance` (time floor). The Phase 5 processor state digest on the full dataset is unchanged (§6).
- `goldflow/settings.py`: validates an optional `orderflow` block at `Config.load`.
- `goldflow/cli.py`: `orderflow` and `orderflow-validate`.
- `pyproject.toml`, `scripts/check.sh`: `goldflow/orderflow` added to mypy `--strict` and the format gate.
- `README.md`, `docs/BUILD_GUIDE.md`.

**Unchanged:** the config file and its hash (`9d77c52f…`), every Phase 3 `code_version` module, `replay.py`.

**Deleted:** none.

## 3. Architecture
```
ReplayEngine ─► TradeProcessor (Phase 5) ─┬─ on_trades(accepted trades) ─► TakerOrderBuilder ─► BigTradeDetector
                                          │                                              └─► AbsorptionDetector (orders)
                                          ├─ on_advance(time floor) ─► close orders, expire candidates
                                          └─ on_bars(interval, bars, footprint levels)
                                               1m: ResetCVD (cvd table), imbalances (+ stacked events), ExhaustionDetector
                                               5s: AbsorptionDetector      5m: DivergenceDetector (own CVD)
OrderflowAnalytics: labels (sessions, weekend, DQ, coverage), invariant check, OrderflowStore (parts at checkpoints
→ date partitions + manifest), state inside the Phase 5/replay checkpoint (refused if the settings changed)
```

## 4. Commands
```bash
pip install -e ".[dev]"
python -m goldflow.cli orderflow --name o1            # six months: about 6.5 min in the cloud container
python -m goldflow.cli orderflow --name o1 --resume   # after --until / --budget / Ctrl-C
python -m goldflow.cli orderflow-validate              # 7 sample days, about 20 s
scripts/check.sh
```

## 5. Tests and quality gates
- **pytest: 127 passed, 0 failed** (105 before this phase + 22 new).
- **ruff, ruff format, mypy --strict:** pass (44 source files, including all of `goldflow/orderflow`).
- **Phase 7 tests cover:**
  - settings validation (the config hash is unchanged);
  - hand-computed diagonal and horizontal imbalances, the zero policy, exact ratio boundaries and stacking;
  - imbalances equal an independent pandas recomputation;
  - CVD resets (day, session, anchor) with unknown volume kept separate;
  - day-end CVD equals the partition totals;
  - taker-order rules;
  - **brute-force references:**
    - taker-order builder: 40 random streams with random chunking and state restores (also run during development);
    - rolling quantile: 30 streams;
    - Big Trade quantile thresholds: 5 streams;
    - band arithmetic;
  - no self-influence or look-ahead in thresholds;
  - absorption candidate → confirmed with a later timestamp;
  - a single large trade is never absorption; expiry at the deadline;
  - exhaustion with delta divergence;
  - divergence confirmed only after the right-hand bars close, then invalidated;
  - **end-to-end invariants** (all six checks 0);
  - **identical digests** across runs, chunk sizes, two interruption points, and duplicate delivery;
  - resume refused after a settings change;
  - the CLI outputs;
  - the validator.

## 6. Real-data evidence (full six-month dataset, rebuilt in this session)
The dataset was rebuilt here from the frozen range: 932 archive files, SHA-256 verified. Phase 3 reproduced the documented figures: 912 partitions, **149,515,816 trades**, 0 quarantined, trade days 181 COMPLETE / 1 UNVERIFIED / 1 PARTIAL.

| Measure | Value |
|---|---|
| Events replayed | 155,997,644 in 381.7 s (**408,707 events/s**); peak RSS 1,246 MB |
| **Replay sequence digest** | `eeacba10…2d50`, **identical to the Phase 4 reference** |
| **Phase 5 processor state digest** | `ad595214596f8399…`, **identical to the Phase 5 report**: the new hooks changed nothing |
| Taker orders reconstructed | 42,668,536 |
| Events | 314,773. Logical digest `0c971546c4977c1c…` |
| Imbalance rows / CVD rows | 1,505,581 (`0e27ee46…`) / 736,636 (`ace81978…`) |
| Catalogue on disk | 89 MB |

**Events by type** (initial events; terminal statuses in brackets):

| Type | Initial | Outcomes |
|---|---|---|
| BIG_TRADE | 218,932 (≈1,196/day; buy 107,180, sell 111,752) | – |
| STACKED_IMBALANCE | 76,719 | – |
| EXHAUSTION | 4,171 | confirmed 2,070, ambiguous 1,242, invalidated 817, expired 42 |
| DIVERGENCE_DELTA | 2,080 | invalidated 1,456 |
| ABSORPTION | 2,188 | confirmed 1,480, failed 483, expired 197, ambiguous 28 |
| DIVERGENCE_CVD | 1,677 | invalidated 1,191 (204 pivot pairs skipped: different trading days, so CVD isn't comparable) |

Counts by session are in `data/reports/orderflow_catalogue.md`. London and New York dominate. Weekend flow is about 8 % of Big Trades and 30 % of absorption candidates.

**Labels on events:** DQ status OK 311,921, UNVERIFIED 2,727, PARTIAL 125. Coverage FULL for all. Unknown-side lots 0: every Binance trade has a reported side, and the synthetic tests cover the non-zero case.

**Invariant checks (all 0):**
- event time before evidence end;
- orphan status events;
- initial events without trade ids;
- null unknown lots;
- big-trade id-count mismatch;
- duplicate event ids;
- Big Trades with truncated id lists.

**Determinism on real data** (22–27 Oct 2026: 6,397,702 events, a weekend and the UK DST change). Identical `events`, `imbalances` and `cvd` digests (`f5c182d2…`, `76fb57b3…`, `07c53b9d…`) for:
- an uninterrupted run;
- 4,999-event chunks;
- a run stopped on Saturday at 13:07:11.5 and resumed;
- every chunk delivered twice (6,179,974 duplicates ignored).

**Independent validation** (`data/reports/orderflow_validation.md`). 7 days spread over the six months, all trades and aggTrades COMPLETE, recomputed with separate code: **7/7 PASS.**
- **Day-end CVD** = Σ buy − Σ sell from the raw partition exactly, on every day (e.g. 2026-07-17: −20,641,944 lots).
- **Imbalances:** pandas recomputation and catalogue identical (e.g. 13,170 / 13,170, 0 missing, 0 extra).
- **Big Trades:** 8,995 of 8,995 have consecutive trade ids on one side that sum exactly to the reported size.
- **Same-millisecond splits:** aggTrade fills that share one millisecond are split by the reconstruction **0 times**.

## 7. Findings, limitations and pushback
1. **aggTrades are not taker orders (measured).** Every aggTrade spans at most exactly 100 ms, and some hold separate orders (a few 1–2-lot fills, then a distinct sweep 51 ms later). Binance aggregates same-price fills inside a ~100 ms window across orders. So `cluster_ms = 0` stays the default. Raising it only glues orders together; the table is in `METRICS_ORDERFLOW_V1.md` §4. Taker orders in the same millisecond, on the same side, at adjacent prices remain indistinguishable (no order ids).
2. **"Big Trade" at q = 0.995 is a percentile by construction.** About 1,200 a day, with a median threshold of about 29.6 oz (29,555 lots) and a median Big Trade of 45.7 oz. That is the top 0.5 % of taker orders, not "institutional" size. If you want rarer events, set `abs_lots` / `notional_usdt` or a higher q.
3. **Exhaustion on 1m bars is often AMBIGUOUS** (30 %): the pullback and a new high happen inside the same minute. Checking status on 5s bars would cut this. It is an open parameter choice, not a defect; I didn't change it without your call.
4. **Confirmation rates are not edge.** Absorption is confirmed 68 % of the time and exhaustion 50 %, but the confirm distance (3 × $0.10 = $0.30) is small next to gold's typical intraday ranges. These rates describe the rules, not trading value. Phase 12/13 must test them out of sample, after costs, at the broker.
5. **Session-dependent outputs** (session CVD, session labels) use the **proposed** session times (D4).
6. **Candidates still open at the end of the data** are counted in the manifest, not resolved (0 at the end of this run).

## 8. Security and operations
- No credentials are needed; nothing is sent anywhere.
- Derived data stays under `data/` (git-ignored). Only the small reports are committed.
- The cloud rebuild regenerated the Phase 3 manifest and validation report with cloud-catalog references. The PC's committed versions were restored, so the canonical record is unchanged.
- **On the PC:** `git pull`, then run `python -m goldflow.cli orderflow --name o1`. No reprocessing of Phases 3–5 is needed.

## 9. Acceptance criteria

| Criterion | Result | Evidence |
|---|---|---|
| Side uncertainty visible in every output | **PASS** | `unknown_lots` / `cum_unknown_lots` on every event, imbalance and CVD row (non-null check 0); UNKNOWN taker orders keep side UNKNOWN; synthetic tests with ~1 % unknown |
| No volume silently assigned to bid or ask | **PASS** | Only exchange-reported sides enter buy/sell/delta; CVD unknown equals the partition's UNKNOWN total (test); inferred sides are not used |
| Every event traces to its input trade ids | **PASS** | 0 initial events without a trade-id range; Big Trades carry full id lists (8,995 of 8,995 verified on real days); status events link to their candidate (0 orphans) |
| Events reproducible (same digest across runs) | **PASS** | Identical digests across runs, chunk sizes, interrupt/resume and duplicate delivery, on synthetic and real data |
| Confirmation timestamps ≥ evidence window end | **PASS** | Checked at recording and in the report: 0 violations among 314,773 events; tests assert later timestamps for confirmations |
| Six-month event catalogue with counts by session; validation report | **PASS** | `orderflow/` catalogue (183 days), `orderflow_catalogue.md`, `orderflow_validation.md` (7/7) |

## 10. Recommended next phase
**Phase 6** (Volume Profile, POC, Value Area, HVN/LVN, VWAP). It is the remaining M3 analytics phase and supplies the context levels for Phase 7 events. Not started without your instruction. Decisions for you: D4 (sessions); and whether to adjust the Big Trade threshold and the exhaustion status interval (findings 2 and 3).
