# Phase 14 completion report: real-time Binance ingestion and historical/live parity

Date: 2026-10-31. Specification: `docs/LIVE_V1.md`.

## 1. Scope completed / not completed
**Completed** (`goldflow/live/`, paper mode: signals only)
- **Live trade definition, decision D3 option a.** Individual trades, including RPI trades, come from REST polling of
  `/fapi/v1/trades`, which is the same definition as the history. RPI trades are counted so their share of volume is
  measured live. The `aggTrade` stream is used for monitoring only. Any other `trade_source` is refused.
- **Normalisation by the Phase 3 validator.** Live rows become `TradeEvent` v1 rows (`record_origin = LIVE_REST_TRADES`).
  Rejected rows are counted by reason.
- **Raw capture first.** Every REST response and every WebSocket message is written before it is parsed, in the
  Phase 8 capture format (new record kind `rest_trades`).
- **Gap handling.**
  - A gap is suspected when a poll returns a full page that does not overlap the last seen id; ids are not contiguous
    on XAUUSDT, so a skipped id alone proves nothing.
  - Suspected gaps are backfilled with `historicalTrades` when an API key is set.
  - Otherwise a `GAP` data-quality interval is recorded.
- **Live bus.**
  - It delivers the same `EventChunk` interface as the Phase 4 replay to the unchanged Phase 5–12 consumers.
  - Its clock is the last successful poll time minus `grace_ms`, so a failed poll holds the clock.
- **Feed health.** Per-stream staleness timers open a `STALE` interval with an open end, which suppresses signals
  (`DQ_OPEN`). The interval is closed when the stream answers again.
- **Late trades.** A late trade opens a `LATE` interval. It is excluded from every bar and window and counted, so the
  aggregates stay consistent with each other.
- **Checkpoints and restart recovery.** On restart the checkpoint is restored and a `RECOVERY` interval blocks signals.
  The runner backfills from the last durable trade id, then resumes.
- **WebSocket monitor.** `aggTrade` and `markPrice`, with bounded reconnect backoff and rotation at 23 h. Silence does
  not drop the connection; it is reported by the staleness timers.
- **Parity harness.** `parity --start --end` uses archived trades packed as REST polls; `parity --from-capture` uses a
  live capture. Every output of the full stack is compared exactly against the Phase 4 replay.
- **Commands:** `live-run --name L [--duration S]` and `parity --start ISO --end ISO [--from-capture]`. The new `live`
  config block is validated.

**Fixes found in this phase**
- **WebSocket routing.** The legacy `wss://fstream.binance.com/stream` now accepts connections but delivers **no
  messages**. Depth streams are on `/public/stream`; `aggTrade` and `markPrice` are on `/market/stream`. Both were
  verified here on 2026-10-31. The Phase 8 depth recorder's default URL was changed accordingly; it would otherwise have
  looked connected and recorded nothing.
- **`merge_dq`** (Phase 5, used by the Phase 6/7/10/11 engines). An interval with the same start and status now
  *replaces* the earlier end, which is how an open-ended live interval is closed. Historical quality events never
  repeat a (start, status) pair.
- **Late trades in the processor.** A trade whose base bar is already final used to be dropped from the finished bars
  only, while it was still added to coarser bars that were still open. Phase 7 then failed its own consistency check
  (coarse delta = sum of base deltas). Such a trade is now excluded everywhere and counted. A replay never delivers one.
- **Bus clock.** The first version advanced on wall time while polls were failing, which finalised bars before their
  trades arrived (found by the poll-error test). The clock now follows the last successful poll.

**Not completed**
- **Live REST polling against Binance.** This host gets HTTP 451, so the poller, backfill, RPI share and
  receive-latency measurements must run on the trading PC or a non-US VPS (D6). Everything around them is tested with a
  fake endpoint and a fake clock.
- **Parity from a live capture.** It needs a `live-run` on a REST-reachable host; the harness for it is built and
  tested.
- **`tradingSession` stream.** Not subscribed until its name is verified on the live host.

## 2. Files
**Created**
- `goldflow/live/`: `__init__.py`, `trades.py`, `bus.py`, `runner.py`, `parity.py`
- `tests/test_live.py` (9 tests)
- `docs/LIVE_V1.md` and this report

**Modified**
- `goldflow/processing/trade_processor.py`: `merge_dq`, and late trades excluded everywhere.
- `goldflow/orderflow/engine.py`, `flow_engine.py`, `initial_balance.py`, `goldflow/strategies/engine.py`: they use
  `merge_dq`.
- `goldflow/liquidity/capture.py`: new record kind `rest_trades`.
- `goldflow/settings.py`: Phase 8 depth URL `/public/stream`, and validation of the `live` block.
- `goldflow/cli.py`: new commands `live-run` and `parity`.
- `config/binance_xauusdt.json`: new `live` block.
- `pyproject.toml`, `scripts/check.sh`: `goldflow.live` is under mypy --strict and the format check.
- `tests/test_phase8.py`: depth URL.
- `docs/METRICS_LIQUIDITY_V1.md`, `README.md`, `docs/BUILD_GUIDE.md`.

## 3. Tests
`scripts/check.sh`: **all checks passed**. That is ruff, ruff format, mypy --strict on 86 files, and **210 tests**
(201 earlier plus 9 new).

The new tests cover:
- **Normaliser:** the Phase 3 validator rejects an off-tick price and conflicting repeats and keeps exact repeats once;
  the `TradeEvent` schema; RPI counters.
- **Gap tracker:** overlap, non-contiguous ids (normal), a full page beyond the last id (suspected), and a full page
  that overlaps (complete); state round trip.
- **`merge_dq`:** an open interval is closed by the same (start, status).
- **Feed health:** `STALE` opens with an open end and is not reopened while open; it closes on the next message; state
  round trip.
- **Settings:** `trade_source` other than `rest_poll`, out-of-range poll, unknown keys, and grace below poll are all
  refused.
- **Bus into the replay consumers:** trades are delivered, the clock never moves back, and an open `STALE` reaches the
  processor and the strategy engine and is then closed.
- **Runner, fake REST and fake clock:**
  - 10 min live, then a checkpoint;
  - a restart after a 1 h outage gives `RECOVERY` open, then backfill via `historicalTrades` (count exact), then
    `RECOVERY` closed;
  - every poll is captured raw and read back.
- **Runner without an API key:**
  - poll errors are counted and hold the clock;
  - a 20 s outage gives `STALE`, closed on recovery;
  - a burst beyond one page gives a `GAP` interval;
  - a trade published 25 s late gives a `LATE` interval and is excluded from the bars;
  - HTTP 451 raises `GeoBlocked`.
- **Parity on a synthetic dataset (2 days, with a PARTIAL hour):** all outputs are equal.
  - Negative control: polls delayed past the grace period make the harness report differences.
  - Archived trades with unknown sides or recovered from aggTrades are refused.

## 4. Evidence on real data
**Parity, 4 real days (2026-10-23 → 2026-10-27, 3,197,220 trades).** The live path received 278,688 simulated REST
polls (750 ms interval, trades published 200 ms before each poll, 3 s grace). It was compared with the Phase 4 replay.
**Every output is exactly equal**, with 0 rejected and 0 late:

| Output | Rows (both paths) |
|---|---|
| Phase 5 bars 1s / 5s / 1m / 5m / 15m / 1h | 237,114 / 67,173 / 5,760 / 1,152 / 384 / 96 |
| Phase 5 windows | 15 |
| Phase 6 profile snapshots / nodes | 20,480 / 83,703 |
| Phase 7 bars / CVD / imbalances / stacked / big trades / events | 6,912 / 17,664 / 19,612 / 888 / 4,708 / 334 |
| Phase 10 IB rows / events / levels | 240 / 44 / 4 |
| Phase 9 levels / events / outcomes / states / activity | 60 / 1,188 / 2,654 / 380 / 5,760 |
| Phase 11 signals / suppressed | 5 / 1 |

Report: `data/reports/parity_report.{json,md}`. Run time: 20.5 min. A single day (1,513,252 trades, 90,679 polls)
was also equal on every output.

**Earlier digests unchanged** after the `merge_dq`, late-trade and URL changes, on the full replay of the 4 days:

| Digest | Value |
|---|---|
| Phase 5 processor | `4e235890…` |
| Phase 6 profile | `b318578b…` |
| Phase 7 orderflow | `e75e52d2…` |
| Phase 10 IB | `e6d2c1e6…` |
| Phase 9 auction | `e66e56a8…` |
| Phase 11 strategies | `bc3e06ab…` |

**Live WebSocket on this host (2026-10-31, a Saturday; XAUUSDT trades at weekends).** 120 s on `/market/stream` gave
123 aggTrades and 40 markPrice updates. Receive time minus event time was a median of 70 ms (p95 205 ms) for aggTrades
and a median of 112 ms for markPrice. Feed health showed both streams fresh. The legacy `/stream` URL gave 0 messages
in the same conditions, for both XAUUSDT and BTCUSDT.

## 5. Open decisions
- **D6: non-US host.** Required for the REST poller, which means live trades and any `live-run`.
- **D3:** option (a) is implemented as recommended. Confirm it.
- **`grace_ms`:** the 3 s default is a proposal. Set it above the p99.9 of receive-minus-trade time measured on the
  live host.
- **API key (MARKET_DATA):** needed for gap backfill. Without it, gaps are recorded and gated, not filled.

## 6. Acceptance criteria
| Criterion | Status | Evidence |
|---|---|---|
| Stale feeds are visible and block signals | **Passed** | `STALE` opens with an open end, the strategy engine sees it (`DQ_OPEN`), and it closes on recovery (tests); `health` in the run output |
| Book rebuilt after sequence loss | **Passed (Phase 8)** | Phase 8 driver tests; the depth URL is now fixed so the recorder receives data |
| The same events give equivalent calculations (parity report) | **Passed** | Exactly equal on 3.2 M real trades and 26 outputs; negative control detects differences |
| Live trade definition matches history (documented RPI policy) | **Passed (design) / pending measurement** | REST individual trades incl. RPI; the RPI share is measured once run on a REST-reachable host |
| Live shares code with replay (M6) | **Passed** | Same `EventChunk` consumers; the normaliser is the Phase 3 validator |

## 7. Next phase
Phase 15: additional providers and cross-market mapping. Provider adapters will implement the Phase 1 contract with
honest capabilities. Markets are never merged.
