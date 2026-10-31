# Phase 12 completion report: event-driven backtesting and execution simulation

Date: 2026-10-31. Specification: `docs/BACKTEST_V1.md`.

## 1. Scope completed / not completed
**Completed**
- **Broker dataset.** Import of MT5 tick exports in both formats: server time zone to UTC with DST, forward-filled quotes, quarantine with reasons, per-day storage, a manifest with digests and a dataset version, and the `bid_ask_quotes` capability.
- **Basis model.** Rolling lower median of 1 s Binance − broker-mid samples, using only the past (asserted), with exact translation of signal levels.
- **Backtester.** A `ProcessorSink` on the same replay clock and code path as research and live (Phases 5–11), with warm-up enforced by the strategy engine.
- **Order simulator.**
  - fills on broker bid/ask with latency and fixed plus volatility-scaled slippage;
  - stops filled at the gapped quote; targets as limits;
  - first-touch rule, plus the unfavourable gap rule (stop first);
  - seeded broker rejections and the stops-level rule;
  - expiry and maximum hold;
  - commission and swap (server-time rollovers, triple day).
- **Risk simulation.** Sizing from equity and stop distance, lot limits, maximum exposure and concurrent positions, daily loss limit, maximum consecutive losses, maximum drawdown stop, session filter.
- **Outputs.** Orders, fills, trades, cash ledger, equity, `run.json` (the reproducibility record) and `report.md`. The report prints the execution assumptions first; its statistics are reconciled to the cent before it is written.
- **Safety.** A backtest is refused without a configured broker (D2). Synthetic broker data is refused unless explicitly allowed, and is then labelled NOT A RESULT throughout the report.

**Not completed (blocked on D2)**
- **A real backtest:** it needs your broker's XAUUSD specification and its bid/ask tick history for 30 Apr – 30 Oct 2026.
- **Limit and stop entries:** not simulated. The example strategies use market entries.

## 2. Files
**Created**
- `goldflow/backtesting/`: `__init__.py`, `settings.py`, `broker.py`, `basis.py`, `simulator.py`, `engine.py`;
- `tests/test_backtest.py` (9 tests);
- `docs/BACKTEST_V1.md` and this report.

**Modified**
- `goldflow/cli.py`: `broker-import` and `backtest`.
- `goldflow/settings.py`: the `broker` and `backtest` blocks are validated.
- `config/binance_xauusdt.json`: `broker` (null until D2) and `backtest` blocks.
- `goldflow/strategies/engine.py`: the strategy engine must follow its analytics engines (rather than be the last sink), so the backtester can follow it.
- `pyproject.toml`, `scripts/check.sh`: the package is under mypy --strict and ruff format.
- `README.md`, `docs/BUILD_GUIDE.md`, `docs/SIGNALS_V1.md`, `docs/PHASE11_COMPLETION_REPORT.md`.

## 3. Tests
- `scripts/check.sh`: **all checks passed**, with **194 tests** (185 earlier plus 9 new) and mypy --strict on 74 files.
- The new tests cover:
  - **Broker spec:** required when backtesting, with a server time zone, exact decimals and a valid zone.
  - **Import of the terminal format:** Athens server time across the 25 Oct DST change; forward-filled ask; crossed, off-point and backwards rows quarantined. CSV import is also tested.
  - **Basis:** lower median and window; refusal when too few samples; half-up translation; the look-ahead assertion.
  - **Rollovers:** the one closing Wednesday is triple; no weekend charge.
  - **Hand-computed fills and costs:**
    - long entry at ask + slippage after latency, target exit, commission and P&L to the cent (net $97.60);
    - a gapped stop;
    - the gap-assumed stop;
    - broker rejection, the stops-level rule, expiry and a stale quote;
    - triple swap charged ($3.00);
    - daily-loss lock, maximum concurrent positions, size below minimum.
  - **Refusals:** no broker configured, and synthetic data without permission.
  - **Full pipeline on SYNTHETIC broker ticks:**
    - every fill at the tick's ask/bid ± slippage, at or after signal + latency;
    - the basis recovers the synthetic $1.50 offset exactly;
    - the ledger reconciles;
    - the report is labelled NOT A RESULT;
    - a rerun gives identical trades, and stop/resume gives identical trades.

## 4. Evidence
| Check | Result |
|---|---|
| Real config | `backtest` exits 2: "broker.name is not set (decision D2) … Binance prices are never used for fills" |
| Scale and mechanics (4 real Binance days + **synthetic** broker quotes, 541,221 quotes; mechanics only, no P&L reported) | 3.34 M events in 24.4 s, including all analytics and strategies; 237,114 basis samples; 5 signals → 5 fills; ledger reconciled; 0 late signals |

## 5. Open decisions
- **D2: broker, XAUUSD specification and tick export.** Blocks real backtests and Phase 13.
- **D7: risk limits.** The defaults are PROPOSED.
- **Rollover convention and costs:** confirm with the broker.

## 6. Acceptance criteria
| Criterion | Status | Evidence |
|---|---|---|
| No look-ahead (clock assertions; basis only up to T) | **Passed** | Broker ticks read only below the processor clock; signals asserted ≤ clock; the basis asserts no late samples; fills only at or after signal + latency (tested on every fill) |
| Same inputs give the same results | **Passed** | Identical trades across reruns and across stop/resume |
| Execution assumptions printed in every report | **Passed** | `report.md` opens with them; `run.json` holds the full model |
| Ledger reconciles exactly to the performance statistics | **Passed** | `stats()` asserts balance, trade and per-order equalities to the cent before any report |
| A real backtest on broker prices | **Blocked (D2)** | Needs the broker's specification and tick history |

## 7. Next phase
Phase 13 (reports, walk-forward, research) builds on these outputs. It can be built and tested now; its results need real backtests, which need D2.
