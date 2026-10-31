# Phase 11 completion report: unified strategy interface and signal generation

Date: 2026-10-31. Specification: `docs/SIGNALS_V1.md`.

## 1. Scope completed / not completed
**Completed**
- **Strategy contract.** Required capabilities, required analytics inputs, triggers, a typed config schema, warm-up, `evidence_for` / pure `build`, and a description.
- **Capability and data-quality gating,** engine-enforced and suppress-only:
  - unsupported capability or missing input → refused at construction;
  - `WARMUP`, `DQ_OPEN`, `DQ_EVIDENCE`;
  - `DUPLICATE_ACTIVE`, an addition found on real data: two triggers confirmed by the same bar would otherwise double one setup.
- **Regime features** (no look-ahead) and a **grouped, documented, uncalibrated score**.
- **`TradingSignal` schema** per the guide, with exact R:R, an evidence list with timestamps and source ids, and a structural check.
- **Three example research strategies,** each with a written hypothesis (failed auction at VAH/VAL, absorption at an HVN or VWAP band, IB failed break), plus a depth example that demonstrates capability refusal.
- **Tooling.** `StrategyEngine` as the last sink with time-ordered dispatch; a signals dataset with a suppression log; `signals` and `signals-verify` commands.

**Not completed**
- **Score calibration** is Phase 13 work.
- **Broker translation and fills** are Phase 12 work.
- **Six-month run:** do it on the PC.

## 2. Files
**Created**
- `goldflow/strategies/`: `__init__.py`, `signal.py`, `base.py`, `regime.py`, `library.py`, `engine.py`, `verify.py`;
- `tests/test_strategies.py` (6 tests);
- `docs/SIGNALS_V1.md` and this report.

**Modified**
- `goldflow/cli.py`: `signals` and `signals-verify`.
- `goldflow/settings.py`: the `strategies` block is validated.
- `config/binance_xauusdt.json`: new `strategies` block.
- `pyproject.toml` and `scripts/check.sh`: `goldflow.strategies` is under mypy --strict and ruff format.
- `README.md`, `docs/BUILD_GUIDE.md`.

## 3. Architecture
```
TradeProcessor sinks: [ProfileEngine, OrderflowEngine, IBEngine, AuctionEngine, StrategyEngine]
  analytics engines ── listeners ──► StrategyEngine buffer
  StrategyEngine.on_advance: sort by available_at; apply every row at time t to AnalyticsState;
      then for each trigger at t: strategy.evidence_for → build → check → gate → SignalStore / listeners
```

## 4. Commands
```bash
python -m goldflow.cli signals --name s1 [--resume]     # full pipeline in memory, signals persisted, then verification
python -m goldflow.cli signals-verify
```

## 5. Tests
- `scripts/check.sh`: **all checks passed**, with **185 tests** (179 earlier plus 6 new) and mypy --strict on 68 files.
- The new tests cover:
  - schema rules: wrong-side stop and target, look-ahead evidence, exact R:R, a stable id;
  - parameter refusal (unknown or mistyped), config refusal (unknown strategy), capability refusal (historical L2), and missing analytics engines;
  - `build` of all three strategies on hand-built evidence, including the R:R floor, the delta requirement, and rounding the target toward the entry;
  - every gating reason, in order;
  - a look-ahead assertion;
  - the full pipeline on synthetic replay: signals produced, a clock check (0 violations), evidence never later than the signal, verification re-derives every signal, stop/resume gives the same digest, and changed parameters refuse resume.

## 6. Real-data evidence (cloud, 4 days, 2026-10-23 → 26)
| Check | Result |
|---|---|
| Pipeline | ProfileEngine + OrderflowEngine + IBEngine + AuctionEngine + StrategyEngine: 80,909 analytics rows dispatched in time order; 20.9 s (160 k events/s); 639 MB |
| Replay digest | `8f528ccd…` (unchanged) |
| Strategy state digest | `bc3e06ab…`; identical after stopping on Monday 10:00 or on Sunday 01:13 and resuming |
| `signals-verify` | **5 of 5 signals re-derived exactly from their stored evidence**; 0 problems |
| Signals | `fa_value_edge` 1 (London, short, R:R 17/11); `absorption_level` 2 (long, 2R); `ib_failed_break` 2 (New York, one each way) |
| Suppressed | 1 `DUPLICATE_ACTIVE` (two absorption candidates confirmed by the same 1 s bar) |

Five signals in four days says nothing about merit. Their profitability is for Phase 12–13 to test on broker prices, out of sample.

## 7. Limitations and open decisions
- **D13 (new): example strategy parameters.** Keep them for research and judge them only out of sample.
- Scores are uncalibrated.
- Signals use Binance prices; broker translation is Phase 12.

## 8. Acceptance criteria
| Criterion | Status | Evidence |
|---|---|---|
| Same inputs give the same signals | **Passed** | Identical state digests across uninterrupted and resumed runs (real and synthetic); deterministic `signal_id` |
| Every signal explainable from its evidence list | **Passed** | `signals-verify` re-derives every stored signal from its evidence alone (5/5 real, all synthetic) |
| Only information available at signal time is used | **Passed** | Time-ordered dispatch; `check()` asserts evidence ≤ event time; clock assertion; listener clock check in tests |
| A strategy refuses unsupported inputs | **Passed** | `CapabilityUnavailable` for historical L2; refusal of a missing analytics engine; refusal of unknown or mistyped parameters |

## 9. Next phase and its blocker
Phase 12 (event-driven backtesting) **needs your broker's XAUUSD bid/ask tick history (D2)**. Fills must be on broker prices, never on Binance's. The engine, simulator and basis model can be built and tested with synthetic broker ticks, but no real backtest result can be produced until those ticks are exported from MT5.
