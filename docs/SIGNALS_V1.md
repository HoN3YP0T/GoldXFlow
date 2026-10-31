# Strategy interface and signals v1

Package: `goldflow/strategies/`. CLI: `signals`, `signals-verify`. Config: the `strategies` block.
Signals are **proposals in signal-venue prices** (Binance XAUUSDT ticks), not orders. Phase 12 translates them to the broker through the basis known at signal time and decides fills.

| Module | Role |
|---|---|
| `signal.py` | `TradingSignal` and `Evidence` schema, deterministic `signal_id`, structural `check()` |
| `base.py` | `Strategy` contract and `AnalyticsState` (what a strategy may read) |
| `regime.py` | Regime features and the setup score |
| `library.py` | Example research strategies (none assumed profitable) |
| `engine.py` | `StrategyEngine(ProcessorSink)`: wiring, time-ordered dispatch, gating, `SignalStore` |
| `verify.py` | `signals-verify`: re-derivation from evidence, rules, summaries |

## 1. Strategy contract
| Member | Meaning |
|---|---|
| `id`, `version`, `hypothesis` | identity and the written hypothesis to test |
| `required_capabilities` | provider capabilities (Phase 1 contract). The engine **refuses** to construct a strategy the provider cannot serve. For example, `historical_l2_depth` is unavailable on this history, so a depth strategy raises `CapabilityUnavailable` |
| `required_inputs` | analytics engines read (`orderflow`, `auction`, `ib`, `profile`); missing ones are refused |
| `triggers` | tables whose new rows it reacts to (for example `auction.events`) |
| `defaults` | config schema: every parameter, its default and its meaning. Unknown or mistyped parameters are refused; decimal values are strings |
| `warmup_ms` | from `strategies.warmup` (default 24 h). No signal before `observe_from + warmup` |
| `evidence_for(table, row, state)` | gathers evidence available **now** |
| `build(evidence, meta)` | **pure**: evidence → signal or None. Used live and by the verifier |
| state | example strategies are stateless; the engine's state (analytics, regime, gating) is checkpointed |

## 2. TradingSignal
Fields:
- identity: `signal_id`, strategy id and version, provider, instrument;
- timing: `event_time`, `available_at`;
- trade plan: `direction`, `entry_type` and `entry_price_ticks`, `stop_loss_ticks`, `take_profit_ticks`, `rr` (float) and `rr_exact` (rational);
- sizing and scoring: `risk_intent` (`1R`; the risk layer sizes it), `setup_score` with `score_calibrated = false` and `score_detail`;
- validity: `expiry_at`, `invalidation_ticks`;
- requirements and quality: `required_capabilities`, `data_quality_status`;
- `evidence[]`: each item has `metric`, `value`, `threshold`, `timestamp`, `source_ids` and `passed`;
- `trigger_id`.

`check()` enforces:
- every evidence timestamp ≤ `event_time` (**no look-ahead**);
- stop and target on the correct sides of the entry;
- `expiry_at > event_time`;
- a known direction and entry type.

A failure is a bug and stops the run.

## 3. Engine: dispatch and gating
- **Dispatch.** The `StrategyEngine` is the **last** sink. It listens to the analytics engines, buffers every row emitted during a processor advance, and dispatches them in `available_at` order. All rows with the same `available_at` are applied to the `AnalyticsState` before any trigger at that time is evaluated. Nothing later is visible.
- **Gating** (suppress only; each suppressed signal is stored with its reason):

| Reason | Rule |
|---|---|
| (construction) | missing capability → `CapabilityUnavailable`; missing analytics engine → `ValueError` |
| `WARMUP` | `event_time < observe_from + warmup` |
| `DQ_OPEN` | a non-OK data-quality interval (Phase 3/4 quality events; live feed health in Phase 14) is open at `event_time` |
| `DQ_EVIDENCE` | the evidence's data-quality status is not OK |
| `DUPLICATE_ACTIVE` | the same strategy already has an unexpired signal in the same direction (one setup, one signal) |

## 4. Regime features and score
- **Regime** (context, never a trigger):
  - realised volatility: σ of the last 12 five-minute log returns, in bps;
  - volume ratio: the last 5-minute bar against the same time-of-day bucket over the previous 20 days;
  - the latest Phase 9 auction state of the trading day (trend vs rotation);
  - open sessions and the weekend flag.
- **Score:** within a group of correlated evidence (for example bar delta and the probe's responsive delta), only the strongest item counts. Groups are combined with documented weights into [0, 1]. **It is not a probability**; it becomes one only after calibration on validation data (Phase 13).

## 5. Example research strategies (none assumed profitable)
| Id | Trigger | Hypothesis | Entry / stop / target |
|---|---|---|---|
| `fa_value_edge` | Phase 9 `FAILED_AUCTION` at VAH/VAL | a failed attempt to leave value rotates back toward the value area's POC | close of the confirming 1m bar; probe extreme + 10 ticks; that window's POC (rounded toward the entry); needs the bar's delta or the probe's responsive delta to point back; R:R ≥ 1 |
| `absorption_level` | Phase 7 `ABSORPTION` `CONFIRMED` within 20 ticks of a trading-day HVN or a VWAP ±1σ/2σ band | passive liquidity defending an accepted price; the response continues | confirmation price; invalidation level + 5 ticks; 2R |
| `ib_failed_break` | Phase 10 `FAILED_BREAK_UP/DOWN` | a failed IB breakout traps breakout traders; price rotates back toward the IB midpoint | failed-break close; extreme reached during the breakout + 10 ticks; IB mid (or the opposite extreme); R:R ≥ 1 |

`depth_example` exists only to demonstrate the capability refusal.

## 6. Dataset and verification
```
data/derived/.../signals/date=YYYY-MM-DD/{signals,suppressed}.parquet + _manifest.json (strategies described, hashes)
data/reports/signals_verification.{json,md}
```
`signals-verify` re-runs each strategy's `build` on the **stored evidence**. The resulting row must equal the stored one: direction, prices, exact R:R, score, expiry, data quality, signal id. It also re-checks `check()`, warm-up, evidence data quality, that the trigger is part of the evidence, and that no two signals of a strategy and direction overlap.

**Determinism:** the state digest is identical for an uninterrupted run and a resumed one. A resume under changed strategies, parameters or upstream settings is refused.

## 7. Limitations
- Signals are not orders. Entry, stop and target are in Binance ticks; broker translation, fills and costs are Phase 12.
- Scores are uncalibrated.
- Example strategy parameters are PROPOSED (decision D13). Their merit is for Phase 13 to judge out of sample, never by in-sample results.
