# Sessions, holidays and Initial Balance v1 (IVB disabled)

Package: `goldflow/ib/`. CLI: `ib` (run) and `ib-verify`. Config: the `initial_balance` and `calendar` blocks of `config/binance_xauusdt.json`. Neither block feeds the Phase 5 processing hash, so changing them never invalidates the footprint, profile or orderflow datasets.

| Module | Role |
|---|---|
| `holidays.py` | `HolidayCalendar`: rule-computed holiday dates, CME status per day (labels only) |
| `settings.py` | Validation of `initial_balance` and `calendar` (at `Config.load`); IVB refusal; settings hash |
| `tracker.py` | `IBTracker`: the pure per-instance state machine (IB formation, breakouts, failed breaks, acceptance, extensions, summary) |
| `engine.py` | `IBEngine(ProcessorSink)`: instances from the definitions, two-clock driver (`drive`), labels, checkpoint state |
| `store.py` | `IBStore`: Parquet events, developing IB and session summaries, plus a manifest; staged at replay checkpoints |
| `verify.py` | `ib-verify`: IB extremes straight from raw trades, plus a full rebuild from bars recomputed from raw trades |

Units are those of Phase 5: prices in ticks (0.01 USDT), volume in lots (0.001 oz), time in UTC ms, windows half-open.

## 1. Session calendar
- **Sessions** are the Phase 5 calendar (`sessions` block): Asia/Tokyo, Europe/London and America/New_York local times, DST through zoneinfo, the trading day rolling at 18:00 New York, and the regular trading week Sun 22:00 → Fri 21:00 UTC (`weekend` outside it).
- **IB definitions** (`initial_balance.definitions`) are separate named windows. Each has `tz`, a local `start`, `minutes`, a local `session_end` (the end of the behaviour window, the same local day) and `weekdays`.
  - A non-existent local time (a DST gap) is refused, never shifted.
  - Boundaries come from the same zoneinfo conversion in replay and live.
- **Defaults: PROPOSED, decision D4.**

  | IB | Local time | Session end | UTC in summer → winter |
  |---|---|---|---|
  | `new_york` | 08:20–09:20 America/New_York (COMEX open) | 17:00 | 12:20 → 13:20 |
  | `london` | 08:00–09:00 Europe/London | 16:00 | 07:00 → 08:00 |

  In the weeks when only one country has changed clocks (2026 weekdays: 9–27 March and 26–30 October), the gap between the two IBs is one hour shorter. This is tested for both changes.
- **Holiday calendar** (`calendar`, versioned):
  - **Dates** are computed from the US holiday rules: New Year, MLK, Presidents', Good Friday (from Gregorian Easter), Memorial, Juneteenth, Independence, Labor, Thanksgiving and Christmas, with Saturday→Friday / Sunday→Monday observance.
  - **CME metals hours** on those days are **not verified**. CME announces them about two weeks before each holiday, and third-party sources disagree. Every holiday therefore ships as `UNVERIFIED`.
  - Add `overrides: [{date, status: CLOSED|EARLY_HALT|OPEN, halt_local, tz, source}]` from CME's notices. A verified status requires a `source`, and `EARLY_HALT` requires `halt_local`.
  - Holidays only **label** rows: `holiday`, `holiday_status`, and `gold_closed` = weekend or a holiday marked `CLOSED`. They never change a calculation. The Binance perpetual trades on all of these days, and strategies decide what that flow means.

## 2. Initial Balance
- **Input:** the Phase 5 processor's finalised 1-minute bars (`formation_interval`). Their high and low are the exact trade extremes.
- **IB high/low** = max/min over bars in `[ib_start, ib_end)`. Mid = (high + low)/2 and range = high − low (ticks). Volume, buy, sell, delta and the open are reported too.
- **Extensions:** `high + k·range` and `low − k·range` for `k ∈ extensions` (default 0.5, 1, 1.5, 2, 3; exact decimal strings).
- **While forming:** one `IB_DEVELOPING` row at the end of every formation bar. It carries the IB so far and is labelled with its `event_time`.
- **At `ib_end`:** `IB_FORMED`, or `IB_EMPTY` if nothing traded.

## 3. IB behaviour (from `ib_end` to `session_end`)
| Event | Rule (all closes are finalised bar closes) |
|---|---|
| `BREAKOUT_UP` / `_DOWN` | a `behaviour_interval` (default 5 min) bar closes **strictly** beyond the IB high (low) |
| `FAILED_BREAK_*` | after a breakout, a behaviour bar closes back on or inside the broken extreme with `bar_end ≤ breakout time + failed_break_window` (default 30 min) |
| `BREAKOUT_HELD_*` | that window ends without such a close; emitted at the deadline |
| `RETURN_INSIDE_*` | a later close back inside after a held breakout. After a failed or returned breakout, the side can break again |
| `ACCEPTED_ABOVE` / `_BELOW` | the Phase 9 acceptance rule (below) |
| `RANGE_EXT_UP` / `_DOWN` (k) | the first formation bar whose high ≥ `high + k·range` (low ≤ `low − k·range`); exact rational comparison |
| `SESSION_SUMMARY` | at `session_end`: IB, session high/low/close, close location (above/inside/below), range ratio, the highest k reached per side, counts per event type, breakouts still open |

- **Acceptance outside** (the Phase 9 definition, implemented here so that both phases share it).
  - **Stretch.** A run of consecutive `acceptance.close_interval` (1 min) closes beyond the extreme. A close back on or inside ends it.
  - **When it fires.** Acceptance is declared once per stretch, at the first bar where either condition holds:
    - **time:** the stretch has lasted ≥ `accept_time` (15 min);
    - **volume:** the lots traded strictly beyond the extreme during the stretch reach ≥ `accept_vol_frac` (0.30) × the window volume since `ib_start`.
  - **Reported:** `criterion` (`time`, `volume` or `time+volume`), `beyond_lots` and `window_lots`.
- **Volume and delta are only documented boolean filters on breakouts.** They never gate an event.
  - `delta_confirms`: the breakout bar's delta has the breakout's sign.
  - `volume_confirms`: the breakout bar's volume ≥ `breakout_vol_mult` (1.5) × the mean IB volume per behaviour bar (IB volume ÷ the number of behaviour bars in the IB).
- **No look-ahead.**
  - Every row carries `evidence_start ≤ evidence_end ≤ event_time` and `available_at = event_time + trade shift`.
  - Rows are produced only after the processor has finalised every bar they depend on (tested by a clock check on every emitted row).
  - The engine runs two clocks, so 1-minute events are not held back by the 5-minute bars. Stream A (formation and acceptance) runs to `final_upto[1m]`, stream B (behaviour) to `final_upto[5m]`, and the summary waits for both.

## 4. Labels
Every row carries:
- `holiday`, `holiday_status` and `gold_closed`;
- `weekend`, from the Phase 5 regular schedule at `ib_start`;
- `dq_status`: the worst Phase 3 status overlapping `[ib_start, event_time)`, meant to suppress use only;
- `coverage`: `FULL`; `PARTIAL_START` if processing began after `ib_start`; `FILTERED` under replay date/session filters.

## 5. IVB: **disabled** (decision D1)
"IVB" has no single standard definition. It could mean Initial Value Balance, Initial Volume Balance or a proprietary indicator. There is deliberately **no IVB code path**:
- `initial_balance.ivb.enabled = true` is refused at `Config.load` (exit 2);
- IVB is never replaced by the IB or by any other metric.

To enable it, supply the exact meaning, formula, inputs, time window and interpretation, ideally with a worked example or a chart. It will then be implemented as its own versioned module after sign-off.

## 6. Dataset
```
data/derived/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/initial_balance/date=YYYY-MM-DD/events.parquet
                                                                                              .../developing.parquet
                                                                                              .../sessions.parquet
data/derived/.../initial_balance/_manifest.json   per-date row counts and digests, settings hashes, definitions, source dataset version
```
- **Key columns** (all tables): `ib`, `label` (local date), `ib_start`, `ib_end`, `session_end`, `event`, `event_time`, `available_at`, `evidence_start`, `evidence_end`, then the IB levels and extensions, then the labels.
- **Partitioning:** a row is stored under the UTC date of `event_time − 1 ms`.

## 7. Verification (`ib-verify`)
For every finished instance with FULL coverage:
1. IB high, low, volume, buy and sell are recomputed **straight from the trades partitions** (no replay, processor or bars) and must equal `IB_FORMED`.
2. Bars (1-minute with 1-tick levels, and 5-minute) are **rebuilt from the trades partitions** and fed to a fresh tracker. Every developing row, event and session summary must be identical to the stored one.

The report (`data/reports/ib_verification.md`) also lists the UTC IB start times across the October DST weeks and the event counts per definition.

## 8. Parameters (v1 defaults: **PROPOSED**)
| Key | Default |
|---|---|
| `definitions` | New York 08:20 + 60 min → 17:00; London 08:00 + 60 min → 16:00 (local) |
| `extensions` | 0.5, 1, 1.5, 2, 3 |
| `formation_interval` / `behaviour_interval` | 1m / 5m |
| `failed_break_window` | 30m |
| `acceptance.close_interval` / `accept_time` / `accept_vol_frac` | 1m / 15m / 0.30 |
| `filters.breakout_vol_mult` | 1.5 |
| `ivb.enabled` | false (cannot be enabled before D1) |

## 9. Limitations
- **Venue.** The IB is computed on the Binance XAUUSDT perpetual, not on COMEX futures and not on the broker's XAUUSD feed. COMEX IB levels will differ.
- **Holiday hours.** CME holiday hours are unverified, so there is no automatic early-close handling. Sessions run to their configured end on every weekday and are only labelled.
- **Time past the data.** If a replay is asked to run beyond the dataset's last day, instances there report `IB_EMPTY` with `dq_status` OK. The Phase 3 catalog has no status for time it never covered (the same as Phase 5). The default replay range stops at the last data day.
- **Live late trades** (Phase 14) are excluded from the processor's finalised bars, and therefore from the IB, as in Phases 5 and 6.
