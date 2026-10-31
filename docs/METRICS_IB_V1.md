# Sessions, Initial Balance and IVB v1

Package:
- `goldflow/orderflow/initial_balance.py` (engine, settings, store);
- `ib_verify.py`;
- `ivb.py` (disabled);
- `goldflow/processing/holidays.py`.

CLI: `ib`, `ib-verify`; IB levels are also fed into `auction`. Config: the `initial_balance`, `holidays` and `ivb` blocks of `config/binance_xauusdt.json`.

## 1. Session calendar
The Phase 5 calendar (`sessions` block, `docs/PROCESSING_V1.md` §7) is the single source of session boundaries:
- named time zones, with DST handled by zoneinfo;
- a local time that does not exist is refused;
- trading day = 18:00 New York → 18:00;
- the regular trading week, with the weekend flag.

Replay and live use the same `TradeProcessor`, so boundaries are identical in both (tested with hand-built live-style chunks). The session times are still **PROPOSED** (decision D4).

**Holiday calendar** (`holidays`, new, **PROPOSED**):
- dates are local to America/New_York;
- `CLOSED`: no regular gold trading, so no IB is built (`holiday_closed_skipped`);
- `MODIFIED_HOURS`: an early halt or irregular hours; the IB is built and its rows carry `holiday = "MODIFIED_HOURS:<name>"`.

The calendar labels; it does **not** change the Phase 5 session instances, so every Phase 5–9 digest and derived dataset is unchanged. Binance perpetual flow during gold closures stays in the data and is flagged (`weekend` from Phase 5, `holiday` on IB rows).

The 2026 entries are the US federal holidays plus Good Friday:
- **CLOSED:** New Year, Good Friday, Christmas;
- **MODIFIED_HOURS:** the others.

They must be checked against CME Group's published holiday schedule before being relied on.

## 2. Initial Balance (`initial_balance`)
Each definition names a parent session and the IB window inside it: `offset` from the session start, **or** a local `start` time in the session's time zone, plus `length`. At startup every definition is checked to lie inside its session on dates around both 2026 DST changes and in summer.

| Default | Window | UTC example |
|---|---|---|
| `new_york` | session `new_york` + 0 min, 60 min: **08:20–09:20 America/New_York** | 12:20 (EDT) / 13:20 (EST) |
| `london` | local **08:00–09:00 Europe/London** inside session `london` | 07:00 (BST) / 08:00 (GMT) |

US and UK clocks change on different dates (2026: US 8 Mar and 1 Nov, UK 29 Mar and 25 Oct). The gap weeks are tested: for example, on 16 Mar the New York IB starts at 12:20 UTC and the London IB at 08:00 UTC.

**Outputs** (`ib` table):
- **Developing rows** every `snapshot_every` (1 min) while the IB forms. Each is built only from bars that ended before `as_of`; the engine keeps per-minute aggregates, so a later bar can never leak into an earlier snapshot.
- **The final row** at the IB end, with:
  - high, low, mid (exact half ticks), range, volume, buy, sell and delta;
  - the extension levels `IB high + k × range` and `IB low − k × range` for k ∈ {0.5, 1, 1.5, 2, 3} (exact rationals, as JSON);
  - `holiday`, `weekend`, `dq_status` and `coverage`.
- An IB that began before processing started is labelled `PARTIAL_START` and is never offered as a reference level.

## 3. IB behaviour (`ib_events`), from the IB end until the parent session ends
| Event | Rule |
|---|---|
| `BREAKOUT_UP` / `_DOWN` | a `break_interval` (5 min) bar closes beyond the IB extreme |
| `FAILED_BREAK_UP` / `_DOWN` | after a breakout, a close back inside the IB within `failed_break_window` (30 min). A later breakout can follow |
| `EXTENSION_UP` / `_DOWN` | the first trade at or beyond each extension level, stamped at the end of its 1 s bar |
| `SESSION_SUMMARY` | at the session end: largest extension reached each way (in IB ranges), number of breakouts and failed breaks, extensions hit |

**Documented boolean filters** on breakouts. They are reported only and never suppress an event:
- `volume_filter`: the bar's volume ≥ the IB's average volume per bar of that interval;
- `delta_filter`: the bar's delta agrees with the direction.

**Acceptance outside the IB** is Phase 9's acceptance on the IB levels. Every fully observed final IB is offered to the `AuctionEngine` as `IB_HIGH`, `IB_LOW` and `IB_MID` reference levels (series `ib:<name>`), available at the IB end. Phase 9's acceptance, rejection and failed-auction rules then apply unchanged. `auction.levels.sources` includes `ib` by default.

## 4. IVB (`ivb`): disabled
"IVB" has no single standard definition: Initial Value Balance, Initial Volume Balance, or a proprietary indicator. The module ships **disabled**:
- `ivb.enabled: true` is refused at startup until a complete definition is supplied: meaning, formula, inputs, window, interpretation, worked example, and who signed it off;
- it is also refused until an implementation for that definition exists and has been tested;
- asking for IVB raises `IVBUnavailable`. It is **never** replaced by IB or by any other metric.

Decision D1 remains open.

## 5. Dataset and verification
```
data/derived/.../initial_balance/date=YYYY-MM-DD/{ib,ib_events}.parquet  + _manifest.json
data/reports/ib_verification.{json,md}
```
`ib-verify` recomputes from the trades partitions and compares exactly:
- completeness of every expected IB;
- every fully observed snapshot (high, low, volume, sides, extensions);
- breakouts and failed breaks from rebuilt 5-minute bars;
- extension first touches;
- session summaries;
- timing (`available_at`).

Restore and continue, any chunk size, and live-style chunks give identical state and rows (tested). A resume under changed settings is refused.

## 6. Limitations
- The IB definitions and session times are PROPOSED (D4).
- The holiday dates and types are PROPOSED: verify them against CME.
- IVB is not available (D1).
- Extension touches have 1 s resolution.
- IB levels are offered to the auction engine only for fully observed IBs.
