# Phase 10 completion report: sessions, holiday calendar, Initial Balance (IVB disabled)

Date: 2026-10-31. Instrument: Binance USDⓈ-M XAUUSDT. Input: the Phase 5 `TradeProcessor` over the Phase 4 replay. Specification: `docs/METRICS_IB_V1.md`.

**Order of phases.** This phase was built before Phase 9, which the guide allows ("the Phase 10 session calendar may be built first or in parallel"). The one Phase 9 rule it needs, acceptance outside a level, is implemented exactly as the Phase 9 specification defines it, so Phase 9 can reuse it.

## 1. Scope completed / not completed
**Completed**
- **Session calendar.** The Phase 5 versioned calendar (zoneinfo, DST, trading day, weekend) is extended with:
  - named IB definitions (time zone, local start, length, local session end, weekdays);
  - a **holiday calendar** whose dates are computed from the US holiday rules (Good Friday from Gregorian Easter) with observance shifting.

  Holidays and weekends label every row (`holiday`, `holiday_status`, `gold_closed`, `weekend`), so perpetual flow during gold closures is flagged.
- **Initial Balance:**
  - high, low, mid and range;
  - extensions at k ∈ {0.5, 1, 1.5, 2, 3}, compared in exact rationals;
  - developing values every minute while forming;
  - `IB_FORMED` or `IB_EMPTY` at the IB end.
- **IB behaviour:**
  - breakouts on 5-minute closes;
  - failed breaks within 30 minutes, held breakouts and returns inside;
  - **acceptance outside** by the Phase 9 rule (a 15-minute stretch of closes beyond, or ≥ 30 % of the window volume traded beyond);
  - range expansion per k;
  - a session summary.

  Volume and delta appear only as documented boolean filters on breakouts (`delta_confirms`, `volume_confirms`), never as gates.
- **`IBEngine`:**
  - a Phase 5 `ProcessorSink`, so replay and live share one code path;
  - a pure `IBTracker` state machine;
  - a two-clock driver, so 1-minute events are not delayed by 5-minute bars;
  - DQ and coverage labels;
  - checkpoint state, refused if the settings changed.
- **Dataset and checks:**
  - Parquet `events`, `developing` and `sessions` tables with a manifest;
  - `ib-verify`: IB extremes straight from raw trades, plus a full rebuild from bars recomputed from raw trades.
- **CLI:** `ib` (resumable and budgeted) and `ib-verify`. The `initial_balance` and `calendar` blocks are validated at `Config.load`.
- **Tests and docs:** 18 tests in `tests/test_ib.py`; `docs/METRICS_IB_V1.md`.

**Not completed / by design**
- **IVB is disabled (decision D1).**
  - There is no IVB code path, and `ivb.enabled = true` is refused at startup (tested).
  - It is never replaced by the IB.
  - Needed from you: the exact meaning, formula, inputs, window and interpretation, ideally with a worked example or chart.
- **CME holiday trading hours are not verified.**
  - CME's own pages could not be read from this environment, and third-party guides disagree. For example, the Juneteenth and July 3 halts are given as both 12:30 and 13:30 CT.
  - Every holiday therefore ships as `UNVERIFIED`. Holidays label rows and never change a calculation.
  - Fill the hours in from CME's notices as `calendar.overrides`.
- **IB windows are PROPOSED (D4):** New York 08:20–09:20 America/New_York, and London 08:00–09:00 Europe/London (the guide's examples).

## 2. Files created, modified, deleted
**Created**
- `goldflow/ib/__init__.py`, `holidays.py`, `settings.py`, `tracker.py`, `engine.py`, `store.py`, `verify.py`
- `tests/test_ib.py`
- `docs/METRICS_IB_V1.md`, `docs/PHASE10_COMPLETION_REPORT.md`
- `data/reports/ib_verification.{md,json}` (full six-month run)

**Modified**
- `goldflow/settings.py`: validates `initial_balance` and `calendar` at load.
- `goldflow/cli.py`: `ib`, `ib-verify`.
- `config/binance_xauusdt.json`: new `calendar` and `initial_balance` blocks. They are not part of the processing hash, so the Phase 5/6/7 datasets stay valid.
- `pyproject.toml`: `goldflow/ib` under strict mypy; version 0.10.0. `scripts/check.sh`: `goldflow/ib` added to the format checks.
- `README.md`, `docs/BUILD_GUIDE.md`.
- `goldflow/processing/trade_processor.py`: two no-op sink hooks.
  - `on_after_advance(proc)`: the end of every clock advance, after bars and windows.
  - `on_dq(start, end, status)`: a data-quality interval became known.

  Processor state is untouched; the six-month processor digest is unchanged (§6).

**Deleted:** none.

## 3. Architecture and data flow
```
ReplayEngine ── EventChunk ──► TradeProcessor ──► finalised 1m bars (+1-tick levels), 5m bars ──► IBEngine.on_bars (pending)
                                     ├─ on_dq ──► DQ intervals
                                     └─ on_after_advance(final_upto) ──► drive():
                                          stream A to final_upto[1m]: IB formation, developing rows, acceptance, range extensions
                                          stream B to final_upto[5m]: breakouts, failed breaks, held, returns
                                          summary once both passed session_end
IBEngine ──► IBStore (Parquet per date, staged at checkpoints) ──► ib-verify
         └─► listeners / current(name)    (live use, Phase 11)
```

## 4. Commands
```bash
python -m goldflow.cli ib --name i1                # full dataset; writes the IB dataset and verifies it
python -m goldflow.cli ib --name i1 --resume       # after --until / --budget / Ctrl-C
python -m goldflow.cli ib-verify
scripts/check.sh
# On your PC (restricted mount):
scripts/pc_run.sh 165 ib --name pc-p10 --budget 130 ; scripts/pc_run.sh 165 ib --name pc-p10 --resume --budget 130   # repeat until "completed"
```

## 5. Tests
**145 passed, 0 failed** (`scripts/check.sh`: ruff, ruff format, strict mypy on 51 files and pytest all pass). That is 127 existing and 18 for Phase 10, counting the 6 invalid-settings cases separately.

The Phase 10 tests cover:
- **Holiday rules:** Easter for 2000/2019/2024/2025/2026; all 2026 rule dates; Sunday→Monday and Saturday→Friday observance.
- **Calendar behaviour:** labels and overrides; a verified status needs a source, and an early halt needs a halt time.
- **IB bounds in both DST transition periods**, where the US and UK change on different dates:
  - New York 13:20 UTC on 6 Mar and 12:20 UTC on 9 Mar;
  - London 08:00 UTC on 9 Mar and 07:00 UTC on 30 Mar;
  - London 07:00 UTC on 23 Oct and 08:00 UTC on 26 Oct;
  - New York 12:20 UTC on 30 Oct and 13:20 UTC on 2 Nov.
- **Settings:** 6 invalid-settings cases; **IVB enabled → refused** (`ConfigError` at `Config.load`).
- **Hand-built bars:**
  - developing IB values and `IB_FORMED` levels and extensions;
  - breakout → failed break → second breakout → held → return inside, with exact times and both filters;
  - acceptance by **time** (15 consecutive closes) and by **volume** (400 of 1,210 lots beyond);
  - range extension to k = 0.5 only;
  - an empty IB;
  - `event_time ≥ evidence_end` everywhere.
- **Engine on replay:**
  - verification against raw trades;
  - DST session boundaries; no IB at weekends; IB extremes = raw trade extremes;
  - an emission-clock check on every row;
  - snapshot/restore at two cut points (state digest and every file digest), and resume refused after a settings change;
  - chunk-size independence;
  - **live-style hand-built chunks identical to replay** (session boundaries are the same in replay and live).

## 6. Real-data evidence
### 6.1 Full six months (`ib --name ib6`)
| Measure | Value |
|---|---|
| Events replayed | 155,997,644; replay digest `eeacba10…2d50` (Phase 4 reference) |
| Processor state digest | `ad595214596f8399…` (Phase 5 reference; unchanged) |
| Wall time / throughput / peak RSS | 246 s / **633,287 events/s** / 1,210 MB |
| Bars consumed | 263,518 × 1m, 52,704 × 5m |
| IB instances | 262 (131 weekdays × New York and London); 0 empty; 0 DST-gap skips |
| Rows | 15,720 developing, 4,022 events, 262 session summaries |
| IB state digest | `9cac1d4cfa3b1bf7…` |
| **Verification** (`data/reports/ib_verification.md`) | **262 / 262 instances rebuilt from raw trades, 0 mismatches.** IB extremes from the trades partitions are exact, and every developing row, event and summary is identical after rebuilding the bars from raw trades |
| Holiday labels | Memorial Day 25 May, Juneteenth 19 Jun, Independence Day (observed) 3 Jul, Labor Day 7 Sep: labelled `UNVERIFIED`; `gold_closed` false |
| DQ labels | 28 Oct: both IBs `UNVERIFIED` (the Phase 3 status of that day). 29 Oct: OK, because the upstream gap starts at 21:49 UTC, after the New York session end (21:00 UTC) |

**Event counts over six months.** These describe the data; they say nothing about profitability.

| | New York | London |
|---|---|---|
| Median IB range | $21.57 | $14.18 |
| Median session range / IB range | 2.29 | 3.91 |
| Breakouts (up + down) | 478 | 592 |
| … failed within 30 min | 297 (62 %) | 353 (60 %) |
| … held | 175 | 225 |
| … with `delta_confirms` / `volume_confirms` | 358 / 125 | 446 / 274 |
| Acceptances above / below | 102 / 137 | 148 / 182 |
| Session close above / inside / below the IB | 40 / 50 / 41 | 54 / 20 / 57 |

### 6.2 Stop/resume on real data (19–29 Oct 2026, 13,637,477 events, includes the UK DST change)
- The run was stopped at 07:41:13.5 UTC on 23 Oct (inside the London IB) and at 13:52 UTC on 26 Oct (inside a New York failed-break window), then resumed.
- It reproduced replay digest `d3affed7…`, processor state `12f3dcd7…` and IB state `e6ef1f5c…`, with **identical event, developing and session files** for all 9 dates.
- The verification report lists the IB starts across the October DST change:
  - London moves from 07:00 UTC (through Fri 23 Oct) to 08:00 UTC (from Mon 26 Oct);
  - New York stays at 12:20 UTC, because the US changes only on 1 Nov.

## 7. Known limitations, missing capabilities, unresolved decisions
- **D1, IVB:** blocked on your definition.
- **D4, IB windows and sessions:** proposed defaults. A change alters only the IB settings hash, so it needs a fresh `ib` run; no other dataset is affected.
- **CME holiday hours:** unverified (see §1). There is no automatic early-close truncation; please supply `calendar.overrides` from CME notices.
- **Parameters** (5-minute breakout closes, the 30-minute failed-break window, acceptance at 15 min / 30 %, the 1.5× volume filter) are v1 research defaults with no claim of edge. Phase 13 judges them out of sample.
- **Venue.** This is the IB of the Binance perpetual, not COMEX and not the broker feed.
- **Time past the data.** A replay explicitly extended past the dataset's last day reports `IB_EMPTY` with `dq_status` OK there, because the Phase 3 catalog has no status for time it never covered. The same is true in Phase 5. The default range stops at the last data day; a "beyond the catalog" DQ label belongs in Phase 14 (live) design.

## 8. Security and operational considerations
- **Credentials and data:** none used. The derived dataset is git-ignored; only the verification report is committed.
- **State and writes:** engine state lives in the replay checkpoint. Writes are atomic, and staging files are moved rather than deleted on the restricted mount.
- **Size:** about 3 MB for six months.

## 9. Acceptance criteria
| Criterion | Result | Evidence |
|---|---|---|
| Session boundaries are identical in replay and live | **PASS** | Hand-built live-style chunks give rows identical to replay (test); one zoneinfo conversion path; 262 real instances rebuilt identically |
| The DST transition weeks are tested (US and UK change on different dates) | **PASS** | March and October/November 2026 boundary tests for both IBs; the real October change in the data (§6.2) |
| IVB is implemented only after formula sign-off | **PASS (not implemented)** | No IVB code path; enabling it is refused at `Config.load`; D1 still open |
| (Spec) IB high/low/mid/range, developing values and extensions | **PASS** | Hand-computed test; 262/262 IB extremes equal the raw trade extremes |
| (Spec) Breakout, acceptance outside, failed break, range expansion; volume/delta as boolean filters only | **PASS** | Hand-built sequence tests with exact times; filters reported, never gating |
| (Spec) Holiday calendar and flagging of perpetual flow during gold closures | **PASS, with hours unverified** | Rule-computed dates (tested); `holiday`/`gold_closed`/`weekend` on every row; CME hours `UNVERIFIED` pending your overrides |

## 10. Recommended next phase
**Phase 6, then Phase 9.** Phase 6 (volume profile and VWAP) is built and kept as a separate commit, to be applied on top of this one. Phase 9 needs the Phase 6 levels, the Phase 7 delta and the Phase 10 IB, and its acceptance rule is already implemented here. Phase 9 is not started.

Before or during Phase 9, please send:
- **D1**, the IVB definition;
- **D4**, confirmation of the IB windows;
- the CME holiday hours you want to use.
