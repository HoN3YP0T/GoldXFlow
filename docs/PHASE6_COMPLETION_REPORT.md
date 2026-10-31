# Phase 6 completion report: Volume Profile, POC, Value Area, HVN/LVN and VWAP

Date: 2026-10-31. Instrument: Binance USDⓈ-M XAUUSDT. Input: the Phase 5 `TradeProcessor` over the Phase 4 replay of the Phase 3 dataset. Specification: `docs/METRICS_PROFILE_V1.md`.

## 1. Scope completed / not completed
**Completed**
- **Pure profile functions** (`volume_profile.py`):
  - binning;
  - POC, with a tie-break on distance to the window's volume-weighted mean, then the lower price;
  - Value Area by the CBOT two-bin pair rule, plus the single-bin variant, with a written-down tie rule and edge handling;
  - HVN/LVN on an integer Gaussian-smoothed profile, with topographic prominence, volume filters, minimum separation and half-prominence widths.

  Every decision is made in exact integer or rational arithmetic. Configured fractions are decimal strings, and floats are refused.
- **Exact VWAP** (`vwap.py`): `Σq`, `Σqp` and `Σqp²` are Python integers built from 1-tick levels. It reports the VWAP, the volume-weighted *population* sigma and the `±kσ` bands (k = 1, 2, 3).
- **`ProfileEngine`**, a Phase 5 `ProcessorSink` (the same code path for replay and live):
  - exact per-window state for UTC day, trading day, trading week, sessions, custom ranges and anchors;
  - developing snapshots every minute containing exactly the trades before `as_of`;
  - a final snapshot at each window end, checked against the processor's own window levels;
  - DQ, coverage and `covered_from` labels;
  - runtime anchors, which are refused if back-dated;
  - live accessors and listeners;
  - snapshot/restore inside the replay checkpoint, refused if the settings changed.
- **Dataset** (`profile_store.py`): Parquet snapshots, nodes and final-window levels per date, with a manifest. It is staged at checkpoints, so a crash loses nothing.
- **Independent checks** (`profile_rebuild.py`, `profile_verify.py`):
  - a from-scratch rebuild from the trades partitions (exact parity);
  - a float64 VWAP from the decimal price/quantity columns;
  - Phase 5 totals.
- **Chart-ready JSON** (`profile_export.py`): the profile, POC, value area, nodes, VWAP bands and the developing series, for final windows or any `as_of`.
- **CLI:** `vprofile` (resumable, budgeted, optional `--footprint`), `vprofile-verify` and `vprofile-export`. The `volume_profile` config block is validated at `Config.load` (exit 2).
- **Tests and docs:** 31 tests in `tests/test_profile.py`; `docs/METRICS_PROFILE_V1.md`.

**Not completed / by design**
- **Parameters are proposed.** The node and value-area parameters are v1 defaults (`PROPOSED`). They are documented and hashed, but their trading relevance is a Phase 9/10 research question, to be judged out of sample.
- **Session times** are still decision D4 (inherited from Phase 5).
- **Developing levels** are not stored per minute (only metrics and nodes). The binned profile is stored for final windows; `vprofile-export --as-of` rebuilds any developing profile exactly from trades.
- **Notional profiles** are not produced (they are optional in the guide).
- **Order of phases.** Phase 7 was built in parallel, before this phase, in the same `goldflow/orderflow/` package. Phase 6 was rebased onto it (§2), so its modules are prefixed `profile_*` next to `volume_profile.py` and `vwap.py`. Phase 7 does not use Phase 6 levels yet; it states they can be added as context.

## 2. Files created, modified, deleted
**Created**
- `goldflow/orderflow/volume_profile.py`, `vwap.py`, `profile_settings.py`, `profile_engine.py`, `profile_store.py`, `profile_rebuild.py`, `profile_verify.py`, `profile_export.py`
- `tests/test_profile.py`
- `docs/METRICS_PROFILE_V1.md`, `docs/PHASE6_COMPLETION_REPORT.md`
- `data/reports/profile_verification.{md,json}` (from the full six-month run)

**Modified**
- `goldflow/processing/trade_processor.py`: two new no-op sink hooks.
  - `on_dq(start, end, status)`: a data-quality interval became known, so long windows and anchors keep it after the processor prunes its own list.
  - `on_after_advance(proc)`: the end of each clock advance, after bars and windows.

  Processor state is untouched. The six-day state digest `ca180413…` and the six-month digest `ad595214…` both equal the Phase 5 references (§6).
- `goldflow/orderflow/__init__.py`: exports the Phase 6 API next to Phase 7's (`read_profile_manifest`, `read_profile_table`).
- `goldflow/settings.py`: validates `volume_profile` at load.
- `goldflow/cli.py`: `vprofile`, `vprofile-verify`, `vprofile-export`.
- `config/binance_xauusdt.json`: a new `volume_profile` block. Existing blocks are untouched, so the Phase 3, 4 and 5 hashes are unaffected.
- `pyproject.toml`: version 0.7.0. The package was already under strict mypy and `scripts/check.sh` format checks (added by Phase 7).
- `README.md`, `docs/BUILD_GUIDE.md`.

**Deleted:** none.

## 3. Architecture and data flow
```
ReplayEngine ── EventChunk ──► TradeProcessor ──► finalised 1 s bars (exact 1-tick levels) ──► ProfileEngine.on_bars (pending rows)
                                     │                                                          │
                                     ├─ on_dq ───────────────────────────────────────────────► DQ intervals
                                     ├─ on_window(final window + its levels) ───────────────► final snapshot + parity check
                                     └─ on_after_advance(final_upto) ───────────────────────► for each minute boundary M ≤ final_upto:
                                                                                                 apply rows < M to every open window,
                                                                                                 snapshot (POC, VA, nodes, VWAP, bands)
ProfileEngine ──► ProfileStore (Parquet per date, staged at checkpoints) ──► vprofile-verify / vprofile-export
              └─► listeners / current(kind, name)   (live use, Phase 9–11)
checkpoint: replay cursor + processor state + engine state + staged files, one transaction
```

## 4. Commands
```bash
pip install -r requirements.txt pytest
python -m goldflow.cli vprofile --name v1 [--footprint]     # full dataset; writes the profile dataset and verifies it
python -m goldflow.cli vprofile --name v1 --resume          # after --until / --budget / Ctrl-C
python -m goldflow.cli vprofile-verify --sample-hours 6
python -m goldflow.cli vprofile-export --window session:london:2026-10-26 --bin 10 [--as-of 2026-10-26T09:00:00Z]
scripts/check.sh
# On your PC (restricted mount):
scripts/pc_run.sh 165 vprofile --name pc-p6 --budget 130 ; scripts/pc_run.sh 165 vprofile --name pc-p6 --resume --budget 130   # repeat until "completed"
```

## 5. Tests
**157 passed, 0 failed** with Phase 6 rebased onto Phase 7 (`scripts/check.sh`: ruff, ruff format, strict mypy on 52 files and pytest all pass). That is 105 existing, 21 from Phase 7 and 31 from Phase 6, counting the 8 invalid-settings cases separately.

The Phase 6 tests cover:
- **Hand-computed POC:** a unique maximum; a tie at the exact mean (lower price wins); a tie resolved by the mean; bin-centre tie-breaks at 10-tick bins.
- **Hand-computed Value Area:** the pair rule (`[1,2,5,9,4,4,1,1,3]` → bins 1–5, 24 lots, 2 steps); the single-bin variant; equal pairs (the mean side, or both); the profile edge with fewer than two bins; 100 %.
- **Hand-computed prominence** (peaks 4/1/8 and valleys 4/1 on `[0,5,1,3,2,8,0]`), plus plateaus and edges.
- **Nodes on synthetic profiles:**
  - bimodal HVN/LVN positions;
  - a small bump rejected by prominence;
  - the half-prominence width equal to the analytic Gaussian FWHM;
  - minimum separation.
- **Binning:** the integer kernel; dense and sparse binning agree.
- **VWAP:** exact VWAP and bands, the int64-overflow fallback, the empty case.
- **Settings:** 8 invalid-settings cases, the `footprint_bin_ticks` requirement, and `Config.load` failing with `ConfigError`.
- **Engine on replay:**
  - parity with the processor's windows and with a from-scratch rebuild (metrics and nodes);
  - a trade exactly at 10:00:00.000 excluded from the 10:00 snapshot and included in 10:01;
  - `available_at` and the emission clock (no look-ahead);
  - London resets at 06:00 UTC (BST, 23 Oct) and 07:00 UTC (GMT, 26 Oct);
  - anchor VWAP vs float recomputation across the DST change, and a back-dated anchor refused;
  - dataset, verification and tamper detection;
  - export JSON for final and `as_of`;
  - snapshot/restore at two cut points (state digest and every file digest), and resume refused after a settings change;
  - chunk-size independence;
  - live-style hand-built chunks equal to replay;
  - a seek that discards developing state and labels PARTIAL_START with the right `covered_from`.

## 6. Real-data evidence
**Data.** The full dataset was rebuilt from scratch in this session from the frozen range: 932 archive files, ≈4.0 GB, SHA-256-verified. Phase 3 reproduced exactly the documented figures:
- 912 partitions;
- **149,515,816 trades**, 0 quarantined;
- trades days 181 COMPLETE / 1 UNVERIFIED (28 Oct) / 1 PARTIAL (29 Oct);
- cloud result digest `3860107a…`, the same as the Phase 5 cloud rebuild.

### 6.1 Full six months (`vprofile --name full6 --footprint`)
| Measure | Value |
|---|---|
| Events replayed | 155,997,644 |
| **Replay sequence digest** | `eeacba10…2d50`, identical to the Phase 4 and Phase 5 references |
| **Processor state digest** | `ad595214596f8399…`, identical to the Phase 5 reference, so the new sink hooks changed nothing upstream |
| Wall time / throughput | 621 s / 251,151 events/s (4 vCPU; processor with all 6 intervals, the footprint writer, the profile engine and its writer, and checkpoints all on) |
| Peak RSS | 1,399 MB |
| Rows applied / lots | 49,882,287 1 s footprint rows / 55,245,785,609 lots (= the processor's lots processed) |
| Snapshots | 999,375 developing + 785 final = 1,000,160 rows at bin 10 ticks; 6,803,563 node rows; 556,807 final-profile level rows; 183 dates, all complete; ≈181 MB |
| **Final parity** (engine state vs the processor's window levels) | 785 / 785 equal, 0 mismatches; 0 DQ-label mismatches |
| Profile state digest | `cc8f71fa5724e958…` |

**Verification** (`data/reports/profile_verification.md`, re-run with the final merged code):

| Check | Result |
|---|---|
| Dates | **183 / 183 PASS** |
| Final snapshots rebuilt from scratch from the trades partitions | 785. **0 mismatches** in totals, traded range, `Σq`/`Σqp`/`Σqp²`, POC, VAL, VAH, VA lots, bins and steps, and the full HVN/LVN lists |
| Developing snapshots rebuilt (as_of every 6 h: 00/06/12/18 UTC, every open window) | 2,537. **0 mismatches** |
| VWAP vs float64 from the decimal `price`/`quantity` columns | max relative error **7.4e-15** (tolerance 1e-9) |
| Sigma vs float64 | max relative error 2.1e-13 |
| UTC-day profile = Phase 5 footprint manifest (volume, buy, sell lots) | 183 / 183 days |
| UTC-day profile = trades partition total | 183 / 183 days |

The two non-COMPLETE days are labelled honestly: `dq_status` is UNVERIFIED on 28 Oct and PARTIAL from 21:00 UTC on 29 Oct. They still equal their partitions, because the profile reflects exactly the data that exists; downstream code must suppress on those labels.

Example: the final profiles for Monday 26 Oct 2026 at bin 0.10 USDT.

| Window | Volume (oz) | POC | VAL – VAH | VWAP ± σ | HVN / LVN |
|---|---|---|---|---|---|
| Asia | 163,283 | 4002.00 | 3986.80 – 4016.00 | 4007.03 ± 10.76 | 5 / 1 |
| London | 182,379 | 4025.60 | 4015.60 – 4033.00 | 4020.39 ± 8.86 | 2 / 1 |
| New York | 166,148 | 4025.60 | 4014.60 – 4034.20 | 4021.29 ± 9.83 | 3 / 2 |
| UTC day | 378,797 | 4023.50 | 4009.70 – 4034.10 | 4016.23 ± 12.03 | 4 / 1 |

### 6.2 Six-day sample (22–27 Oct 2026; includes a weekend and the UK DST change)
- **Results:** 32,295 developing and 25 final snapshots in 17 s (370k events/s, without the footprint writer).
- **Parity:** verification at hourly sampling found 0 mismatches over 540 rebuilt snapshots. VWAP max relative error was 2.7e-15.
- **Stop/resume:** the run was stopped at 13:37:11.5 UTC on Saturday 24 Oct and at 09:00 UTC on Monday 26 Oct (after the DST change), then resumed. It reproduced:
  - replay digest `7a161ff9…`;
  - processor state `ca180413…`, the same as the Phase 5 six-day reference;
  - profile state `9d16f0df…`;
  - **identical snapshot, node and level files** for all six dates.
- **After the Phase 7 rebase:** the same run with the merged code gives the same three digests. That is why the six-month dataset above, built just before the rebase, stands for the committed code.

## 7. Known limitations, missing capabilities, unresolved decisions
- **Proposed parameters (new, for you to review):**
  - `bin_ticks = [10]`;
  - value-area pct `0.70`, `pair`;
  - node σ = 5 bins, `prom_frac` 0.10, HVN ≥ 1.0 × mean bin volume, LVN ≤ 0.5 × mean, separation 10 bins.

  Changing any of them changes the settings hash, so it needs a fresh `vprofile` run (resume is refused by design).
- **Value-area tie rule.** The guide's wording, "side closer to the POC distance-weighted mean", is implemented as "the side on which the window's volume-weighted mean lies relative to the POC centre; if exactly at the centre, both". It is documented and tested. If you intended a different reading, tell me before Phase 9 uses the value area.
- **D4 sessions** are still open (from Phase 5).
- **Weekend flow** is inside the day and week profiles. The perpetual trades while gold is closed; strategies must decide using the Phase 5 `weekend` labels.
- **Live late trades** (Phase 14) would show up as `final_parity_mismatch`. This is 0 in replay by construction.
- **Venue.** These are profiles of Binance's XAUUSDT perpetual, not COMEX and not the broker's OTC XAUUSD feed (see §11 of the build guide).

## 8. Security and operational considerations
- **Credentials and data:** none used or stored. Market data and the derived profile dataset are git-ignored; only the verification report is committed.
- **State and writes:** engine state lives in the existing replay checkpoint (SQLite state DB). Parquet writes are atomic (`.tmp` + `os.replace`). Staging files are removed with `safe_remove`, which moves rather than deletes on your restricted mount.
- **Disk:** ≈181 MB for six months of profiles at one bin size, roughly proportional to the number of bin sizes.

## 9. Acceptance criteria
| Criterion | Result | Evidence |
|---|---|---|
| Synthetic profiles with hand-computed POC, VA and nodes match exactly | **PASS** | §5: hand-computed POC/VA/prominence tests; node positions, widths and separation on synthetic profiles |
| VWAP on real days matches an independent recomputation from raw Parquet within 1e-9 relative | **PASS** | 3,322 snapshots over 183 days; max 7.4e-15 (float64 from the decimal price/quantity columns) |
| Session resets and anchors are correct across DST | **PASS** | London 06:00/07:00 UTC around the 25 Oct change (test and real data); anchor VWAP across the DST change vs float recomputation |
| Parity: replay-built state equals state rebuilt from scratch at the same timestamp | **PASS** | 785 final + 2,537 developing real snapshots, 0 mismatches; the engine vs the processor's own window levels 785/785 |
| Profiles reconcile to Phase 5 totals | **PASS** | 183/183 days equal the footprint manifest and the trades partitions; lots applied = lots processed |
| (Pitfall) VA stopping rule and ties written down and tested | **PASS** | `docs/METRICS_PROFILE_V1.md` §4; four VA tests |
| (Pitfall) `bin_ticks` stored with every result | **PASS** | A column in every table, plus the manifest |
| (Pitfall) Developing VWAP does not use trades after T | **PASS** | Snapshot boundary test (a trade at exactly T is excluded); `available_at` and emission-clock checks |

## 10. Recommended next phase
**Phase 9: Auction Market Theory and failed auctions.** Phase 7 is already built. Phase 9 needs the value area, POC and HVN/LVN from this phase, plus the Phase 7 events.

It is not started. Before it begins, please:
- review the proposed Phase 6 parameters and the value-area tie reading (§7);
- confirm **D4** (sessions and the IB window), which Phase 10 needs.

Optionally, Phase 7 can now take Phase 6 levels as context, e.g. absorption at the VAH/VAL or an HVN.
