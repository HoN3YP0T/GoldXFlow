# Phase 8 completion report: order-book reconstruction and liquidity (live only)

Date: 2026-10-31. Built in the cloud workspace while Phase 5 was being built in another session. Phase 8 doesn't use any Phase 5 output: it depends only on Phase 1 (settings, provider contract, observability) and the Phase 4 `Consumer` interface.

## 1. Scope completed / not completed
**Completed** (all items in the build guide's "What to build" list):
1. **Binance futures local book** (`liquidity/book.py`). It follows the documented procedure: buffer, snapshot, drop `u < L`, bridge on `U ≤ L ≤ u`, then require `pu ==` previous `u`; otherwise it invalidates and resyncs. Quantity 0 removes a level. Exact integer ticks and lots. A malformed payload, a crossed book or a lost connection also invalidates it.
2. **Recorder** (`liquidity/recorder.py`, `capture.py`, `dataset.py`):
   - persists raw WebSocket messages and REST snapshots (gzip JSONL, before any transformation);
   - reconnects with bounded backoff and rotates connections before Binance's 24 h limit;
   - fetches a snapshot only when needed;
   - stops on a geo-block and never works around it;
   - normalises captures to Parquet DepthEvent v1, with `sequence_validity` per row and validity intervals per book.
3. **Book state and metrics** (`metrics.py`, `tracker.py`): best bid/ask, spread, depth within ±N ticks and ±x %, imbalance per band, concentration (top-k share), additions and reductions, and depletion and replenishment near the touch. Reductions without a matching trade are labelled **"reduction (cancel or hidden fill unknown)"** (`REDUCTION_UNATTRIBUTED`).
4. **Coverage labelling** (`coverage.py`): `depth_capability(window)` returns VALID only for windows covered by one continuous valid book. `require_valid_depth` raises `DepthUnavailable` otherwise.
5. **One book per provider, venue, instrument, market and kind** (`registry.py`). There is no merge operation, routing a foreign symbol raises `MergedBookError`, and RPI is a separate book kind (disabled by default; unverified endpoint).

Also built:
- the `depth-record`, `depth-replay` and `depth-normalize` CLI commands;
- a `depth` settings block, with defaults in code so the config hash is unchanged;
- a `DepthBookConsumer` for the Phase 4 replay engine;
- the real-data reference checker `scripts/verify_depth_reference.py`.

**Not completed / deferred by design**
- **Continuous recording.** It needs a host where REST is reachable (decision D6). From this cloud host the WebSocket works but the REST snapshot returns HTTP 451.
- **Reduction-to-trade matching on live data.** It needs live trades (Phase 14). The function exists and is tested with trade input.
- **Feed-staleness timers and live DQ intervals:** Phase 14.
- **`depth_l2` in `replay.streams`.** Enabling it changes the replay config hash, and there is no recorded data yet.
- **Catalog registration of depth partitions:** Phase 14, together with the live catalog.
- **RPI stream verification.** It needs a live check on the PC before `depth.rpi_enabled` is turned on.

**Merge note (Phase 5 integrated after this report).** Phase 5 (commit `93cbf3b`) was merged into this branch afterwards. Phase 5 adds `processing` and `sessions` blocks to `config/binance_xauusdt.json`, so the config-file hash is now `9d77c52f…` instead of `f0b71abc…`. Phase 8 itself still adds nothing to the config file. After the merge: 105 tests pass; ruff passes; mypy --strict passes on 30 files, including both `goldflow/processing` and `goldflow/liquidity`.

## 2. Files
**Created**
- `goldflow/liquidity/__init__.py`, `units.py`, `book.py`, `registry.py`, `coverage.py`, `metrics.py`, `tracker.py`, `capture.py`, `driver.py`, `recorder.py`, `dataset.py`, `commands.py`
- `scripts/verify_depth_reference.py`
- `tests/test_phase8.py` (20 tests), `tests/depth_sim.py` (synthetic test exchange)
- `docs/METRICS_LIQUIDITY_V1.md`, `docs/PHASE8_COMPLETION_REPORT.md`

**Modified**
- `goldflow/settings.py`: new `DepthCfg` block (`settings.depth`), with defaults in code.
- `goldflow/rest_client.py`:
  - `get(..., raw=True)` returns the response text;
  - new `depth_snapshot_text()`;
  - weights for `/fapi/v1/depth` and `/fapi/v1/rpiDepth`.
- `goldflow/providers/binance/adapter.py`: `REALTIME_L2_DEPTH` changed from NOT_IMPLEMENTED to AVAILABLE, with a note that it works on non-US hosts only.
- `goldflow/cli.py`: three `depth-*` commands and `--duration`.
- `pyproject.toml`: a `live` extra (websockets), websockets in `dev`, and `goldflow/liquidity` under `mypy --strict`.
- `scripts/check.sh`: new modules added to the format gate.
- `README.md`, `docs/BUILD_GUIDE.md` (status), `docs/SCHEMAS_V1.md` (DepthEvent section).

**Unchanged, byte-for-byte:** `normalize.py`, `schemas.py`, `integrity.py`, `catalog.py` (the Phase 3 `code_version`) and `replay.py`.

**Deleted:** none.

## 3. Architecture and data flow
```
WebSocket <sym>@depth@100ms ─┐                     ┌─► CaptureWriter (raw gzip JSONL, before anything else)
REST /fapi/v1/depth (on need)┼─► DepthRecorder ────┤
connection events ───────────┘                     └─► BookDriver ─► LocalOrderBook(s) ─► LiquidityTracker (metrics)
raw capture on disk ─► iter_capture (+ truncation/gap/session markers) ─► BookDriver (same code) ─► same digest
                                                   └─► normalize_capture ─► DepthEvent v1 Parquet + validity intervals
DepthEvent Parquet ─► Phase 4 ReplayEngine ─► DepthBookConsumer ─► LocalOrderBook (same code) ─► same digest
DepthCoverage(intervals) ─► require_valid_depth(window) ─► depth metrics / future depth signals, or DepthUnavailable
```

## 4. Commands
```bash
pip install -e ".[dev]"                                  # or ".[live]" on the recording host
python -m goldflow.cli depth-record [--duration S]       # PC / non-US VPS; exit 4 if geo-blocked
python -m goldflow.cli depth-replay [--dates D ...]
python -m goldflow.cli depth-normalize
python scripts/verify_depth_reference.py --seconds 120   # works wherever the WebSocket is reachable
scripts/check.sh
```

## 5. Tests and quality gates
- **pytest: 76 passed, 0 failed.** 56 from earlier phases, plus 20 for Phase 8.
- **Phase 8 tests cover:**
  - exact unit parsing and refusals;
  - the sync procedure rebuilding the exchange's true book;
  - stale-snapshot rejection and resync;
  - **a sequence gap invalidating the book and the book being rebuilt**;
  - quantity-0 removal and duplicates;
  - crossed-book, invalid-payload and disconnect invalidation;
  - books never merged (foreign symbol, RPI separate, other venue);
  - hand-computed metrics;
  - bands beyond the snapshot depth reported as None;
  - **metrics refused on invalid or syncing books**;
  - change classification and near-touch flow windows;
  - **capability labelled per interval** (VALID, PARTIAL, ABSENT; the historical period is ABSENT);
  - capture round trip with truncation, sequence-gap and session markers;
  - **the live recorder with a lost event, a disconnect and lagged snapshots, giving identical digests and intervals from the live run, the capture replay and the Parquet replay in exchange-time order**;
  - geo-block stop;
  - checkpoint and restore equalling an uninterrupted run;
  - the Phase 4 replay consumer;
  - settings validation (config hash unchanged);
  - the CLI `depth-replay` and `depth-normalize` commands;
  - optimised band metrics equal to a brute-force exact reference on 25 random books.
- **Stress:** 40 seeds of the live recorder (2 gaps, 2 disconnects, WebSocket lag of 0–4 events): live = capture replay = Parquet replay digests and intervals, 0 mismatches.
- **ruff check:** passed. **ruff format:** 26 files clean. **mypy --strict:** no issues in 22 source files, including all of `goldflow/liquidity`.

## 6. Real-data evidence (Binance XAUUSDT, 2026-10-31, cloud host)

| Check | Result |
|---|---|
| WebSocket `fstream.binance.com` reachable from this host | Yes (REST `fapi` returns **HTTP 451**, as documented) |
| Message format matches the parser (`e, E, T, s, U, u, pu, b, a`; exact tick/step multiples) | Yes, 0 parse errors |
| `pu` chain continuity on consecutive events | 653 events, **0 breaks**; `U ≠ prev u + 1` confirmed, so continuity must use `pu` |
| **Reference check:** book seeded from Binance's `depth20` partial book, then maintained from diffs with the production code, compared with every later `depth20` at the same `u` | **220 / 220 comparisons identical, 5,572 price levels**, 0 mismatches, result PASS |
| Negative control (one level silently corrupted) | Detected: 5 mismatches, FAIL |
| `depth-record` from this host | Stops with status `geo_blocked`, exit 4; no book is built without a snapshot |
| Throughput (synthetic, ~1,500-level book) | Capture write 33.7 k records/s; replay 31 k events/s; replay + 1 s metric sampling 6.5 k events/s; normalise 15.7 k events/s. A day is about 0.86 M events. |
| Size (real diffs, Saturday, spot gold closed) | 7.3 events/s; about 44 MB/day gzip (Phase 0 weekday estimate: about 120 MB/day) |

There are **no dataset counts for recorded L2 yet**: recording has to run on a host where REST is reachable. The historical dataset is unaffected. Depth capability for 2026-04-30 → 2026-10-30 is ABSENT.

## 7. Known limitations and open decisions
- **D5 (historical L2: buy or record):** recording is now possible. Starting it early is what creates depth history.
- **D6 (recorder host):** the trading PC works but must stay on 24/7. A small non-US VPS is better. Each restart is a new capture session, and the book resyncs.
- **Known depth range.** A 1,000-level snapshot covers only about ±0.1–0.3 % around the mid. Wider percent bands are reported as None, not extrapolated.
- **RPI.** The stream and endpoint names (`@rpiDepth@500ms`, `/fapi/v1/rpiDepth`) are not verified, so RPI is off by default.
- **Normalisation scope.** `depth-normalize` re-reads the whole capture on each run (idempotent overwrite). Incremental normalisation comes with Phase 14.
- **Boundary rows.** `CONNECTION_LOST` and `INVALID_PAYLOAD` rows are stamped at the last exchange time + 1 ms, so exchange-time ordering keeps them in place. Tested; a large exchange clock jump could still reorder them.
- **Regression digests.** The Phase 3 digest (`7eb02d13…`) and replay digest (`eeacba10…`) can't be recomputed here (no dataset in this workspace). Every module that defines them is byte-for-byte unchanged and the config hash is unchanged (`f0b71abc…`), so they cannot have changed. Re-run `validate` and `replay` on the PC to record the evidence.

## 8. Security and operations
- No credentials are needed: depth endpoints are public.
- Geo-blocks are respected and never worked around.
- Raw captures live under `data/` (git-ignored). They are back-up material, like the archive (D9).
- No order routing is involved; live trading stays locked.

## 9. Acceptance criteria

| Criterion | Result | Evidence |
|---|---|---|
| A simulated sequence gap invalidates and rebuilds the book | **PASS** | `test_sequence_gap_invalidates_and_book_is_rebuilt`; live recorder test and 40-seed stress (gap → `SEQUENCE_GAP` interval end → resnapshot → VALID, book equals exchange truth) |
| No depth-based signal is produced from an invalid or absent book | **PASS** (for the metric and gate layer; strategies arrive in Phase 11) | `book_metrics` raises `DepthUnavailable` unless VALID; `require_valid_depth` refuses PARTIAL and ABSENT windows; the tracker emits nothing while invalid |
| Capability labelled per interval | **PASS** | `DepthCoverage` VALID/PARTIAL/ABSENT tests; validity-interval Parquet; `depth_coverage` report |
| Replay of the recorded stream reproduces the identical book-state digest | **PASS** | Live = capture replay = Parquet (Phase 4 order) replay digests and intervals, in tests and over 40 stress seeds |
| Book correct on real Binance data | **PASS (top-20 reference)** | 220/220 identical comparisons against Binance `depth20`; full REST-snapshot sync awaits the PC run |

## 10. Recommended next steps
1. On the PC, `git pull`, `pip install -e ".[live]"`, then start `python -m goldflow.cli depth-record` and keep it running. Run `depth-normalize` from time to time.
2. On the PC, re-run the Phase 3 `validate` and Phase 4 `replay` digests for the record.
3. Then continue in order once Phase 5 lands: Phase 6, and so on. Not started without instruction.
