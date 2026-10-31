# Phase 4 completion report: historical event replay engine

Date: 2026-10-30. Instrument: Binance USDⓈ-M XAUUSDT. Dataset: Phase 3 normalised partitions (result digest `7eb02d13…f704`).

## 1. Scope completed / not completed
**Completed**
- **Simulation clock:** UTC, independent of the computer clock.
- **Merged replay:** five streams plus data-quality annotations, merged by availability time.
- **Availability rules:** documented per stream; no look-ahead.
- **Deterministic tie order.**
- **Bounded memory:** day-window streaming.
- **Controls:** start, pause, resume, stop, speed, step-event, step-time-window, seek-to-time, plus date/session/stream selection.
- **Checkpoints:** transactional, with dataset and config guards.
- **Interfaces:** replay CLI and Python API, with progress metrics.
- **Verification:** determinism, resume and integrity tests, and full six-month benchmarks on two machines.

**Also done, at your request:** the full dataset now lives on your PC at `Documents\python\goldflow`, independently re-acquired, re-normalised and verified identical to the cloud copy.

**Not completed / by design**
- `seek` does not rebuild consumer state; consumers receive `on_seek`.
- Interactive pause/step is in the Python API only. The CLI offers stop, `--until`, `--budget` and resume.
- The REST-recovered 30 Oct tail is stored raw but is not yet part of the normalised dataset or the replay (see §7).

## 2. Files created, modified, deleted
**Created**
- `goldflow/replay.py`, `goldflow/fsutil.py`
- `scripts/pc_run.sh`, `scripts/snapshot.py`, `scripts/apply_update.py`
- `tests/test_replay.py`
- `docs/REPLAY_V1.md`, `docs/PHASE4_COMPLETION_REPORT.md`

**Modified**
- `goldflow/cli.py`: `replay` and `report` commands, plus `--budget`, `--resume` and `--reset`.
- `goldflow/catalog.py`: Phase 3 split into resumable `process_days` (one commit per day, time budget) and `finalize`; validate uses a scratch catalog; resumable monthly cross-check.
- `goldflow/config.py`: `GOLDFLOW_STATE_DB` override.
- `goldflow/acquire.py`: REST resume derived from file content; non-retryable REST refusals recorded as missing.
- `goldflow/checkpoint.py`, `goldflow/vision_archive.py`, `goldflow/normalize.py`: restricted-mount safe journal mode and remove.
- `config/binance_xauusdt.json`: `replay` block.
- `README.md`

**Deleted:** none. On your PC, files that would have been deleted were moved to `data\_to_delete\`.

## 3. Architecture and data flow
For each availability day D:
1. Read the stream partitions D−1 and D, but only for streams whose offset or latency can cross midnight; otherwise only D.
2. Compute availability.
3. Lexsort by `(availability, rank, seq)` and filter by range and session.
4. Deliver chunks to consumers in order. After each chunk: clock = last availability; checkpoint every N events and at day end, holding cursor and consumer state atomically.

Data-quality intervals from the Phase 3 catalog become DQ_BEGIN/DQ_END events at rank 0. The sequence digest is folded in fixed 4,096-event blocks, so it does not depend on chunking or checkpoint boundaries.

## 4. Commands
```
python -m goldflow.cli replay --name r1            # full six months
python -m goldflow.cli replay --name r1 --resume   # continue after the last delivered event
python -m pytest -q tests
# On your PC (Linux VM over the Windows folder):
scripts/pc_run.sh 165 replay --name pc-full --budget 130
```

## 5. Tests
**34 passed, 0 failed** in the cloud and **34 passed** on your PC: 4 Phase 2, 15 Phase 3 and 15 new Phase 4 tests.

The Phase 4 tests cover:
- same sequence across runs;
- digest independent of chunk size;
- the 00:00 candle is available only at 00:01;
- tie order at a shared millisecond;
- quality events first;
- the window end excludes later-available events;
- interrupt and resume at 4 positions equals an uninterrupted run (same digest, events concatenate exactly);
- resume refused after a config or dataset change;
- latency shifts availability;
- step-events, step-time and pause;
- seek;
- speed pacing against the simulation clock;
- session and date filters.

## 6. Replay counts, determinism, performance
Full replay from 2026-04-30 00:00 to the end of 2026-10-29 delivered **155,997,644 events**:

| Stream | Events |
|---|---|
| trades | 149,515,816 |
| bookDepth | 6,165,348 |
| klines_1m | 262,079 |
| metrics | 52,704 |
| quality | 1,697 |

The final clock is 2026-10-29T23:59:59.381Z. There is one fewer candle event than candle rows because the 29 Oct 23:59 candle becomes available at 30 Oct 00:00, outside the window. That is the no-look-ahead rule working.

| Run | Machine | Wall time | Throughput | Peak RSS | Sequence digest |
|---|---|---|---|---|---|
| Uninterrupted | cloud (2 CPU, 7 GB) | 55.8 s | 2.80 M ev/s | 1,255 MB | `eeacba10…2d50` |
| Stopped twice (`--until`, `--budget`), then resumed | cloud | 3 runs | — | 1,243 MB | `eeacba10…2d50` |
| 977-event chunks, different checkpoint spacing | cloud | 62.4 s | 2.50 M ev/s | 1,255 MB | `eeacba10…2d50` |
| Uninterrupted | your PC (2 CPU, 3.9 GB VM) | ≈85 s | 1.84 M ev/s | 1,193 MB | `eeacba10…2d50` |

Integrity violations were 0 in every run.

**Dataset on your PC:**
- **Download:** 927 raw files re-downloaded and SHA-256-verified locally. Your PC independently reproduced the June monthly defect.
- **Comparison:** 912 of 912 partitions match the cloud's content digests exactly, and the Phase 3 result digest matches (`7eb02d13…`).
- **Spec:** the instrument spec was verified live from your PC (TRADING, `TRADIFI_PERPETUAL`, tick 0.01, step 0.001, listed 2025-12-11).
- **REST recovery from your PC:** the 30 Oct 00:00–08:42:51 tail was recovered as 223,132 aggTrades, contiguous with 29 Oct. The 28 Oct aggTrades are permanently unavailable from public sources (Binance error −4166: "Search window is restricted to recent 2 days only"). Individual trades for the tail need an API key.

## 7. Known limitations
- Quality events are **offline annotations**: they may suppress work but must never trigger a signal.
- Metrics availability uses `create_time`, but Binance's publication delay is unknown. Set `latency_ms` before using metrics in a strategy.
- The memory peak is about 1.26 GB per busiest day; it fits your PC's 3.9 GB VM.
- The REST tail and the 28 Oct gap are not in the replay. Adding the tail would need a REST-aggTrades normalisation path, as aggregated (not individual) trades.
- On your PC, the working database lives in the Claude desktop VM (`$HOME/gf_state`) and a snapshot is saved in your folder after each step. If the VM is reset, the wrapper restores from the snapshot.

## 8. Security and operations
- No API keys were used or stored.
- About 7.1 GB now lives at `Documents\python\goldflow` with no backup. Copy it to a second drive.
- `data\_to_delete\` (≈293 MB) holds superseded snapshots and setup leftovers; you can delete it manually.

## 9. Acceptance criteria
| Criterion | Result | Evidence |
|---|---|---|
| Same dataset/configuration/initial state → same event sequence | **PASS** | 4 full replays on 2 machines with the same digest; unit tests |
| Restart/resume does not duplicate or omit events | **PASS** | Stopped twice and resumed gives the same digest and count; unit tests at 4 stop points check exact concatenation |
| Entire six-month dataset replays within documented resource limits | **PASS** | 156.0 M events in 56–85 s with ≤ 1.26 GB peak RSS |

## 10. Recommended next phase
**Phase 5: trade processing, classification and price-level aggregation.** This is the shared processor that turns replayed trades into price-level and time-window aggregates (buy/sell/unknown volume, notional, counts), with sessions and snapshot/restore. Not started.
