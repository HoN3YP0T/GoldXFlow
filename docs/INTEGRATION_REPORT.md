# Integration verification: whole codebase, Phases 0–17

Date: 2026-11-01. Scope: verify the entire build from a clean checkout, find and fix bugs, and connect the phases so the
system can be used end to end.

## 1. Method
1. **Clean checkout.** The repository was exported to an empty folder (`git archive HEAD`), as a user would get it.
2. **Fresh data.** The 4 archive days (2026-10-23 → 10-27) were downloaded from data.binance.vision: 20 files,
   checksums verified.
3. **Every command, in phase order** (45 runs): `manifest`, `normalize`, `validate`, `report`, `replay`, `process`,
   `reconcile`, `vprofile`(+verify, export), `orderflow`(+verify), `ib`(+verify), `auction`(+verify, review),
   `signals`(+verify), `backtest`, `broker-import`, `perf-report`, `ledger-verify`, `parity`, `venue-import`
   (OKX, Bybit), `venue-run`, `basis`, `volume-oz`, `providers-report`, `depth-replay`, `depth-normalize`,
   `ops-status`, `ops-check`, `ea-vector`, `bridge-run`, `exec-gate`, `backup`, `pipeline`, `live-run`,
   `status`, `providers`.
4. **Results compared** with the evidence recorded in the phase reports.
5. **Code review** of the code that tests cover least (Phases 14–17 and the CLI), with a regression test for every
   bug found.

## 2. Results
- **Every command ran.**
  - 34 exited 0.
  - 11 exited with a **deliberate, clean refusal**: no broker data (D2), REST geo-blocked here (D6), no bridge
    secret, no depth capture yet, the frozen acquisition range.
  - **No tracebacks.**
- **Reproducibility.** From a clean checkout and freshly downloaded data, the six state digests are **identical** to
  the recorded evidence: processor `4e235890…`, profile `b318578b…`, orderflow `e75e52d2…`, IB `e6d2c1e6…`,
  auction `e66e56a8…`, strategies `bc3e06ab…`.
- **Verifiers.** Every independent verifier reports 0 failures: Phase 5 reconciliation and the Phase 6, 7, 9, 10 and
  11 verifiers.

## 3. Wiring: how the phases now connect
- **One stack builder** (`goldflow/pipeline.py: build_stack`). The Phase 5–12 stack used to be assembled by hand in 8
  places: CLI `signals`, `backtest`, `live-run` and `bridge-run`, the parity harness, the research runner, and the
  venue run. All of them now use one builder, so history, live, research and other venues cannot drift apart. The
  order is: processor → profile → orderflow → IB → auction(profile, IB) → strategies → (backtester), with a signal
  fan-out for the execution bridge.
- **`pipeline` command.** Phases 5–11 run in **one** replay with every dataset written, followed by every verifier;
  the result goes to `data/reports/pipeline_<name>.json`. With `--backtest` and a broker block (D2), Phase 12 runs in
  the same pass.
  - Real data: **23 s of replay instead of about 190 s** across five commands.
  - **All 72 dataset files are identical** to those the five single-phase commands write.
  - The six digests are unchanged.
- **Run scripts.** `run_windows.bat` (trading PC) and `scripts/run_all.sh` (Linux/macOS) go from nothing to verified
  signals. The README has a "how the phases connect" table: command → reads → writes.
- **Dependencies.** `requirements.txt` was missing `pydantic` (the settings layer needs it) and `websockets` (live
  mode). Both are added. `pyproject.toml` version is now 0.17.0.

## 4. Bugs found and fixed
| # | Bug | Impact | Fix and test |
|---|---|---|---|
| 1 | **Emergency stop re-latched on every POSITIONS/HEARTBEAT** while equity stayed below the loss limit; each latch requested positions again | A **runaway message loop** between engine and EA: the regression test never finished on the old code and was killed at the 300 s timeout | Latch once; flatten retries are paced by `tick()`. `test_emergency_latches_once_not_on_every_heartbeat` |
| 2 | `acquire --start/--end/--months` **silently ignored** whenever `data/state/range.json` existed (it ships with the repo) | You could ask for one range and download another | A different explicit range is refused with a message that names `--new-range`. Test added |
| 3 | A **deliberately stopped** live runner or bridge kept raising STALE_FEED / BRIDGE_DOWN (critical) forever | False critical alerts | The live checkpoint records `status`; an audit log ending with STOP means stopped. Alerts skip stopped runs, and a runner that died still alerts. Test added |
| 4 | `perf-report` on a missing backtest crashed with a traceback | Unclear failure | Clean message, exit 2. Test added |
| 5 | `live-run` / `bridge-run` restarted on a checkpoint taken with other settings crashed with a traceback | Unclear failure | Clean message naming the checkpoint, exit 2 |
| 6 | `bridge-run` opened the TCP port before checking the secret | A port left bound on refusal | Secret checked first; the transport is closed on a refused start |
| 7 | `requirements.txt` missing `pydantic` and `websockets` | `pip install -r requirements.txt` gave a CLI that failed at start (pydantic) and no live mode | Added |
| 8 | `run_windows.bat` covered Phase 2 only, and passed `--months 6`, which conflicts with #2 | Not usable end to end | Full flow through `pipeline`; uses the frozen range |

Also new: a test of the execution bridge over a **real loopback TCP socket** against the simulated EA (until now the
tests used only the in-memory pipe).

## 5. Tests
`scripts/check.sh` passes, with **245 tests** (239 earlier plus 6 new) and mypy --strict on 118 files. The new tests:
- `test_pipeline.py`: builder order and refusals; **pipeline datasets identical to the single-phase commands**, file
  by file; clean CLI refusals; the frozen-range guard.
- `test_execution.py`: bridge over real TCP.
- `test_ops.py`: no false alerts for stopped runs; the emergency latches once.

## 6. What still cannot be exercised here
- **Phases 12–13 on real data, and the bridge:** they need your broker data (D2).
- **Live REST:** HTTP 451 from this host (D6).
- **The MQL5 EA:** needs MetaEditor and an MT5 terminal.

Each of these paths refuses cleanly with a message that says what is missing, and is covered by tests on synthetic
inputs.
