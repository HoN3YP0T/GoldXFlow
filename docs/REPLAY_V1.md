# Replay engine v1: availability, ordering, controls, checkpoints

Module: `goldflow/replay.py`. CLI: `python -m goldflow.cli replay`. Input: the Phase 3 normalised partitions plus the catalog.

## Availability rules (no look-ahead)
An event is delivered only when the simulation clock reaches its **availability time**:

`availability_ms = time column + time_offset_ms + latency_ms`

These are configured in `config.replay.streams`. Changing any of them changes the config hash, and a checkpoint taken under the old settings will refuse to resume.

| Stream | Time column | Offset | Rank | Rule |
|---|---|---|---|---|
| quality (DQ_BEGIN/DQ_END) | interval start/end | 0 | 0 | Offline annotations from the Phase 3 catalog. Consumers may use them to **suppress** work, never to trigger it, because a live system would detect some gaps only afterwards. |
| klines_1m | `close_time_utc` | +1 ms | 10 | A candle exists only once its minute has closed. The 00:00 candle is available at 00:01:00.000. |
| metrics | `create_time_utc` | 0 | 20 | Binance's publication delay is unknown; set `latency_ms` to be conservative. |
| bookDepth | `snapshot_time_utc` | 0 | 30 | Band snapshot at the time it was taken. |
| aggTrades | `exchange_event_time_utc` | 0 | 40 | Disabled by default (it duplicates trades). |
| trades | `exchange_event_time_utc` | 0 | 50 | Exchange trade time. |

`latency_ms` (default 0) models feed delay. Execution latency belongs to the Phase 12 order simulator, not here.

## Deterministic order
The event key is `(availability_ms, rank, seq)`, delivered in strictly increasing order.
- `rank` breaks ties between streams at the same millisecond: quality first, then candles, metrics, depth, trades. The rationale is that information about the past (a closed candle, a snapshot) is visible before the trades of that instant.
- `seq = days_since_epoch(partition date) × 10¹⁰ + row` within the stream's Phase 3 partition. Those partitions are already deterministically sorted (trades by trade id; depth by time and band).
- Partition contents are fixed by their Phase 3 logical digests, so the same dataset, configuration and starting state always yield the same sequence.

## Processing model
One availability-day window `[D, D+1)` is processed at a time. Every enabled stream's events whose availability falls in the window are read, including from partition D−1 when an offset or latency can push events across midnight (candles always do). The streams are merged with a vectorised lexsort and delivered as `EventChunk`s. Memory is bounded by one day, with a measured peak of 1.26 GB on the busiest days.

`EventChunk` carries:
- `keys`: the structured (avail, rank, seq, stream) array;
- `rows`: row index into each stream's day table;
- `tables`: the full canonical columns;
- `clock_ms`;
- helpers `stream_table(name)` and `iter_events()`.

## Controls (Python API, thread-safe)
`run(resume, until)`, `start_background()`, `pause()`, `resume()`, `stop()`, `set_speed(x)` (0 = maximum; 1 = real time), `step_events(n)`, `step_time(ms)`, `seek(t)`.
- `seek` jumps forward without delivering the skipped events. Consumers receive `on_seek(t)` and must rebuild their own state.
- Selection: `start`, `end`, `dates`, `sessions` (UTC time-of-day windows; quality events are never filtered out), and `streams`.
- CLI: `--start/--end/--dates/--session HH:MM-HH:MM/--speed/--until/--budget/--name/--resume/--streams`. Ctrl-C, SIGTERM or `--budget` stops at a checkpoint.

## Checkpoints and resume
Table `replay_checkpoints` stores:
- dataset version (hash of every partition's logical digest for the enabled streams, plus the quality annotations);
- config hash (streams, range, dates, sessions);
- the last delivered key and the simulation clock;
- the event count;
- every consumer's `get_state()`.

They are written in one transaction every `checkpoint_every_events`, at every day boundary, and on stop. Resume refuses (`ReplayGuardError`) if the dataset version or config hash differs. It then restores consumer state and delivers only events with key > last key, so nothing is duplicated and nothing is skipped. Because consumer state and cursor are saved together, a crash between delivery and checkpoint re-delivers to a consumer whose state was also rolled back.

## Verification consumers
- `ChainHasher`: digest of the delivered key sequence, folded in fixed 4,096-event blocks. It is independent of chunking, pacing, stepping and checkpoint boundaries, and its partial block is stored in the checkpoint.
- `IntegrityChecker`: verifies keys strictly increase across chunks and resumes, and that no event's availability exceeds the clock.
