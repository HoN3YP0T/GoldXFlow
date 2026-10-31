# goldflow: Binance XAUUSDT gold orderflow data foundation (Phases 2–4)

Downloads and verifies historical market data for **Binance USDⓈ-M `XAUUSDT`** (TradFi perpetual on spot gold). Raw data is kept exactly as Binance published it; Phase 3 normalises it into validated Parquet partitions and Phase 4 replays it deterministically.

**No market data is committed to this repository.** Rebuild it with `python -m goldflow.cli acquire` then `python -m goldflow.cli normalize`. The frozen range in `data/state/range.json` reproduces the exact dataset, and the partition digests in `data/manifests/dataset_manifest.json` let you verify it.

## Layout
```
config/binance_xauusdt.json   symbol, datasets, range, concurrency, retry, REST limits
goldflow/
  rest_client.py    Binance futures REST: exchangeInfo/spec, aggTrades, trades, historicalTrades;
                    weight-based rate limiting (X-MBX-USED-WEIGHT-1M), 429/418 Retry-After, 451 = GeoBlocked
  vision_archive.py data.binance.vision: S3 listing (paginated), Range-resume download, SHA-256 .CHECKSUM check
  checkpoint.py     SQLite (WAL, synchronous=FULL): units, gaps, REST cursors, events, runs
  acquire.py        planner (monthly where whole month, else daily; missing -> explicit record + REST unit),
                    thread pool downloader, progress.json, republish detection, monthly->daily fallback
  integrity.py      streaming pyarrow verification (bounded memory): ids, gaps, dups, time order, values
  report.py         coverage of [start,end), cross-file id continuity, trades-vs-1m-klines cross-check, manifest
  cli.py            discover | acquire | rest | manifest
docs/L2_ORDERBOOK_AVAILABILITY.md
tests/test_core.py
data/raw/binance/futures_um/XAUUSDT/<dataset>/<daily|monthly|rest>/   raw zips + .CHECKSUM (+ _verify sidecars)
data/state/   acquisition.sqlite, range.json, plan-*.json, progress.json, acquisition.log, instrument_discovery.json
data/manifest.json
```

## Usage
```bash
pip install pyarrow requests numpy
python -m goldflow.cli discover                 # instrument spec (REST) + archive availability
python -m goldflow.cli acquire --months 6       # first run freezes [start,end) in data/state/range.json
python -m goldflow.cli acquire                  # resume: verified files are skipped (size + ETag + status)
python -m goldflow.cli rest                     # REST-only recovery units (run where fapi.binance.com is allowed)
python -m goldflow.cli manifest                 # rebuild manifest.json from the checkpoint DB
```
Exit code 1 means a unit failed or the primary `trades` dataset has an uncovered interval. Nothing is skipped silently. Every missing file, id gap and uncovered interval is a row in the `gaps`/`units` tables and appears in the manifest with its reason.

## Rules enforced
- Range is half-open `[start, end)` in UTC ms. `end` is the acquisition timestamp, frozen on the first run.
- A file counts only when its SHA-256 matches Binance's `.CHECKSUM` **and** it passes streaming verification.
- If an upstream file is republished (ETag change), the old copy moves to `superseded/` and the change is logged.
- A monthly file that fails verification is kept, marked `rejected`, and its month is re-acquired from daily files.
- REST access returning HTTP 451 is recorded as a geo-block. The client never switches to an alternative host to get around it.

## Phase 3: validation, normalisation and storage
```bash
python -m goldflow.cli normalize            # validate + quarantine + write Parquet partitions + catalog + reports
python -m goldflow.cli validate             # recompute everything from raw, write nothing, compare result digest
python -m goldflow.cli normalize --dates 2026-10-29   # single days (midnight straddles at the edges stay PARTIAL)
python -m pytest -q tests
```
Outputs:
```
data/normalized/provider=binance/market=usdm_perpetual/instrument=XAUUSDT/data_type=<trades|aggTrades|klines_1m|bookDepth|metrics>/date=YYYY-MM-DD/part-000.parquet
data/quarantine/...same partitioning...      invalid records with reason codes (created only when there are any)
data/manifests/dataset_manifest.json         coverage, partitions (sha256 + content digest), raw files by role, limitations
data/reports/validation_report.{md,json}     statuses, non-complete days/hours, monthly cross-check, run comparison
data/reports/quarantine_report.md, coverage_daily.csv, coverage_monthly.csv
```
Catalog tables (same SQLite DB): `raw_files`, `partitions`, `day_status`, `hour_status`, `quarantine_counts`, `raw_day_stats`, `crosscheck`, `validation_runs`.
See `docs/SCHEMAS_V1.md` for field definitions and reason codes, and `docs/ADR-0001-phase3-storage.md` for the storage decisions.

## Phase 4: deterministic replay
```bash
python -m goldflow.cli replay --name r1                         # full dataset at maximum speed
python -m goldflow.cli replay --name r1 --resume                # continue exactly after the last delivered event
python -m goldflow.cli replay --start 2026-10-29T13:00:00Z --end 2026-10-29T21:00:00Z --speed 60 --name ny
python -m goldflow.cli replay --dates 2026-07-09 --session 13:00-21:00 --streams trades klines_1m --name x
```
Prints the event counts per stream, the final simulation clock, throughput, peak memory, the sequence digest and integrity violations. See `docs/REPLAY_V1.md` for availability rules, tie order, controls and checkpoint semantics. Python API: `goldflow.replay.ReplayEngine` with `Consumer` subclasses.

## Running on a restricted Windows-shared folder (Claude desktop Linux VM)
On mounts where SQLite journals fail and files can't be deleted, run every step through the wrapper:
```bash
GOLDFLOW_SNAPSHOT=0 scripts/pc_run.sh 165 acquire --no-rest --skip-reconcile   # repeat until "run result"
scripts/pc_run.sh 165 rest --skip-reconcile                                     # REST recovery (needs a non-US host)
GOLDFLOW_SNAPSHOT=0 scripts/pc_run.sh 165 normalize --budget 95 --no-report     # repeat until finalized
scripts/pc_run.sh 165 validate --budget 95 ; scripts/pc_run.sh 165 validate --resume --budget 95 ...
scripts/pc_run.sh 165 report --budget 95                                        # resumable monthly cross-check + reports
```
The working DB lives on local disk (`$HOME/gf_state`) and a consistent snapshot is saved to `data/state/acquisition.sqlite`. Files that would be deleted are moved to `data/_to_delete/`. Code updates are applied with `python3 scripts/apply_update.py update.tgz`.
