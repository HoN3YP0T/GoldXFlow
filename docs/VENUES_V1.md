# Additional providers and cross-market mapping, v1 (Phase 15)

Code: `goldflow/providers/{okx,bybit,bitget}/`, `goldflow/providers/archive_venue.py`, `goldflow/venues/` (`mapping.py`,
`archives.py`, `ingest.py`, `run.py`, `crossvenue.py`, `report.py`).
Commands: `venue-import`, `venue-run`, `basis`, `volume-oz`, `providers-report`. Config block: `venues`.

## 1. Rules
- **One venue, one market.** Every venue has its own identity: provider id, venue id, market type and instrument id
  (`okx:usdt_swap:XAU-USDT-SWAP`). Each venue also has its own partitions (`provider=<venue>/market=<type>/…`), its own
  catalog (`data/state/venues/<venue>.sqlite`) and its own processor.
- **No merging.** The Phase 5 processor refuses trades of any other instrument. No book, profile or bar is ever merged
  across venues.
- **Cross-venue series are separate outputs, computed from separate datasets.** They are the basis series and the
  ounce volume aggregate, and they are explicitly labelled. They are never fed back into a venue's analytics.
- **Honest capabilities.** Each adapter declares what it has, what it lacks, and what is geo-restricted from this host.
  Asking for something unsupported raises `CapabilityUnavailable`.

## 2. Venues
| Venue | Instrument | Tick | Quantity unit | oz per lot | History (verified 2026-10-31 from the build host) | Completeness | RPI flag |
|---|---|---|---|---|---|---|---|
| Binance | `XAUUSDT` USDⓈ-M perpetual | 0.01 | XAU (step 0.001) | 0.001 | data.binance.vision (Phases 2–3) | kline counts (Phase 3) | REST only (Phase 14) |
| OKX | `XAU-USDT-SWAP` linear swap | **0.1** | contract = 0.001 XAU (step 1) | 0.001 | `static.okx.com` daily zip, **UTC+8 days** | **contiguous trade ids**: provable | `source` = 1 |
| Bybit | `XAUUSDT` linear perpetual | 0.01 | XAU (step 0.001) | 0.001 | `public.bybit.com/trading` daily CSV (from 2026-03-09) | **not provable**: UUID ids, REST geo-restricted | `RPI` = 1 |
| Bitget | `XAUUSDT` USDT-M perpetual | 0.01 | XAU (step 0.01) | 0.01 | archive host unreachable here; **not implemented** | – | – |

Paid sources are not in v1 (they need entitlements; decision D5): COMEX GC/MGC via Rithmic, CQG or dxFeed, and
order-book history from Tardis.dev or the venues themselves.

## 3. Instrument mapping records (`mapping.py`)
Each record holds:
- **Source instrument:** provider, venue id, market type, symbol, instrument id.
- **Contract terms:** quote and settlement currency, tick size, quantity step, quantity unit, ounces per quantity unit
  and ounces per lot.
- **Session and price basis.**
- **Execution instrument:** the broker symbol, once the `broker` block is filled in (D2).
- **Archive day offset:** the UTC offset of the venue's archive days.
- **Specification source.**

Defaults live in the code; the `venues` block overrides them per venue. `providers-report` compares each record with
the venue's live discovery wherever discovery is reachable. On 2026-10-31 OKX and Bitget **matched**. Bybit's
discovery is geo-restricted, so its record is DECLARED. The archive was checked for it: every price on 2026-10-23 is a
multiple of 0.01 and every size a multiple of 0.001.

`venue_config(cfg, venue)` derives a normal configuration for a venue's data. It carries the same analytics settings
and the venue's own instrument, partitions and catalog, with only the trades stream enabled.

**Tick-denominated parameters are inherited unchanged**: profile bins, imbalance thresholds and strategy stops. On OKX
(tick 0.1) they are 10× wider in price, so review them before comparing results across venues.

## 4. Import (`venue-import`)
1. **Fetch.** The archive files that cover each UTC day are fetched. For OKX, UTC day D needs files D and D+1, because
   file D covers [D−1 16:00Z, D 16:00Z).
2. **Raw files.** Raw files are kept exactly as published, in `data/raw/<venue>/…/trades/daily/`, with a meta sidecar
   (URL, size, SHA-256, ETag, download time). A present file is never fetched again, and a changed one is refused.
3. **Parse.**
   - **OKX:** side is the taker side; size is in contracts. The quote notional is not published, so the derived
     price × size is used.
   - **Bybit:** timestamps are seconds with up to 4 decimals, parsed **as text** into whole milliseconds (floor).
     `source_trade_id` is derived as `day_number × 10⁷ + row`. The UUID is checked for duplicates.
4. **Validate** with the **Phase 3 trade validator**, over each file's own interval.
5. **Write** `TradeEvent` v1 rows per UTC day and record the logical digest.
6. **Catalog.** Status is recorded in the Phase 3 format, so the Phase 4 replay turns it into quality events:

| Status | Meaning |
|---|---|
| COMPLETE (OKX) | accepted ids contiguous from the last trade before the day to the first trade after it |
| PARTIAL (OKX) | an id break (or a rejected row) inside the day; the hours it spans are PARTIAL |
| UNVERIFIED | Bybit always (completeness unprovable); OKX when the next UTC+8 file is not published yet |
| MISSING | no archive file covers the day |

## 5. The unchanged stack on another venue (`venue-run`)
The Phase 4 replay, Phase 5 processor and Phase 6/7/10/9/11 engines run on the venue's configuration without a code
change. Outputs go to `derived/provider=<venue>/…/venue_run=<name>/` and are reconciled: Σ 1 m bar volume and count
equal Σ partition lots and rows. On Bybit, every signal is suppressed (`DQ_OPEN`) because its days are UNVERIFIED.

## 6. Cross-venue series
**Basis** (`basis --pair A:B`).
- On a 1 s grid, each venue's price is its last trade **strictly before** the grid time, if that trade is at most 5 s
  old.
- basis = A − B, exact, in the largest unit dividing both ticks: 0.01 USDT.
- A grid point needs both prices; its availability time is the grid time.
- Outputs: `derived/cross_venue/basis/pair=A-B/date=D/basis.parquet`, plus hourly and overall statistics with
  coverage in `summary.json`.
- These are trade-price series, not bid/ask spreads: Binance publishes no quote history for XAUUSDT.

**Cross-venue volume** (`volume-oz`).
- Per minute and per venue, the executed ounces, with the venue's data-quality status for that minute.
- A total labelled `CROSS_VENUE_AGGREGATE v1`, `venues_present`, and `all_complete` (true only when every venue was
  proven complete for that minute).
- Output: `derived/cross_venue/volume_oz/interval=1m/date=D/volume_oz.parquet`, with the label in the schema metadata.

## 7. Report (`providers-report`)
`data/reports/providers.{json,md}`. Per provider it shows:
- the mapping record and the specification check against live discovery;
- reachability from this host;
- the capability table;
- coverage: days by status, trades, ounces, rejected rows, and the RPI share of volume where the venue flags it.

## 8. Limits
- Only historical trades are imported for OKX and Bybit. Real-time adapters, order books and klines for these venues
  are declared NOT_IMPLEMENTED.
- Bitget history is not available from this host.
- Bybit completeness cannot be proven, so its signals stay gated. Bybit's REST (klines) is reachable from a non-US host
  and would allow kline-count verification as on Binance.
- The venues' RPI-like flags (`source` = 1 on OKX, `RPI` = 1 on Bybit) are counted but not carried in `TradeEvent` v1,
  which has no RPI column (the Binance archive has none).
