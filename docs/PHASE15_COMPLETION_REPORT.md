# Phase 15 completion report: additional providers and cross-market mapping

Date: 2026-10-31. Specification: `docs/VENUES_V1.md`.

## 1. Scope completed / not completed
**Completed**
- **Venue identity.** Every venue has its own provider id, venue id, market type and instrument id. Partition paths
  are `provider=<venue>/market=<type>`. Each venue has its own catalog and its own processing-settings hash, so another
  venue's state is never restored.
  - The replay reads the venue given by the config.
  - Binance outputs are unchanged: all six evidence digests are identical, see §4.
- **Adapters** implementing the Phase 1 contract, with honest capabilities:
  - **OKX** (`XAU-USDT-SWAP`): REST discovery plus the public daily trade archive. Trade ids are contiguous, so
    completeness can be proven.
  - **Bybit** (`XAUUSDT`): the public daily trade archive with an RPI flag. REST is geo-restricted from this host, and
    the capability table says so.
  - **Bitget** (`XAUUSDT`): REST discovery only. History is declared NOT_IMPLEMENTED and refused, because the archive
    host is unreachable from here.
- **Instrument mapping records**: source instrument, execution instrument, contract terms (ounces per quantity unit and
  per lot), quote currency, tick, step, session and price basis, and the specification source. Each record is checked
  against live discovery.
- **Import** (`venue-import`):
  - raw files are kept exactly as published, checksummed and immutable;
  - OKX's UTC+8 archive days are handled;
  - the Phase 3 trade validator runs on every file;
  - `TradeEvent` v1 partitions are written per UTC day;
  - completeness goes into the venue catalog: COMPLETE or PARTIAL from contiguous ids, UNVERIFIED, MISSING.
- **The unchanged analytics and strategy stack on another venue** (`venue-run`), reconciled against the partitions.
- **Cross-market module:**
  - time-aligned **basis** series per venue pair: 1 s grid, as-of last trade strictly before, 5 s staleness, exact
    common unit;
  - a **labelled cross-venue ounce aggregate**, with each venue's data-quality status per minute.
- **Coverage and capability report** per provider (`providers-report`).
- **Phase 14 follow-up:** the Binance capability table now declares the built live trade poller and aggTrade monitor.

**Not completed**
- **Real-time adapters for OKX, Bybit and Bitget.** They are declared NOT_IMPLEMENTED.
- **Order-book history.** It is paid or not published (D5).
- **COMEX GC/MGC.** It needs a paid entitlement.
- **Bitget history.** Its archive host is unreachable from here.
- **Bybit completeness.** It cannot be proven here; kline counts via Bybit REST need a non-US host.

## 2. Files
**Created**
- `goldflow/venues/`: `__init__.py`, `mapping.py`, `archives.py`, `ingest.py`, `run.py`, `crossvenue.py`, `report.py`.
- `goldflow/providers/archive_venue.py`, `goldflow/providers/{okx,bybit,bitget}/`.
- `tests/test_venues.py` (8 tests).
- `docs/VENUES_V1.md` and this report.

**Modified**
- `goldflow/schemas.py`: `VENUES`, `venue_of`, and `instrument_id(symbol, provider)`.
- `goldflow/normalize.py`: `const_cols` and `part_path` take a provider; the default is Binance.
- `goldflow/replay.py`: the partition root comes from the config's venue.
- `goldflow/processing/settings.py`: venue instrument id; the provider enters the hash for non-Binance venues only.
- `goldflow/providers/registry.py`, `goldflow/providers/binance/adapter.py` (Phase 14 capabilities).
- `goldflow/settings.py`: validation of the `venues` block.
- `goldflow/cli.py`: five new commands.
- `config/binance_xauusdt.json`: `venues` block.
- `pyproject.toml`, `scripts/check.sh`: `goldflow.venues` is under mypy --strict.
- `README.md`, `docs/BUILD_GUIDE.md`.

## 3. Tests
`scripts/check.sh`: **all checks passed**, with **218 tests** (210 earlier plus 8 new) and mypy --strict on 100 files.

The new tests use synthetic archive files, built in the tests and labelled as synthetic. They cover:
- **Identity:** per-venue ids, paths and constant columns; Binance defaults unchanged; Bybit's processing hash differs
  from Binance's although symbol, tick and step are the same.
- **Mappings and venue configs:** ounces per lot; catalog location, including under `GOLDFLOW_STATE_DB`; config
  validation refusals.
- **OKX import:**
  - a UTC day is assembled from two UTC+8 files;
  - COMPLETE from contiguous ids;
  - a 3-id break gives a PARTIAL day with exactly the one PARTIAL hour;
  - UNVERIFIED when the next file is not published, MISSING when there is no file;
  - exact ticks (tick 0.1), lots (contracts), sides, RPI count and ounces;
  - raw files kept with meta, never fetched twice, and refused once changed;
  - an off-tick price rejected (its id becomes a provable hole), and a wrong instrument refused.
- **Bybit import:** text timestamps to ms (`.26` → 260 ms, `.0372` → 37 ms); derived increasing ids; RPI count; an
  off-tick price rejected; UNVERIFIED.
- **Venue run:** the unchanged stack runs and reconciles (8,640 trades, 2,880 1 m bars); a processor for one venue
  refuses another venue's trades.
- **Basis:** hand-computed values, the strictly-before rule, ages and staleness cut-off, unit 0.01.
- **Ounce aggregate:** per-venue units, total, statuses, `all_complete` false with an UNVERIFIED venue, and the label
  in the metadata.
- **Capabilities:** Bitget history and Bybit discovery are refused with `CapabilityUnavailable`.
- **Providers report:** every provider is listed, with coverage and capabilities.

## 4. Evidence on real data (2026-10-23 → 2026-10-27, the same 4 days as the Binance evidence)
**Import**

| Venue | Days | Trades | Ounces | Rejected | RPI share of volume | Raw |
|---|---|---|---|---|---|---|
| OKX | 4 × **COMPLETE** (ids contiguous across 5 archive files) | 509,207 | 192,068.640 | 0 | 5.7 % (3.7–7.0 % per day) | 5 zips, SHA-256 recorded |
| Bybit | 4 × UNVERIFIED (by design) | 471,842 | 84,309.251 | 0 | **17.8 %** (11.5–22.8 % per day) | 4 csv.gz |

**The unchanged stack on each venue.** Every table was produced: bars, profiles, CVD, imbalances, big trades, events,
IB, auction levels, events and states, and signals.

| Venue | Reconciliation (Σ 1 m bars = partitions) | Signals | Suppressed |
|---|---|---|---|
| OKX | 509,207 trades, 192,068,640 lots: exact | 19 | 12 (`DUPLICATE_ACTIVE`, `WARMUP`) |
| Bybit | 471,842 trades, 84,309,251 lots: exact | 0 | 14, all `DQ_OPEN`: the UNVERIFIED days gate them |

OKX signals use the inherited tick-denominated parameters, which are 10× wider in price on OKX. They are a mechanics
check, not research results.

**Basis.** Values are price on the first venue minus price on the second, 1 s grid, 5 s staleness:

| Pair | Coverage | Mean | Median | p5 / p95 | Weekdays (Fri, Mon) mean | Weekend mean |
|---|---|---|---|---|---|---|
| Binance − OKX | 60.3 % | −0.84 | −0.69 | −2.20 / +0.18 | −0.46 (coverage 77 %) | −1.52 (44 %) |
| Binance − Bybit | 56.1 % | −1.00 | −1.01 | −1.71 / −0.28 | −0.94 (78 %) | −1.14 (35 %) |
| OKX − Bybit | 42.1 % | −0.27 | −0.33 | −1.18 / +0.85 | – | – |

Binance XAUUSDT traded on average **$0.5–1.0 below** OKX and Bybit, and further below at the weekend. Coverage is low
at weekends because there are fewer trades. **Independent check:** a pandas `merge_asof` recomputation of the
Binance−OKX and Binance−Bybit series on 2026-10-23 and 2026-10-26 gives **identical series point for point**
(70,155 / 62,182 / 68,099 / 66,032 points).

**Cross-venue ounce aggregate (labelled).** Over the 4 days the venues traded 1,409,313 oz in total: Binance 80.4 %,
OKX 13.6 %, Bybit 6.0 %. `minutes_all_complete` is 0, because Bybit is never proven complete.

**Specification check.** Live discovery of OKX (tick 0.1, lot 1, ctVal 0.001) and Bitget (tick 0.01, step 0.01)
**matches** the mapping records.

**Binance unchanged.** The full 4-day replay with the new code gives the same digests for processor `4e235890…`,
profile `b318578b…`, orderflow `e75e52d2…`, IB `e6d2c1e6…`, auction `e66e56a8…` and strategies `bc3e06ab…`.

## 5. Open decisions
- **Cross-venue research use.** Whether any strategy may use cross-venue evidence, such as basis or venue share. Today
  none does. Adding it is a new hypothesis for the Phase 13 ledger.
- **Venue parameters.** Tick-denominated analytics parameters for OKX (tick 0.1), if OKX results are to be compared
  with Binance's.
- **Bybit completeness.** Run Bybit kline-count verification from a non-US host, or accept UNVERIFIED (signals gated).
- **D5:** paid order-book history; **COMEX GC/MGC** entitlement.

## 6. Acceptance criteria
| Criterion | Status | Evidence |
|---|---|---|
| A provider is added without changing the analytics interfaces | **Passed** | OKX and Bybit run through the unchanged Phase 4–11 stack via a venue config; reconciled exactly; Binance digests unchanged |
| No artificial merged book or profile exists | **Passed** | Per-venue partitions, catalogs and processors; a processor refuses other instruments (tested); cross-venue outputs are separate and labelled |
| Coverage and capability reports exist per provider | **Passed** | `data/reports/providers.{json,md}`: capabilities, reachability, specification check, coverage by status, RPI share |

## 7. Next phase
Phase 16: the MT5 execution bridge and risk controls. Live trading stays off by default, with fault injection.
Building it needs a Windows MT5 terminal and your broker account (D2 and D7); see that phase's notes.
