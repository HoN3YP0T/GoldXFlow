# Orderflow event catalogue (Phase 7)

Descriptive counts of rule-based labels. Not evidence of profitability (Phase 12/13).

Settings hash `086f855c2f22bd23`, source dataset `3da8b283f0241d18`.

## Tables

| Table | Rows | Logical digest |
|---|---|---|
| events | 314,773 | `0c971546c4977c1c…` |
| imbalances | 1,505,581 | `0e27ee46e202a3a6…` |
| cvd | 736,636 | `ace81978f8f4af23…` |

## Events by type and status

| Type | Status | Count |
|---|---|---|
| ABSORPTION | AMBIGUOUS | 28 |
| ABSORPTION | CANDIDATE | 2,188 |
| ABSORPTION | CONFIRMED | 1,480 |
| ABSORPTION | EXPIRED | 197 |
| ABSORPTION | FAILED | 483 |
| BIG_TRADE | DETECTED | 218,932 |
| DIVERGENCE_CVD | DETECTED | 1,677 |
| DIVERGENCE_CVD | INVALIDATED | 1,191 |
| DIVERGENCE_DELTA | DETECTED | 2,080 |
| DIVERGENCE_DELTA | INVALIDATED | 1,456 |
| EXHAUSTION | AMBIGUOUS | 1,242 |
| EXHAUSTION | CANDIDATE | 4,171 |
| EXHAUSTION | CONFIRMED | 2,070 |
| EXHAUSTION | EXPIRED | 42 |
| EXHAUSTION | INVALIDATED | 817 |
| STACKED_IMBALANCE | DETECTED | 76,719 |

## Initial events by session (an event in two overlapping sessions counts in both)

- **ABSORPTION**: asia 559, london 752, new_york 463, none 802, weekday 1,527, weekend 661
- **BIG_TRADE**: asia 62,886, london 101,248, new_york 94,587, none 26,013, weekday 201,987, weekend 16,945
- **DIVERGENCE_CVD**: asia 563, london 641, new_york 562, none 273, weekday 1,441, weekend 236
- **DIVERGENCE_DELTA**: asia 655, london 691, new_york 634, none 484, weekday 1,715, weekend 365
- **EXHAUSTION**: asia 1,349, london 1,240, new_york 1,210, none 1,044, weekday 3,330, weekend 841
- **STACKED_IMBALANCE**: asia 26,634, london 35,931, new_york 34,375, none 3,331, weekday 75,606, weekend 1,113

## Outcomes (descriptive)

| Type | Initial | Terminal statuses | Confirmed rate |
|---|---|---|---|
| ABSORPTION | 2,188 | CONFIRMED 1,480, EXPIRED 197, FAILED 483, AMBIGUOUS 28 | 0.6764 |
| BIG_TRADE | 218,932 | - | - |
| DIVERGENCE_CVD | 1,677 | INVALIDATED 1,191 | - |
| DIVERGENCE_DELTA | 2,080 | INVALIDATED 1,456 | - |
| EXHAUSTION | 4,171 | CONFIRMED 2,070, AMBIGUOUS 1,242, INVALIDATED 817, EXPIRED 42 | 0.4963 |
| STACKED_IMBALANCE | 76,719 | - | - |

## Invariant checks (all must be 0)

| Check | Value |
|---|---|
| event_time_before_evidence_end | 0 |
| orphan_status_events | 0 |
| initial_events_without_trade_ids | 0 |
| null_unknown_lots | 0 |
| big_trade_id_count_mismatch | 0 |
| duplicate_event_ids | 0 |

Result: **PASS**
