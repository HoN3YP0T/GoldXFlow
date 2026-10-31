# Phase 10 Initial Balance verification

Instances: 262; rebuilt from raw trades (FULL coverage): 262; skipped: none; mismatches: **0** (PASS)

Checks per instance: IB high/low/volume/buy/sell straight from the trades partitions, and every developing row, event and session summary rebuilt from bars recomputed from the trades partitions.

## Event counts

| IB | ACCEPTED_ABOVE | ACCEPTED_BELOW | BREAKOUT_DOWN | BREAKOUT_HELD_DOWN | BREAKOUT_HELD_UP | BREAKOUT_UP | FAILED_BREAK_DOWN | FAILED_BREAK_UP | IB_DEVELOPING | IB_FORMED | RANGE_EXT_DOWN | RANGE_EXT_UP | RETURN_INSIDE_DOWN | RETURN_INSIDE_UP | SESSION_SUMMARY |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| london | 148 | 182 | 327 | 125 | 100 | 265 | 195 | 158 | 7860 | 131 | 294 | 256 | 75 | 53 | 131 |
| new_york | 102 | 137 | 248 | 100 | 75 | 230 | 148 | 149 | 7860 | 131 | 164 | 129 | 59 | 41 | 131 |

## IB start times around the 2026 DST changes (UK 25 Oct, US 1 Nov)

| IB | Local date | IB start (UTC) | IB end (UTC) |
|---|---|---|---|
| london | 2026-10-19 | 2026-10-19T07:00:00.000Z | 2026-10-19T08:00:00.000Z |
| new_york | 2026-10-19 | 2026-10-19T12:20:00.000Z | 2026-10-19T13:20:00.000Z |
| london | 2026-10-20 | 2026-10-20T07:00:00.000Z | 2026-10-20T08:00:00.000Z |
| new_york | 2026-10-20 | 2026-10-20T12:20:00.000Z | 2026-10-20T13:20:00.000Z |
| london | 2026-10-21 | 2026-10-21T07:00:00.000Z | 2026-10-21T08:00:00.000Z |
| new_york | 2026-10-21 | 2026-10-21T12:20:00.000Z | 2026-10-21T13:20:00.000Z |
| london | 2026-10-22 | 2026-10-22T07:00:00.000Z | 2026-10-22T08:00:00.000Z |
| new_york | 2026-10-22 | 2026-10-22T12:20:00.000Z | 2026-10-22T13:20:00.000Z |
| london | 2026-10-23 | 2026-10-23T07:00:00.000Z | 2026-10-23T08:00:00.000Z |
| new_york | 2026-10-23 | 2026-10-23T12:20:00.000Z | 2026-10-23T13:20:00.000Z |
| london | 2026-10-26 | 2026-10-26T08:00:00.000Z | 2026-10-26T09:00:00.000Z |
| new_york | 2026-10-26 | 2026-10-26T12:20:00.000Z | 2026-10-26T13:20:00.000Z |
| london | 2026-10-27 | 2026-10-27T08:00:00.000Z | 2026-10-27T09:00:00.000Z |
| new_york | 2026-10-27 | 2026-10-27T12:20:00.000Z | 2026-10-27T13:20:00.000Z |
| london | 2026-10-28 | 2026-10-28T08:00:00.000Z | 2026-10-28T09:00:00.000Z |
| new_york | 2026-10-28 | 2026-10-28T12:20:00.000Z | 2026-10-28T13:20:00.000Z |
| london | 2026-10-29 | 2026-10-29T08:00:00.000Z | 2026-10-29T09:00:00.000Z |
| new_york | 2026-10-29 | 2026-10-29T12:20:00.000Z | 2026-10-29T13:20:00.000Z |
