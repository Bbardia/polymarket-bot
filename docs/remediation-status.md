# Remediation status

Date: 2026-09-11
Scope: implementation and public/paper-only verification. No live orders, account reads, worker restarts, capital changes, or repository pushes were performed.

## Status legend

- **implemented/tested**: code exists and automated verification passed.
- **evidence-blocked**: the safe capability exists, but the required historical sample, point-in-time denominator, or observation window is not available.
- **not-authorized**: the requested verification requires authenticated/live venue behavior and was deliberately not performed.
- **parked**: the lane is disabled because the review found negative or untrustworthy expectancy.

## Numbered plan

| Item | Status | Implementation / evidence | Remaining gate |
|---|---|---|---|
| 1 | implemented/tested | Entries default to disabled for directional, ladder, and V8 paper profiles; scanners and historical ledgers remain readable. | User authorization would be required for any future opt-in campaign. |
| 2 | implemented/tested | Bid-side executable VWAP/fee marking, zero mark for insufficient depth, MTM drawdown, gross exposure, persistent peak migration, partial-depth telemetry, and between-fill risk checks. | Re-run against a fresh campaign only after code review; do not use old ledgers as a new sample. |
| 3 | implemented/tested | Additive full-cohort public CLOB winner reconciliation with bounded retries, provenance, lot ambiguity checks, hold-versus-exit P&L, and unresolved-stake publication gate. | Historical point-in-time labels and any failed public fetches still need replay. |
| 4 | implemented/tested | Chronology, geography, probability, partition, timestamp-age, spread, depth, and finite-value sanity checks; BLS/Fed regression fixtures. | No promotion until all upstream data sources provide the required provenance. |
| 5 | implemented/tested | Resolver identity is parsed per market; station metadata, map disagreement, unsupported authorities, Hong Kong, and malformed URLs fail closed. Public station metadata was checksum-verified. | The complete historical market set still needs label backfill. |
| 6 | evidence-blocked | Durable METAR/SPECI station-label collector, AviationWeather/IEM adapters, DST-local windows, Fahrenheit T-group handling, immutable cache provenance, manifest validation, and CLI are implemented. A public two-event smoke produced 2 labels and 0 winner validations. | Required gate is at least 500 validated settled events at >=99.5% agreement; current evidence is insufficient. |
| 7 | implemented/tested; evidence-blocked | Full-ladder PMF module supports two-sided/one-sided quotes, power de-vigging, missing-rung reporting, and >=9/11 coverage. | Required 95% coverage over at least 40 dates is not available; PMF cannot promote a lane alone. |
| 8 | implemented/tested; evidence-blocked | Gaussian/Student-t CRPS, PIT/KS, interval coverage, Brier decomposition, date-cluster bootstrap, and chronological evaluation checks are implemented. Empty evaluation returned `insufficient_data`. | Forecast promotion requires clustered out-of-sample evidence over at least 400 observation-day clusters and baseline comparisons. |
| 9 | implemented/tested | Machine-readable gate registry covers blocked legacy lanes, forecast, station truth, PMF, maker calibration, maker phi, maker variants, extremization, parked pockets, and live microtest. | All empirical gates remain fail-closed until their evidence is supplied. |
| 10 | implemented/tested; evidence-blocked | Timestamped maker tape replay supports queue decay, cancel/join-ahead, repricing IDs, expiry, ghost fills, markouts, and conservative reward accounting. | Independent held-out tape calibration is absent; current calibration report is `insufficient_data`. |
| 10b | implemented/tested; evidence-blocked | `phi` report requires a complete quote-intention denominator, queue strata, date clusters, and clustered lower bound above 0.816. Fills alone return `identifiability_blocked`. | Public trade records do not contain the complete unfilled-intention denominator; no phi promotion claim is made. |
| 11 | implemented/tested; evidence-blocked | Shadow-only T+1 maker tick supports separate reward-band and modal-rung variants, resolver/queue/spread/depth gates, blackout/cancel telemetry, and never submits orders. | Disabled until item 10b passes and a two-week shadow sample supports a positive clustered CI. |
| 12 | implemented/tested; evidence-blocked | Leave-one-date-out lead-indexed PMF power fit is clamped to [1.00, 1.35] with fallback 1.00 and no future-label use. | Requires at least 40 dates and a positive clustered out-of-sample result. |
| 13 | implemented/tested; parked | New decision helper uses executable all-in cost, friction, and clustered uncertainty; legacy ICC/ESS path remains only for reproducibility. | No new forecast-driven entry path is promoted. |
| 14 | implemented/tested; parked | Admissibility sizing applies variance shrinkage, same-date design effect, event/date caps, and skips when the venue minimum would require rounding up. | Requires an admitted lane and validated fair-value distribution. |
| 15 | implemented/tested; parked | New policy holds to resolution and limits exits to dead-rung, maker-only edge-flip, and resolver-certain closes; legacy hybrid records remain readable. | No exit policy is promoted without executable-depth evidence. |
| 16 | implemented/tested; not-authorized | TTL floor, rejection/retry state design, confirmed-fill-only inventory lifecycle, cancel-all abstraction, and mock coverage are implemented. | Real order, account-stream, heartbeat, and <$5 microtest remain explicitly unauthorized. |
| 17 | implemented/tested; parked | Forecast hygiene uses Student-t tails, lead/global and city-offset structure, and model weight zero; forecast is veto/telemetry only. | CRPS gate must pass before any model fair-value consumer is considered. |
| 18 | implemented/tested; parked | Routine-window running-max telemetry uses resolver-compatible observations, dead-rung veto logic, and unfillable telemetry. | No taker running-max lane will be enabled. |
| 19 | implemented/tested; parked | Dependency audit is additive and conservative; failed lanes remain available for historical replay while default entry paths are disabled. | Broad deletion remains deferred until a manual import/operational audit. |

## Verification executed

- Main remediation suite: **280 passed, 1 skipped**.
- V8 remediation suite: **236 passed, 1 skipped**.
- Main syntax compilation: passed for `src`, `scripts`, and `run_v3.py`.
- V8 syntax compilation: passed for `src`, `tests`, and `run_v8.py`.
- Station collector smoke: 2 public events, 2 labels, 0 winner validations, gate `insufficient_data`.
- Gate registry CLI: all blocked/insufficient-data verdicts returned fail-closed statuses.
- Maker tape calibration CLI: `insufficient_data`; parameters not usable.
- Maker phi CLI without a complete quote denominator: `identifiability_blocked`.
- Empty replay CLI: valid zero-event report; summary objects are not treated as tape events.

## Operational boundary

The active paper workers remain stopped. Paper entries remain disabled in the explicit campaign profiles. No live-capable path was activated, no authenticated endpoint was used, and no historical ledger was reset or rewritten.
