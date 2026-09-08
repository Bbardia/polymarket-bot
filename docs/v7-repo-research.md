# V7 weather strategy research

This branch records a read-only review of five public repositories. The review
was static only; no downloaded code was executed and no V6 state was changed.
The complete source-pinned notes used for this decision are kept outside the
repository during the audit.

## Decision

V7 keeps the existing ensemble, market-anchor shrinkage, provider-health gate,
resolver-station checks, local-day observations, nonlinear fee model, and
executable depth walks. The useful, independently implemented change is
**opt-in fee-aware fractional-Kelly paper sizing**:

- size from the current paper bankroll and the conservative Kelly fraction;
- walk the ask book and recompute VWAP, fees, edge, and Kelly at the proposed
  quantity;
- cap by the weather order cap, current cash, and Kelly budget;
- skip a venue minimum that does not fit instead of rounding risk upward;
- recheck cash and Kelly budget immediately before committing each candidate;
- persist sizing inputs and execution-time budget for auditability;
- emit immutable `forecast_snapshots.jsonl` records for every evaluated weather
  side, including rejected candidates, with stable IDs, decision timestamps,
  model/provider probabilities, book/fee context, and an empty finalized label;
- preserve `forecast_issuance_at: null` when a provider does not expose issuance
  time instead of falsely treating retrieval time as issuance time;
- add a cached, request-spaced global 7Timer fallback for coverage gaps. It is
  treated as NOAA/GFS-derived and is skipped whenever Open-Meteo succeeds, so
  it does not create duplicate provider independence merely to pass a gate.

The flag is disabled by default. V7 remains paper-only and refuses to construct
with live trading or account reads enabled.

## What was and was not adopted

- `suislanchez/polymarket-kalshi-weather-bot`: ensemble exceedance counting and
  rejection diagnostics are useful concepts, but its path lacks depth/fee-aware
  execution and has settlement-label/unit defects. No code copied.
- `nicolastinkl/hermes_weatherbot`: Gaussian buckets and Kelly are useful
  hypotheses, but its current execution path confuses outcome prices with
  bid/ask, uses the condition ID as a token ID, and does not verify posting or
  fills. Not adopted.
- `yangyuan-zhen/PolyWeather`: the strongest research reference for
  lead-conditioned residual bias, provenance, caching, and settlement-source
  separation. It is AGPL-3.0 and its calibration path still has point-in-time
  and Fahrenheit-boundary issues. No code or coefficients copied.
- `MoonsatProtocol/Polymarket-Weather-Bot`: observed-running-maximum handling
  is directionally useful, but its prices, fixed 5% cash sizing, and displayed
  P&L are not executable or fee-aware. The local bot already has stronger
  observation logic.
- `AruneshDev/Automated-Trading-System-Kalshi-Weather-Model`: not adopted;
  inspected models contain target leakage/random splitting and fixed-size
  orders without fee/depth evaluation.

None of the repositories establishes a reproducible net profit advantage.
V7 therefore does not loosen entry thresholds, disable provider gates, copy
calibration constants, change exits, or enable live execution.

## Evaluation gate

Run V7 as a fresh, isolated paper campaign. Compare it with the unchanged V6
lane using resolved outcomes and conservative executable-bid marks, normalized
returns, exposure, holding time, provider health, Brier/log loss, and rejection
reasons. A small positive P&L or a passing test suite is not promotion evidence.

A later residual-bias experiment should use the new immutable forecast snapshot
stream and separately joined finalized station labels. Until enough trustworthy
labels and point-in-time provenance exist, fitting a correction would risk
look-ahead and is deliberately not included in this branch.
