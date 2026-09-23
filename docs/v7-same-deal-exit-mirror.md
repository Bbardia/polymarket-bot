# V7 Same-Deal Exit/Hybrid Mirror Experiment

**Branch:** `experiment/v7-same-deal-mirror-20260923`

**Purpose:** Compare exit/hold policies on the exact trades V7 main actually enters, without running another weather scanner.

**Mode:** Paper-only shadow ledgers; no order submission, wallet access, or weather/Meteo forecast/observation calls.

## Source and paired cutover

The source is the running V7 main campaign’s append-only `paper_trades.jsonl`. At first launch, the mirror waits for a stable, healthy source snapshot, seeds every arm with the same *currently open* positions and cash, records the source byte offset, and resets the arm’s realized P&L/trade counters to zero. Historical settlements/exits are not copied into the comparison sample. New V7 trades after that byte offset are mirrored into every arm at their recorded quantity and all-in cost, with an idempotent source audit ID.

This is a **paired, forced-entry comparison**: all arms receive the same main-selected deals. The mirror does not discover markets, create independent entries, or call Open-Meteo, MET Norway, NWS, JMA, or NOAA. It uses public Gamma/CLOB reads for resolution, executable bids, and market-specific fees. Separate mirror ledgers are written under the campaign’s ignored `arms/` directory; the main ledger remains read-only to the mirror.

The experiment measures post-entry exit/settlement policy on V7’s actual trade stream. It does **not** estimate whether ladder/Kelly changes would select different markets or sizes; those entry features are upstream in main and their actual chosen trades are what every arm replays. Paired forced entries can exceed a mirror arm’s independent position/cash/gross-exposure limits after the policies diverge. Such overrides are logged and the arm is a counterfactual, not an independently deployable portfolio; do not interpret its P&L as standalone capital feasibility or extra-trade capacity.

## Arms

Every arm receives the same main entry stream and uses public bid-depth/fee-aware exits:

- `hold` — no early exits; settlement is the fallback.
- `full-25` — full-position exit at 25% net return, subject to the $0.10 minimum profit.
- `full-15` — full-position exit at 15% net return, same minimum profit.
- `full-10` — full-position exit at 10% net return, same minimum profit.
- `hybrid-25` — 75% partial exit at 25% and a 50% runner target; residual shares can settle.

Hybrid partial quantities below venue minimum size are **simulation-only**. The existing V7 `_exit_positions` policy only exits `weather_directional` positions; ladder baskets are mirrored and settled but do not receive early/hybrid exits in this experiment. Complete-set trading remains disabled in the main profile.

## Safety and quota boundaries

- V7 main remains paper/public-only, with live trading/account reads disabled and its existing capital, reserve, per-order, position, realized-loss, mark-drawdown, and gross-exposure caps unchanged.
- The mirror profile disables independent entries and weather providers. The mirror code refuses a weather-enabled profile.
- The mirror never writes to V7 main’s state, trade, settlement, quota, or forecast files; it only reads main’s paper trade source. No Open-Meteo quota is consumed by the mirror.
- CLOB/Gamma reads are still made to mark/settle positions. The engine shares one public client but the exit calls are performed per arm; monitor those non-weather API requests separately.
- The old `v7-weather-allfeatures-20260916` and `v7-frequency-treatment-20260919` ledgers are historical campaign data under `main`, not Git branches. They remain untouched for auditability.

## Promotion/rejection criteria

Compare each mirror arm only over the post-cutover paired window. Report entries mirrored, exits, settlements, early-exit P&L, settlement P&L, open cost, executable bid mark, mark equity, holding time, source rows skipped, and forced-cap override counts. Keep each arm separate. Do not select a target from trade count or early-exit P&L alone; require resolved outcomes, non-degraded source health, and a date/event-clustered uncertainty comparison. The live V7 main is the source policy and continues independently.
