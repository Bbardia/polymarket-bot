# Paper hybrid model research

## Decision

Run two isolated public-data-only paper campaigns:

- `v5-paper`: the existing main campaign, full-position early exits at a 28%
  return-on-cost target.
- `v5-hybrid-28-paper`: a fresh bounded campaign that exits 75% of a weather
  position at 28%, retains 25% as a runner, and exits the runner only at 50% or
  lets it resolve normally.

The hybrid campaign is a paper simulation. A 75% exit of the current five-share
paper minimum can be below the venue's minimum order size, so it must not be
interpreted as executable live performance.

## Evidence from the local paper ledger

The previous 25% campaign had 21 early exits in the earlier audit window. Of 15
that later resolved publicly, holding to resolution beat the early exit on 12 and
the early exit beat holding on 3; aggregate hold-to-resolution P&L was 4.62832350
pUSD higher for that resolved subset. The result is selection-biased and is not a
proof of future performance, but it motivates retaining a runner instead of
selling every share at the first target.

The completed 30% test produced six exits, all above 30%, but had no settlements
and only five open positions. It was therefore suggestive, not a sufficient basis
for replacing the main strategy.

## Current implementation hypothesis

The hybrid lane tests whether partial profit capture can preserve the downside
protection of an early exit while retaining upside when a forecast-driven move
continues. It uses the same executable bid-depth walk and nonlinear fee math for
both stages. It allocates entry cost proportionally when the first partial exit is
recorded, so the residual position can settle independently without double-counting
cost or P&L.

## External mechanics checked

- [Polymarket negative-risk markets](https://docs.polymarket.com/concepts/negative-risk):
  multi-outcome events where only one outcome can win and negative-risk
  conversion changes position/inventory mechanics.
- [Polymarket fees](https://docs.polymarket.com/trading/fees): makers are not
  charged trading fees; weather is listed as a fee-enabled category with a
  taker-fee curve.
- [Polymarket market making](https://docs.polymarket.com/trading/market-making):
  requires minimum-size validation, inventory tracking, stale-quote cancellation,
  fill monitoring, and explicit risk controls.

These sources support researching maker execution and negative-risk baskets, but
they do not establish profitability. The current hybrid lane remains directional
weather only; negative-risk baskets are not enabled until event membership,
resolution semantics, depth, fees, and non-atomic leg risk are independently
validated.

## Evaluation rule

Do not declare the hybrid model better from realized early-exit P&L alone. Compare
both campaigns separately using:

- entries, partial/full exits, settlements, and remaining exposure;
- realized P&L plus conservative executable-bid marks;
- exit return and holding-time distributions;
- resolved hold-versus-exit outcomes;
- provider availability, observation errors, and calibration metrics.

Keep live trading and authenticated account reads disabled throughout the test.
