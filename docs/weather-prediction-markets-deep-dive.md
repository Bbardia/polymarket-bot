# Weather prediction-market deep dive

**Research date:** 2026-09-08  
**Scope:** Polymarket/Kalshi temperature markets, forecast calculation,
calibration, sizing, execution, and publicly reported trader experience.

This is a research memo, not evidence that any strategy is profitable. Claims
from commercial pages, repositories, social posts, and short student projects
are separated from reproducible evidence.

## Executive conclusion

The best current method for us is not a new neural network or a copied bot. It
is a settlement-matched, probabilistic temperature forecast with strict
point-in-time calibration, compared against executable market prices and sized
with conservative fee-adjusted fractional Kelly.

The practical order of work is:

1. **Settlement identity first.** Match the exact city, station, local date,
   unit, rounding, reporting window, and official resolver. Polymarket's US
   weather documentation says temperature contracts settle from the local NWS
   Daily Climate Report and lists station-specific sources.[5]
2. **Use a forecast distribution, not a point forecast.** Integrate the
   distribution over the contract's native bucket boundaries. Do not round a
   Celsius distribution into Fahrenheit labels or treat the latest temperature
   as the final daily high. Probabilistic weather decisions require a range of
   scenarios rather than only a deterministic forecast.[8][11]
3. **Prefer a calibrated station-level ensemble where available.** The NWS
   National Blend of Models is explicitly designed as a calibrated blend and
   bias-corrects and weights many model systems.[9] ECMWF research also treats
   ensemble post-processing as necessary because raw ensembles have bias and
   dispersion errors.[10]
4. **Calibrate strictly out of sample.** Store immutable decision-time
   forecasts and join finalized station truth later. Fit residual bias, spread,
   or EMOS parameters only on information available before each decision.
5. **Treat the market as a baseline, not as truth.** Calibrate market prices by
   domain, horizon, price range, and platform. A current large preprint finds
   calibration varies materially with domain, horizon, and trade size; its
   weather result is a hypothesis to test, not a coefficient to import.[12]
6. **Trade only executable net edge.** Polymarket documents a nonlinear taker
   fee, and its order books expose levels, sizes, minimum order size, tick size,
   and a book hash.[1][2] Use the actual ask depth for entry and bid depth for
   marks/exits.
7. **Size after uncertainty and correlation adjustments.** Use conservative
   probability, all-in cost, available cash, event/city exposure caps, and
   fractional Kelly. Never force a venue minimum that is larger than the risk
   budget.

V7 implements item 7 and now emits an immutable `forecast_snapshots.jsonl`
stream for every evaluated weather side, including rejected candidates. The
stream carries stable IDs, decision timestamps, model/provider probabilities,
book/fee context, and explicit empty labels. Items 2–4 still require finalized
station joins and enough data for calibration; the snapshot stream does not by
itself prove forecast skill.

## What the calculation should be

### Binary contract

Let:

- `q` = our probability that the purchased outcome wins;
- `p` = the price at which a share actually fills;
- `fee(p)` = the taker fee per share at that fill price;
- `c = p + fee(p)` = all-in cost per share;
- payout = 1 USDC if the outcome wins, 0 otherwise.

Ignoring exit trading, the expected profit per share is:

```text
EV/share = q - c
```

A positive raw difference `q - p` is not enough. The edge must survive fees,
spread, depth, stale-book risk, and forecast uncertainty. Polymarket's current
documented taker formula is `C × feeRate × p × (1-p)`; weather's listed rate is
0.05, makers are listed as fee-free, and fees are rounded to five decimals.[1]
The implementation must continue reading market-specific metadata rather than
assuming every market has the same schedule.

For a binary contract, the full Kelly fraction of bankroll committed as cost,
using a known all-in cost `c`, is:

```text
f_full = (q - c) / (1 - c)
```

when `q > c`; otherwise it is zero. This is the same fee-adjusted structure
used by the current V3 math. The number of shares implied by a chosen bankroll
fraction is then constrained by the executable book, order cap, and minimum
size. In practice `q` is not known, so full Kelly is too sensitive to model
error. The research on Kelly in prediction markets explicitly studies the
impact of misestimating both belief and investment fraction.[6]

V7 therefore uses a small fractional-Kelly budget and recomputes the decision
at the final VWAP rather than taking the minimum-size signal and multiplying it
blindly.

### Multi-bucket temperature market

For mutually exclusive buckets `B_1 ... B_k`, the forecast should produce:

```text
q_i = P(resolver outcome is in B_i | information available at decision time)
Σ q_i ≈ 1
```

The integral must use the exact resolver-native boundaries. For an interval:

```text
q_i = F(upper_i) - F(lower_i)
```

with appropriate open/closed endpoint handling. For an unbounded tail, use the
remaining CDF mass rather than dropping it. If the market is an exact integer
contract, the mapping must follow its settlement rule; it is not safe to infer
that a displayed `28°C` and a displayed `82°F` represent the same boundary.

A complete-set or cross-bucket trade is only a candidate when all legs belong
to the same verified event, can be filled at the required quantity, and remain
profitable after every leg's fee and depth walk. Polymarket distinguishes
market ID, condition ID, and outcome token ID; the token ID is what connects an
outcome to its order book.[3] The current V7 should keep complete-set logic
shadow-only until historical execution and event-membership validation are
strong enough.

## Forecast construction

### Baselines to beat

A model should be compared against all of these, separately by city and lead:

- current market ask/mid/bid-derived probability, with leakage-safe timing;
- climatological distribution for the resolver station and calendar period;
- persistence or latest observed running high where legally informative;
- raw GFS and ECMWF ensemble distributions;
- a station-matched operational blend such as NBM where coverage and resolver
  identity match;
- a calibrated blend or EMOS-style post-processor.

A short Notre Dame project is particularly useful because it did not assume a
complex model must win. It collected 29 NYC daily market datasets, used the
LaGuardia resolver station, and evaluated several forecasts against the market.
In its hypothetical validation, a naive model and ARIMA had the best reported
net P/L, while linear and neural models lost.[13] The result is not promotion
 evidence: it is a small sample, the report uses a simplified predicted-bucket
selection, and its $100 hypothetical trades do not constitute a full
fee/depth/fill audit.

### Calibration method

For each decision, persist:

- forecast issuance and retrieval time;
- valid local date and resolver station;
- provider/model family and version;
- raw ensemble members or sufficient distribution statistics;
- model probability for every bucket, not only the selected side;
- market book hash, ask/bid depth, fee schedule, and minimum order size;
- rejection reason and the exact probability used for sizing.

Later, join the finalized official outcome idempotently. Then evaluate:

- multiclass Brier score for bucket probabilities;
- log loss, with careful clipping only for numerical safety;
- reliability diagrams by probability bucket;
- CRPS or interval coverage for the continuous temperature distribution;
- residual mean, median, and spread by station, lead, season, and model family;
- net P/L and executable marks, always separate from forecast scores.

Use chronological walk-forward evaluation. A random train/test split is invalid
for this use because it permits future weather regimes, revised forecasts, or
future settlement information to influence the past.

A modest first calibration model should be pooled lead-conditioned residual
bias and dispersion, shrunk toward zero for sparse station/city strata. EMOS is
a principled reference: it maps ensemble statistics through a parametric
predictive distribution and was developed specifically to address ensemble
bias and dispersion.[10] We should not add a learned correction until the tape
contains enough independent finalized station-days and the correction beats
raw ensemble, market, and climatology baselines on a held-out period.

### Why the market must be segmented

A 2026 preprint analyzing 292 million trades across 327,000 Kalshi and
Polymarket contracts reports that calibration is not universal: horizon,
domain, domain-by-horizon interaction, and trade-size effects explain much of
its observed variation. Its reported weather pattern is overconfidence at
short horizons and a different pattern at longer horizons.[12] It is a
preprint, not a settled fact, and it is not weather-station-specific evidence.
It does justify measuring V7 by:

- city and resolver station;
- local lead time and time-to-market-close;
- bucket/tail and price range;
- provider availability and forecast revision age;
- event and city correlation;
- trade size and maker/taker route.

Do not apply a global market-price correction.

## Execution and market structure

Polymarket's documentation says the CLOB is organized by outcome token, with
resting bids and asks and sizes at each price level; the book response includes
minimum order size, tick size, timestamp, and a hash.[2] The trading workflow
requires selecting an outcome token, signing and submitting an order, then
monitoring fills and cancellations.[4] This supports the current V7 emphasis
on executable depth and explains why displayed outcome prices alone are not
trade prices.

Operationally, each candidate needs three separate prices:

1. **entry ask VWAP** for the exact desired quantity;
2. **conservative exit bid VWAP** for the exact currently held quantity;
3. **settlement payout** only after the official resolver is authoritative.

The current Polymarket weather page shows a broad set of daily high/low and
other weather markets with visible displayed prices and volume, but those
snapshots are dynamic and are not a historical performance dataset.[17]

Maker execution is a separate experiment. The documented fee advantage is real
in principle, but a resting quote adds non-fill, queue-position, cancellation,
and adverse-selection risk. We should not count maker fee savings as profit
until a fill model is validated against book updates and partial fills.

## What traders and public projects report

These sources are useful for generating hypotheses, not for proving returns.

- **WeatherEdge Bot:** its archived README claims 82 ensemble members, GFS plus
  ECMWF, NWS bias correction, quarter-Kelly, an 8% minimum edge, and an 81%
  result over 16 trades. The repository contains a README/sales funnel rather
  than a reproducible ledger, full implementation, or independent audit.[14]
- **Predict & Profit:** its commercial page describes NBM-based station-level
  guidance, five additional raw systems as a disagreement check, fee/liquidity
  gates, and fewer than 5% of scanned opportunities entering. These are design
  claims from a product page; there is no independently verifiable return
  series in the retrieved material.[15]
- **DevGenius/Medium:** the post repeats claims such as turning $1,000 into
  $24,000 in London weather and another bot making $65,000. The article is
  partially paywalled and does not provide an auditable order-level ledger,
  settlement reconciliation, fees, or drawdown history in the accessible text.
  Treat it as marketing/anecdotal evidence only.[16]
- **Kalshi academic evidence:** a 2026 George Washington University working
  paper analyzes more than 300,000 contract observations and reports that
  prices become more accurate near closing but show a favorite-longshot pattern;
  it reports poor returns for very low-priced taker contracts and small positive
  returns for higher-priced contracts. This is Kalshi evidence, not a direct
  Polymarket result, but it is a strong warning against high-win-rate,
  low-price strategies and against ignoring fees.[7]

The consistent practitioner lesson is selectivity, station-specific data, and
execution discipline. The consistent evidence problem is the absence of public,
independently reconciled, fee- and depth-aware weather-bot ledgers.

## Research-backed strategy choices

### Adopt now in V7

- fee-aware fractional-Kelly sizing;
- exact ask-depth and fee recomputation after sizing;
- cash and Kelly-budget recheck before each entry;
- preserved station, provider, local-day, and resolution-source gates;
- rejection telemetry separating signal failure, liquidity, cash, and cap
  rejection;
- fresh isolated paper campaign for all comparisons.

### Build next, but only behind replay/shadow tests

1. **Immutable forecast tape.** Capture decision-time forecasts and book hashes
   for all evaluated markets, including rejected ones.
2. **Resolver registry.** Store station, source URL, local timezone, daily
   window, unit, rounding, and revision policy as a versioned contract schema.
3. **NBM lane.** Where NBM coverage and settlement station match, compare NBM
   against the current multi-provider blend instead of assuming NBM is better.
4. **Pooled residual correction.** Fit bias and dispersion by lead using only
   earlier finalized station-days; shrink sparse strata to pooled estimates.
5. **Market calibration layer.** Estimate price-to-frequency calibration by
   platform, horizon, city, bucket, and price range; compare it against the
   raw market and forecast model.
6. **Matched historical replay.** Recreate point-in-time asks, available depth,
   fees, order caps, and latency. Do not use today's order book to backtest
   yesterday's trade.
7. **Maker shadow lane.** Model queue and fill probability before allowing maker
   economics into P/L.
8. **Cross-platform relative value.** Test only contracts with identical
   settlement semantics, synchronized timestamps, sufficient depth on both
   legs, and explicit non-atomic leg risk.

### Keep rejected

- copying public bot code or constants, especially from AGPL projects;
- trusting a README win rate or commercial sales page;
- adding a large neural network before beating persistence, climatology, market,
  and calibrated ensemble baselines;
- using latest temperature as the final high;
- treating outcome prices as order-book bid/ask prices;
- using full Kelly on an estimated probability;
- counting unresolved positions as settled profit;
- changing the V6 lane or enabling live trading during this research phase.

## Promotion gate

A model or sizing change should only graduate from shadow testing when it has:

- a sufficient number of independent station-days and resolved contracts;
- chronological out-of-sample Brier/log-loss/CRPS results better than the
  selected baselines or a clearly documented tradeoff;
- positive net expectancy after exact fees and conservative depth fills;
- stable results across cities, horizons, buckets, and price ranges;
- calibrated uncertainty and no single-provider dependency;
- reconciled settlement and executable-bid P/L;
- drawdown, loss-streak, correlated-event, and stale-data controls exercised;
- a fresh paper window that reproduces the replay conclusion.

Until then, V7 is a better-measured paper experiment, not a proven profit
engine.

## Sources

[1] https://docs.polymarket.com/trading/fees
[2] https://docs.polymarket.com/market-data/prices-order-books
[3] https://docs.polymarket.com/market-data/market-details
[4] https://docs.polymarket.com/trading/overview
[5] https://docs.polymarket.us/faqs/weather-faqs
[6] https://arxiv.org/html/2412.14144v1
[7] https://www2.gwu.edu/~forcpgm/2026-001.pdf
[8] https://www.nature.com/articles/s41586-024-08252-9
[9] https://www.weather.gov/news/200318-nbm32
[10] https://journals.ametsoc.org/view/journals/mwre/133/5/mwr2904.1.xml
[11] https://economics.sas.upenn.edu/pier/working-paper/2001/weather-forecasting-weather-derivatives
[12] https://arxiv.org/html/2602.19520v1
[13] https://github.com/aheck3/nyc-temperature-forecasting-polymarket/raw/refs/heads/main/forecasting-polymarket-temp-report.pdf
[14] https://github.com/Stewyboy1990/weatheredge-bot
[15] https://predictandprofit.io/prediction-market-trading-bot
[16] https://blog.devgenius.io/found-the-weather-trading-bots-quietly-making-24-000-on-polymarket-and-built-one-myself-for-free-120bd34d6f09
[17] https://www.polymarket.com/weather
