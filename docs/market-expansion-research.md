# Market expansion research

**Research date:** 2026-09-08  
**Scope:** Market families that could use the weather lane's core method:
objective settlement, point-in-time data, probability distributions, executable
prices, fees, and conservative sizing.

This memo ranks research candidates. It does not authorize new trading lanes,
change V6, or claim that any candidate is profitable.

## Executive ranking

| Rank | Market family | Method fit | Main edge hypothesis | Current recommendation |
|---|---|---:|---|---|
| 1 | Cross-platform relative value | High mathematical fit | Same event priced differently on Polymarket and Kalshi | Shadow scanner first; highest potential but highest settlement/leg risk |
| 2 | Macro releases and Fed decisions | High | Public nowcasts and rate-path distributions versus market-implied distributions | Best forecasting extension after weather |
| 3 | Negative-risk/combinatorial events | Very high mathematical fit | Complete-set/conversion inequality after fees and depth | Build an offline verifier; do not trade until event membership is exact |
| 4 | Sports | Medium | Team/player model probability versus executable market price | Later, selectively; highly competitive and fee-sensitive |
| 5 | BTC/crypto short-duration markets | Medium-low for us | Oracle-aware short-horizon distribution and order-book microstructure | Shadow only; do not start with live or paper exposure |
| 6 | Counts, mentions, culture, and politics | Low-medium | Poisson/count models or information timing | Avoid until a source, label, and replayable edge are demonstrated |

Polymarket's current public listings show meaningful activity across sports,
finance/Fed, crypto, and other categories, but live volume/liquidity snapshots
are not proof of a repeatable strategy.[10]

## 1. Cross-platform relative value: highest mathematical potential

### Structure

If Polymarket and Kalshi offer contracts with exactly the same event, outcome,
cutoff, source, unit, timezone, and resolution rule, compare executable prices.
For complementary positions, a simplified guaranteed-payoff test is:

```text
all_in_cost = ask_A(YES) + ask_B(NO) + fees_A + fees_B
locked_payout = 1
net_edge = locked_payout - all_in_cost
```

For unequal currencies, collateral, settlement times, or contract sizes, convert
all terms explicitly. Walk both books at the desired quantity. Require either
atomic execution or a bounded, explicitly priced non-atomic leg risk.

A 2026 SSRN working paper reports persistent cross-platform price differences in
two case studies—a legislative bill and a Federal Reserve decision—and reports
positive mean arbitrage after fees.[8] It is a preliminary working paper, not
independent proof that the same edge is available now. Its narrow event sample
makes it a research lead, not a coefficient or expected return.

A public Polymarket/Kalshi arbitrage repository demonstrates the practical
architecture—market matching, category-based comparison, and an opportunity
feed—but its README and dashboard are not an audited profitability record.[9]
The reusable idea is the matching workflow, not its displayed opportunities.

### Why it fits us

- The payoff relation is clearer than a directional forecast.
- The same executable book, fee, VWAP, and conservative mark infrastructure
  already exists in V7.
- The main new work is event matching, resolution-rule equivalence, capital
  allocation, and two-leg execution.
- Public CLOB feeds expose book updates, timestamps, hashes, and trade events,
  allowing a real-time shadow scanner instead of repeated polling.[2]

### Hard risks

- “Same question” does not mean same settlement. Official rules define the
  source, end date, edge cases, and resolution process; Polymarket resolves via
  an UMA optimistic oracle.[4]
- One leg can fill while the other moves away or disappears.
- Capital can remain locked on one platform after the other leg settles.
- Currency conversion, transfer delay, KYC/jurisdiction, and platform fees can
  erase a displayed spread.

### Verdict

**Best next research lane.** Build an offline/read-only matcher that only emits
an opportunity when the rules are byte-level or manually verified equivalent.
Do not call a price difference arbitrage until both legs have executable depth
and the resolution sources are identical.

## 2. Macro releases and Federal Reserve decisions: best forecasting extension

### Structure

This is closest to the weather approach. The contract outcome is an official
number or decision, and public information updates over time.

For an outcome bucket `B_i`, estimate a point-in-time predictive distribution:

```text
q_i = P(release or decision lands in B_i | information available at t)
EV_i = q_i - executable_cost_i
```

Use a nowcast ensemble built from:

- Cleveland Fed daily CPI/PCE nowcasts;
- official release calendars and prior vintages;
- energy, food, labor, and financial inputs for CPI/PCE;
- Fed funds futures, OIS, yield-curve and meeting-path data for rate decisions;
- market-implied probability as a baseline, not as an unquestioned truth.

The Cleveland Fed publishes daily CPI and PCE nowcasts, which creates a
replayable public signal with the same “forecast before settlement” structure as
weather.[7] A Federal Reserve Board working paper finds Kalshi macro markets
provide high-frequency, distributionally rich expectations for inflation,
payrolls, unemployment, GDP, and Fed decisions, and compares them with surveys
and traditional market forecasts.[6] That supports the market family as a
serious forecasting object, but not an automatic trading edge.

### Mathematical model

For CPI or payroll ranges, use a mixture distribution rather than one point
estimate:

```text
q(B_i) = integral over B_i of predictive_density(x) dx
```

Then estimate calibration by release type, time-to-release, surprise regime,
and bucket. For Fed decisions, build a discrete path distribution over the
possible target-rate outcomes and account for meeting-by-meeting dependency.

Use a conservative edge rule:

```text
lower_confidence_bound(q_i) - ask_i - fee_i
    > uncertainty_buffer + execution_buffer
```

### Risks

- Data revisions and benchmark changes can make a backtest use information that
  was not available at the decision time.
- Market rules may refer to a specific release vintage or rounding convention.
- Macro markets can jump faster than a polling worker can execute.
- The market may already incorporate the public nowcast before our scan.

### Verdict

**Strong second lane.** Build a paper-only macro data tape and compare the
Cleveland Fed/market/consensus baselines before adding a model. Start with
slower CPI/PCE monthly buckets rather than intraday Fed or payroll markets.

## 3. Negative-risk and combinatorial markets: strongest pure logic

Polymarket's negative-risk mechanism links multi-outcome markets where only one
outcome can win. The documentation states that a No share in one market can be
converted into one Yes share in every other market.[3]

### Candidate inequalities

For a verified complete partition of `k` outcomes:

```text
sum(executable YES asks_i) + all fees < 1
```

may imply a complete-set opportunity. Other conversion relationships create
similar inequalities involving No shares. Every leg must be from the same event,
with complete membership, correct named outcomes, sufficient depth, and a
conversion/settlement path that is actually available.

### Why it fits us

- It is largely deterministic rather than forecast-dependent.
- V7 already has complete-set math, nonlinear fees, and depth walking.
- The main missing components are event graph validation and non-atomic
  execution accounting.

### Why it is dangerous

Negative-risk events can have placeholders and augmented outcomes. Polymarket's
documentation says only named outcomes should be traded; placeholder outcomes
should be ignored until named.[3] A single missing outcome or misunderstood
“Other” bucket turns an apparent arbitrage into directional exposure.

### Verdict

**Build an offline verifier before any paper entries.** This is a high-value
mathematical lane, but it should remain shadow-only until membership, conversion,
fees, depth, and resolution are independently tested. It should not be mixed
into V6 weather results.

## 4. Sports: viable model family, difficult edge

Sports markets have frequent settlements and often deeper books. The model
families are familiar:

- Poisson/Skellam or bivariate goal models for soccer;
- Elo/Glicko plus home advantage;
- player availability and lineup adjustments;
- Bayesian state-space models for in-game win probability;
- market consensus as a strong prior.

For a binary sports outcome, the decision still reduces to:

```text
q_model - executable_ask - fee - uncertainty_buffer > 0
```

The important difference is competition. Sports prices are observed by
bookmakers, exchanges, syndicates, and automated traders. A model must include
lineup/news latency, market suspension, correlated outcomes, and the exact
settlement rule. The public Polymarket listing shows sports markets with large
visible volume and liquidity, but that is a capacity observation, not evidence
of exploitable edge.[10]

### Verdict

**Later and selective.** If researched, start with a single league and one
market type, compare against market-implied probabilities and bookmaker
consensus, and require walk-forward net edge after fees. Do not start with a
broad “all sports” model.

## 5. BTC and crypto short-duration markets: attractive volume, hardest execution

### Structure

A short-duration BTC contract can be written as a threshold event:

```text
q = P(S_T >= K | order book, spot/perpetual prices, volatility, time remaining)
EV = q - executable_cost - fee
```

A usable model needs:

- the exact settlement oracle and timestamp;
- the precise price index and timezone;
- spot/perpetual basis and funding;
- realized and implied volatility;
- order-book imbalance and trade-flow features;
- latency from source tick to CLOB reaction;
- queue position and partial-fill modelling.

Polymarket provides public real-time market streams with book updates, price
changes, last-trade data, timestamps, best bid/ask, and market lifecycle events,
which is technically sufficient for a read-only microstructure tape.[2]
Orders still have tick-size and minimum-size constraints and may be live,
matched, or delayed, so a displayed edge is not necessarily an executable
edge.[5]

### Why it is not our first choice

- Short horizons make latency and stale-quote risk dominant.
- BTC markets are likely to be heavily automated and rapidly arbitraged.
- Fees are category-specific and can be large relative to tiny directional
  edges.[1]
- A forecast can be correct while the trade loses through spread, slippage,
  delay, or oracle mismatch.
- A model trained on historical exchange prices can accidentally use a price
  feed that differs from the contract's settlement oracle.

### Verdict

**Shadow only.** First collect synchronized oracle/spot/CLOB data and measure
whether any signal survives latency and executable depth. Do not generalize the
weather model's multi-day forecast assumptions to five-minute BTC markets.

## 6. Counts, mentions, and culture markets

Some markets have deterministic count structures, such as social-post counts,
streaming/chart thresholds, or event totals. A count model could use Poisson or
negative-binomial distributions with time-varying intensity:

```text
P(N_T in bucket) = sum of count probabilities over the resolver bucket
```

These markets can have a clean mathematical payoff, but the data source and
rules are often fragile: deleted posts, API changes, timezone windows, bot
activity, and post hoc rule interpretation. They are not attractive until a
stable public source and historical point-in-time tape exist.

**Verdict:** research only after the resolver source and data retention are
verified. Do not infer an edge from an apparent count discrepancy.

## Methods we should not prioritize

- **Long-horizon politics/elections:** large liquidity but strong information
  aggregation, long capital lockup, and resolution/rule risk.
- **Headline/news sentiment:** difficult to timestamp, easy to leak, and hard to
  separate from the price reaction.
- **Unverified cross-platform “arbitrage”:** price similarity is insufficient;
  the two venues can resolve the same-looking contract differently.
- **Large neural networks without a baseline:** a more complex predictor does
  not beat market, climatology, persistence, or a calibrated simpler model by
  default.

## Recommended build order

1. **Cross-platform rule matcher and shadow scanner.** No orders; emit only
   verified equivalent events and fee/depth-adjusted spreads.
2. **Macro paper lane.** Capture Cleveland Fed nowcasts, release vintages,
   market books, and final releases; start with monthly CPI/PCE buckets.
3. **Negative-risk verifier.** Validate event membership, named outcomes,
   conversion relationships, complete-set cost, and partial-fill risk.
4. **V7 weather campaign.** Let the new 7Timer fallback reduce provider-health
   suppression, then compare provider count, calibration, rejection reasons, and
   settled P&L against V6.
5. **BTC shadow tape.** Only after real-time data, oracle mapping, latency, and
   fee/depth replay are available.
6. **Sports single-league spike.** Only after choosing one sport, one market
   type, and one historical data source.

## Promotion gate for any new lane

A new lane must have:

- exact resolution rules and a versioned resolver/source registry;
- point-in-time inputs with no future revisions in the training window;
- a simple baseline and a market-price baseline;
- calibrated probabilities or a mechanically verified payoff inequality;
- executable ask/bid depth, fees, tick size, and minimum order modelling;
- correlated-event and capital-lockup accounting;
- a fresh isolated paper campaign;
- enough resolved independent observations to estimate uncertainty;
- no live/account side effects.

This makes the next experiment measurable instead of merely adding more market
symbols to the scanner.

## Sources

[1] https://docs.polymarket.com/trading/fees
[2] https://docs.polymarket.com/market-data/realtime-data
[3] https://docs.polymarket.com/concepts/negative-risk
[4] https://docs.polymarket.com/concepts/resolution
[5] https://docs.polymarket.com/trading/place-orders
[6] https://www.federalreserve.gov/econres/feds/files/2026010pap.pdf
[7] https://www.clevelandfed.org/indicators-and-data/inflation-nowcasting
[8] https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6905683
[9] https://github.com/ImMike/polymarket-arbitrage
[10] https://polymarket.com/predictions
