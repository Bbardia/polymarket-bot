# V7 weather ladder experiment

The ladder is an opt-in V7 paper strategy. It is disabled by default and does
not change the normal one-bucket weather lane.

## Payoff model

For equal shares `s` in adjacent mutually exclusive buckets `i`:

```text
cost = sum_i(executable_VWAP_i * s + fee_i)
cluster_probability = sum_i(model_probability_i)
expected_profit = s * cluster_probability - cost
profit_if_selected_bucket_wins = s - cost
loss_if_outside_cluster = -cost
```

The strategy enters only when all required controls pass:

- exactly 3 or 4 adjacent exact buckets;
- verified weather station/unit and negative-risk weather event context;
- executable ask depth for every leg;
- fee-adjusted expected profit above the configured threshold;
- cluster probability above the configured threshold;
- one selected-bucket payout covers the complete basket cost;
- basket cost below the configured cap;
- paper cash, position, and loss/drawdown controls.

This is a directional forecast basket, not risk-free arbitrage. If the resolved
weather falls outside the selected cluster, every leg can lose.

## Sizing and state

The first implementation uses equal shares across legs, with the common share
quantity set to the largest venue minimum. It persists every leg's market ID,
token ID, share count, ask VWAP, fee, and all-in cost. Basket positions hold to
resolution. Settlement waits until every leg is publicly resolved, then pays the
winning YES leg(s) and reconciles the whole basket in one settlement record.

A ladder entry reserves the event key and prevents a simultaneous directional
one-bucket entry for the same weather event.

## Controls

```text
V3_PAPER_WEATHER_LADDER_ENABLED=false
V3_PAPER_WEATHER_LADDER_WIDTH=3
V3_PAPER_WEATHER_LADDER_MIN_EXPECTED_PROFIT=0.02
V3_PAPER_WEATHER_LADDER_MIN_CLUSTER_PROBABILITY=0.60
V3_PAPER_WEATHER_LADDER_MAX_BASKET_COST=5
```

The article's underdispersion idea is not an automatic sizing multiplier here.
It must first be measured against immutable V7 forecast snapshots and finalized
station outcomes; shared model bias can make a tightly clustered ensemble
confidently wrong.

## Verification

- Pure ladder mathematics tests cover positive EV, outside-cluster loss, fees,
  depth, and adjacency rejection.
- Integration tests create one three-leg basket and verify directional duplicate
  suppression.
- Full V7 suite passes with the ladder changes.
- A real public-data smoke with entries disabled completed healthy with 30 weather
  markets scanned, 14 directional candidates, zero errors, no positions, and no
  ladder candidate because the sampled live events did not contain a verified
  adjacent three-bucket window.
