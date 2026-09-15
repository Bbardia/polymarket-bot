# Weather replay: executable market baseline

**Status:** prepared; no V7 behavior changed
**Owner:** paper research lane
**Created:** 2026-09-15

## Question

Does a settlement-matched forecast add net information over Polymarket's own
market distribution after executable depth, fees, slippage, and clustered
uncertainty are included?

## Data source

- Repository: https://github.com/marketlenstrade/polymarket-historical-data
- Sample: `nyc-weather-2026-07-16/`
- Scope: 33 resolved NYC daily-temperature markets across July 15–18, 2026
- Advertised order-book/trade rows: 319,831
- License: CC-BY-4.0; preserve attribution if data is retained
- Local probe: metadata CSV (14,320 bytes) and one compact Parquet file (438,119 bytes)
  downloaded to `/tmp/polymarket-weather-replay/`; no repository data was modified.

## Required comparisons

1. market-implied bucket distribution;
2. raw V7 multi-provider distribution;
3. station-corrected distribution, only if trained strictly before each decision;
4. climatology/persistence baseline.

## Execution model

- replay chronological book snapshots and trades;
- use ask-side VWAP for hypothetical entries;
- use bid-side VWAP for marks/exits;
- apply the market-specific Gamma fee schedule;
- model partial fills, missed maker fills, and non-atomic legs explicitly;
- cluster uncertainty by settlement day, not by bucket;
- preserve unresolved and unfillable rows rather than dropping them.

## Promotion gates

- resolver station, timezone, date, units, and bucket boundaries verified;
- zero temporal leakage and zero duplicate market identities;
- at least 20 independent settlement days before strategy interpretation;
- model must beat market, climatology, and persistence on held-out dates;
- positive net executable EV with a clustered lower confidence bound above zero;
- no single city/day or provider supplies the result;
- paper-only shadow lane remains separate from V7 control and historical ledgers.

## Explicit non-go decisions

- Do not copy external bot code.
- Do not enable live orders or authenticated access.
- Do not loosen V7 thresholds or caps based on this sample.
- Do not treat the 33-market sample as sufficient evidence of profitability.
