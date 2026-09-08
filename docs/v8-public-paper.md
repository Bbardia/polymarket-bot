# V8 public paper campaign

V8 is an isolated, public-data-only paper worker. It does not create accounts,
load wallet credentials, read balances, submit orders, or use authenticated
clients. V6 remains on its own branch, data directory, and service.

## Lanes

### Fed

- Discovers active Polymarket macro markets through the public Gamma API.
- Reads public Kalshi `KXFEDDECISION` markets without authentication.
- Converts per-meeting midpoint probabilities for 0/25/>25 bp cuts into a
  discrete distribution for annual cut-count markets.
- For “cut by month” markets, truncates the distribution at the named month.
- Uses executable Polymarket asks and the market fee schedule.

This is a market-relative baseline, not a claim to have CME FedWatch API access.
CME FedWatch is paid/API-gated; Kalshi public data is used only where its public
read endpoint is available.

### CPI

- Reads the public BLS `CUUR0000SA0` CPI-U unadjusted series.
- Computes released year-over-year CPI observations.
- If a threshold is already exceeded in the current year's released data, the
  probability is deterministic for that exact US-style market definition.
- Otherwise estimates a conservative probability that remaining months cross
  the threshold using latest YoY, bounded trend, and historical volatility.
- Foreign-country inflation markets and standalone PCE markets are observation-
  only; US CPI data is never reused for them.

### Sports

- Restricted to sports-tagged match-result markets with win/draw outcomes.
- Rejects spreads, halftime, second-half, first-team-to-score, exact-score,
  totals, and stale matches.
- Uses public ClubElo page data for supported teams.
- Converts ClubElo Golo values into a Poisson score grid and derives home/draw/
  away probabilities.
- Unsupported leagues or teams are observation-only.

## Execution and accounting

- Public Gamma market discovery and public CLOB order books only.
- Buys are simulated at executable ask depth, not midpoint or last trade.
- Nonlinear fee schedule is included in all-in cost and edge.
- Minimum share quantity, cash reserve, global position cap, and per-lane cap
  apply before a paper entry.
- Positions hold to authoritative public resolution; no early exits are enabled.
- State is atomically persisted; candidate, trade, settlement, scan, and status
  records are append-only/local.

## Campaign interpretation

The first valid campaign is `/home/rasbardi/polymarket-bot-v7/data/v8-paper-20260908-clean`.
The earlier prelaunch smoke directories are retained separately and must not be
included in performance totals because they contained invalid model coverage
that was caught and corrected before launch.

a paper entry demonstrates plumbing and a hypothesis, not profit. Review V8 using
settled outcomes, Brier/log loss, calibration, executable marks, fees, depth,
and lane-specific rejection reasons.

## Public sources

- Polymarket Gamma/CLOB: https://gamma-api.polymarket.com/ and https://clob.polymarket.com/
- Kalshi public market reads: https://api.elections.kalshi.com/trade-api/v2/markets
- BLS public API: https://api.bls.gov/publicAPI/v2/timeseries/data/CUUR0000SA0
- ClubElo public ratings: https://clubelo.com/USA, https://clubelo.com/CHN, https://clubelo.com/COL
- Federal Reserve resolution reference: https://www.federalreserve.gov/monetarypolicy/openmarket.htm
