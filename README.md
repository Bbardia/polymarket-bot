# Polymarket Trading Bot — V3 Foundation

Paper-first Polymarket research and trading infrastructure for a small,
risk-capped account. The April-era execution loop is retained only for offline
research compatibility; its live path is permanently disabled.

> Experimental software, not financial advice. No strategy is enabled for live
> trading in this foundation.

## Current safety state

- Uses official `polymarket-client==0.6.0` models and pUSD assumptions.
- No V3 service/worker command exists yet.
- Authenticated account reads and live-capable client construction are lazy and
  use separate gates; reconciliation can run while paper mode remains enabled.
- Legacy `run_full_loop.py --live` exits before constructing a client.
- Repository watchdog is status-only and cannot launch the bot.
- No Polymarket Hermes/Claude cron is required or configured.
- Manual positions can be marked observe-only through the ignored local `.env`;
  reconciliation never sells, cancels, merges, or redeems them.

## V3 architecture

```text
src/v3/
├── api.py              # Official unified SDK adapter; lazy secure client
├── config.py           # Strict live gate and account limits
├── execution.py        # Risk-gated post-only GTD submission primitive
├── ledger.py           # Append-only SQLite event ledger
├── market_context.py   # Live ticks, minimums, fees, state, resolution rules
├── math.py             # Decimal fee, VWAP, uncertainty, Kelly, complete sets
├── orders.py           # Fill-aware/idempotent order aggregate
├── reconciliation.py   # Read-only local-vs-remote comparison
├── risk.py             # Capital, reserve, event, loss, drawdown limits
├── weather.py          # Forecast batching/backoff/calibration metrics
└── strategies/
    ├── weather.py      # Paper-only weather evaluator
    └── complete_set.py # Paper-only executable two-book evaluator
```

### Order lifecycle invariant

An accepted response with `live`, `matched`, or `delayed` status is an order,
not inventory. Position quantity and cost change only after a unique trade event
reaches `CONFIRMED`. Duplicate events are idempotent.

### Capital invariant

```text
capital_base = min(real_account_equity, configured_max_capital)
deployable   = capital_base × (1 - reserve_fraction)
```

Every order is then capped by order notional, event exposure, total deployed
capital, fresh-quote age, short GTD lifetime, daily loss, drawdown, open orders,
and open positions.

### Current fee math

For `C` shares at price `p` in a category with fee coefficient `r`:

```text
fee = C × r × p × (1 - p)
```

Complete-set research walks both ask books, computes full executable VWAP, sums
this nonlinear fee at every consumed depth level, and requires positive net profit
after all-in entry cost. It never submits either leg.

### Weather uncertainty

V3 does not treat 143 correlated ensemble members as 143 independent samples.
It uses the cluster design effect:

```text
n_eff = n / (1 + (n - 1) × rho)
```

The decision threshold is the maximum of the base edge, probability standard
error, half-spread, lead-time penalty, and tail penalty. Kelly uses fee-adjusted
all-in price and a fractional multiplier.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt
cp .env.template .env
chmod 600 .env
```

## Non-running inspection commands

```bash
python run_v3.py validate-config
python run_v3.py architecture
scripts/pm-status.sh
```

These commands do not initialize an authenticated client or call market/account
APIs.

## Tests

```bash
python -m pytest tests -q
python -m py_compile src/v3/*.py src/v3/strategies/*.py run_v3.py
```

V3 tests cover fee curves, depth-aware VWAP, complete-set net edge, correlated
forecast uncertainty, fee-adjusted Kelly, dynamic ticks, fill-only accounting,
idempotency, capital limits, loss/drawdown breakers, SQLite persistence,
external-position protection, unified-SDK gating, reconciliation mapping,
forecast batching/backoff, and non-running entrypoint safety.

## Live certification still required

Before any live command is added, V3 still needs:

1. authenticated user-stream ingestion and reconnect recovery;
2. account reconciliation against a real read-only snapshot;
3. queue-aware maker paper fills and point-in-time replay;
4. at least 30 days of shadow data and 100 resolved independent candidates;
5. explicit review of canary limits and manual-position acknowledgements.

No LLM or cron job belongs in the execution loop. A future Hermes job may produce
a read-only daily research summary after reliable paper data exists; it must not
place orders or restart workers.
