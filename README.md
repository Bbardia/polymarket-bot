# Polymarket Trading Bot — V3 Foundation

Paper-first Polymarket research and trading infrastructure for a small,
risk-capped account. The April-era execution loop is retained only for offline
research compatibility; its live path is permanently disabled.

> Experimental software, not financial advice. No strategy is enabled for live
> trading in this foundation.

## Current safety state

- Uses official `polymarket-client==0.6.0` models and pUSD assumptions.
- `paper-run` is a public-data-only worker with durable local scans, candidates,
  simulated positions, and settlement records. It refuses to run if account
  reads or live trading are enabled.
- V4 weather paper research combines Open-Meteo, MET Norway, and NWS hourly
  forecasts when coverage exists. A provider outage is recorded as degraded
  telemetry; remaining sources continue with wider uncertainty. Resolved paper
  outcomes update a persistent, conservative per-source/city/horizon calibrator.
- Authenticated account reads and live-capable client construction are lazy and
  use separate gates; reconciliation can run while paper mode remains enabled.
- User/market stream events can be normalized, durably replayed, deduplicated,
  and supervised with bounded reconnect backoff; any disconnect or stream end
  sets a sticky reconciliation blocker. No stream worker command exists and no
  authenticated subscription starts automatically. Unknown order IDs are
  observe-only reconciliation blockers and are never adopted as bot orders
  unless explicitly supplied as managed local IDs.
- Queue-aware maker replay and resolved-candidate shadow reports are offline,
  file-based inspection tools only.
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
├── maker_shadow.py     # Honest unsubmitted passive-quote diagnostics
├── math.py             # Decimal fee, VWAP, uncertainty, Kelly, complete sets
├── orders.py           # Fill-aware/idempotent order aggregate
├── paper.py            # Public-only continuous paper worker and state
├── paper_weather.py    # Resolver-aware weather paper research
├── reconciliation.py   # Read-only local-vs-remote comparison
├── risk.py             # Capital, reserve, event, loss, drawdown limits
├── simulation.py       # Queue-aware maker replay and shadow metrics
├── streaming.py        # Durable event normalization/replay/reconnect state
├── weather.py          # Forecast batching/backoff/calibration metrics
├── weather_surface.py  # Complete-partition indicative basket analysis
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

V3 first gives ECMWF, GFS, ICON, and GEM equal model-family weight so a model
with more ensemble members cannot dominate the probability. It then avoids
treating correlated members as independent by using the cluster design effect:

```text
n_eff = n / (1 + (n - 1) × rho)
```

The decision threshold is the maximum of the base edge, probability standard
error, half-spread, lead-time penalty, and tail penalty. Fractional Kelly is
recorded as diagnostic telemetry; paper entries deliberately use the CLOB
minimum executable size and a paper-only `5 pUSD` all-in cap. Live execution
remains separately blocked and unchanged.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt
cp .env.template .env
chmod 600 .env
```

## Paper and inspection commands

```bash
python run_v3.py validate-config
python run_v3.py architecture
python run_v3.py paper-status
python run_v3.py paper-run --cycles 1
python run_v3.py paper-run
python run_v3.py shadow-report <resolved-candidates.jsonl>
python run_v3.py replay-report <maker-events.jsonl>
scripts/pm-status.sh
```

`validate-config`, `architecture`, `paper-status`, `shadow-report`, and
`replay-report` are network-free. `paper-run` calls only public market and order
book APIs; it never initializes an authenticated client or calls account APIs.

The paper worker starts with the configured capital reserve applied and uses two
separately labeled strategies:

- **Complete sets:** broadens discovery past the most-liquid negative-risk
  markets, walks executable depth on YES+NO, and still requires positive return
  after fees. It never accepts a mathematically locked-in loss merely to create
  activity.
- **Directional weather:** discovers exact, range, and tail daily-high buckets
  from the public Weather tag and compares executable prices with the keyless
  Open-Meteo ECMWF/GFS/ICON/GEM ensemble. Resolution URLs must identify the
  modeled airport station. Same-day paper entries always fail closed unless a
  successful public NOAA observation exists for that exact station and local
  date. The paper-only lane uses a 3% base edge plus
  spread/uncertainty/lead-time guards, caps each simulated order at `5 pUSD`,
  and allows at most one bucket per city/date and five concurrent weather
  positions. Maker quotes remain explicitly unsubmitted. Mechanically complete
  city/date baskets are labeled unverified cross-market hypotheses until common
  event membership is proven; they are never candidates and never change state.

Runtime state is written under the configured ignored paper-data directory:

```text
status.json          current health and safety posture
state.json           paper cash, positions, pending audit outbox, aggregate counts
scans.jsonl          every evaluated public market/book snapshot
weather_scans.jsonl  forecast, observation, uncertainty, price, and edge telemetry
weather_events.jsonl partition, violation, maker-shadow, and hypothesis telemetry
weather_calibration.json persistent paper-only source/city/horizon probability calibration
candidates.jsonl     positive strategy candidates and cap decisions
paper_trades.jsonl   idempotent simulated-entry audit records
settlements.jsonl    idempotent public-resolution settlement audit records
```

`shadow-report` expects one resolved candidate per line. Decimal values should
be encoded as strings:

```json
{"candidate_id":"candidate-1","expected_probability":"0.70","entry_price":"0.60","outcome":1,"filled_size":"2","fees_paid":"0.01"}
```

`replay-report` consumes point-in-time events in file order. A conservative maker
fill occurs only when opposite-side prints at the exact quote price first consume
the modeled queue ahead. Each quote's `queue_ahead` must include all size ahead
of it, including earlier simulated quotes at the same level:

```json
{"event_type":"quote","quote_id":"quote-1","token_id":"token-1","side":"BUY","price":"0.40","size":"5","queue_ahead":"3"}
{"event_type":"trade","token_id":"token-1","side":"SELL","price":"0.40","size":"4"}
{"event_type":"cancel","quote_id":"quote-1"}
```

Runtime JSONL files belong under ignored local state such as `data/`; do not
commit account-derived events or candidate records.

## Tests

```bash
python -m pytest tests -q
python -m py_compile src/v3/*.py src/v3/strategies/*.py run_v3.py
```

V3 tests cover fee curves, depth-aware VWAP, complete-set net edge, correlated
forecast uncertainty, fee-adjusted Kelly, dynamic ticks, fill-only accounting,
idempotency, capital limits, loss/drawdown breakers, SQLite persistence,
external-position protection, unified-SDK gating, reconciliation mapping,
forecast batching/backoff, stream replay/reconnect behavior, queue-aware maker
fills, fee-aware shadow metrics, public-only paper-worker safety, durable paper
state, broadened discovery, weather parsing/forecast caching, directional
settlement/Brier metrics, and non-live entrypoint safety.

## Live certification still required

Before any live command is added, V3 still needs:

1. connect the authenticated user-stream processor to a separately reviewed,
   read-only worker and prove recovery against recorded SDK events;
2. reconcile against a real read-only account snapshot;
3. collect real point-in-time books/trades for the queue-aware replay model;
4. accumulate at least 30 days of shadow data and 100 resolved independent
   candidates;
5. explicitly review canary limits and manual-position acknowledgements.

No LLM or cron job belongs in the execution loop. A future Hermes job may produce
a read-only daily research summary after reliable paper data exists; it must not
place orders or restart workers.
