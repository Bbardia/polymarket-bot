# Polymarket Trading Bot

Research-oriented Polymarket trading bot for weather and short-horizon prediction markets. The code is designed to run in **dry-run mode by default** and requires an explicit live-trading opt-in before it can place real orders.

> **Warning**: This is experimental software, not financial advice. Prediction-market trading can lose money. Review the code, start in dry-run mode, and use your own risk limits.

## Safety model

Live trading is fail-closed:

- `.env` is gitignored and must stay local.
- `.env.template` contains placeholders only.
- Running with `--live` is not enough by itself.
- Real-money trading also requires `ENABLE_LIVE_TRADING=true` and `PAPER_TRADING=false` in `.env`.
- Runtime state and logs are ignored: `data/`, `logs/`, `venv/`, `.pytest_cache/`, `.claude/`.

## Project layout

```text
polymarket-bot/
├── run_full_loop.py              # Main dry-run/live loop
├── src/
│   ├── config.py                 # Env loading and live-trading guardrails
│   ├── polymarket_client.py      # Polymarket Gamma/CLOB integration
│   ├── forecast_scanner.py       # Weather forecast-vs-price scanner
│   ├── weather_forecast.py       # Open-Meteo ensemble forecasts
│   ├── edge_math.py              # Probability shrinkage, uncertainty, Kelly helpers
│   ├── kelly.py                  # Kelly sizing and adaptive risk controls
│   ├── portfolio.py              # Local position/P&L tracking
│   ├── orderbook_utils.py        # Orderbook, spread, slippage helpers
│   ├── btc_sniper.py             # BTC short-horizon signal logic
│   ├── btc_straddle.py           # BTC straddle experiments
│   └── whale_tracker.py          # Public-wallet signal scanner
├── scripts/
│   ├── pm-status.sh              # Local status helper
│   ├── watchdog.sh               # Opt-in local watchdog helper
│   └── daily_review.sh           # Dry-run local review helper
├── tests/                        # Unit tests for math/risk logic
└── .env.template                 # Safe local configuration template
```

## Quick start

```bash
git clone <repo-url> polymarket-bot
cd polymarket-bot

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.template .env
# Edit .env locally. Keep PAPER_TRADING=true while testing.

# Run tests
python -m pytest tests -q

# Start in dry-run mode; this does not place orders.
python run_full_loop.py --budget 25
```

## Configuration

Key local `.env` variables:

```dotenv
ENABLE_LIVE_TRADING=false
PAPER_TRADING=true
POLY_PRIVATE_KEY=replace_me_with_local_private_key
POLY_FUNDER_ADDRESS=replace_me_with_local_funder_address
POLY_SIGNATURE_TYPE=1
MAX_CAPITAL=25
MAX_POSITION_SIZE=2
EDGE_THRESHOLD=0.15
LOG_LEVEL=INFO
```

For live trading, set credentials locally and change both safety flags:

```dotenv
ENABLE_LIVE_TRADING=true
PAPER_TRADING=false
```

Then run with `--live` only after reviewing risk limits:

```bash
python run_full_loop.py --live --budget 25
```

## Public-repo hygiene

Before pushing changes publicly, run:

```bash
git status --short
git ls-files | grep -E '(^\.env$|^\.env\.|^data/|^logs/|^venv/|^\.claude/|__pycache__|\.pyc$)' || true
git grep -n -I -E '(PRIVATE_KEY|API_KEY|SECRET|PASSWORD|TOKEN|MNEMONIC|Bearer)' -- . ':!*.md' || true
python -m pytest tests -q
```

Expected sensitive-file result: only `.env.template` may appear, and it must contain placeholders only.

## Notes

- Public APIs used by the project include Polymarket Gamma/CLOB endpoints and Open-Meteo ensemble forecasts.
- Builder API credentials are optional and should never be committed.
- The repository intentionally excludes local trade history and logs, because those can contain private wallet/activity information.
