# Funded V7: read-only position and P&L audit (2026-10-14)

**Scope:** Local event-ledger/status replay and exact public Gamma market-resolution lookups. No authenticated wallet read, chain receipt verification, orders, redemption, runtime change, or live deployment. A consistent SQLite online backup was used in memory; the status/state JSON was sampled separately. This is a point-in-time diagnostic, not an account statement or strategy validation.

| Local bot accounting (USDC) | Amount |
|---|---:|
| Baseline cash (2026-10-03) | 237.128775 |
| Realized early exits (8 confirmed SELL fill legs) | +1.114500 |
| Recorded redemptions (3 winning tokens, payout net of entry basis) | +12.073600 |
| Total locally realized P&L | +13.188100 |
| Remaining 18 managed positions, entry cost | 18.109669 |
| Publicly resolved losing outcome value of those 18 positions | 0.000000 |
| Bot result including remaining losing positions | **−4.921569** |
| Reported cash/equity | 232.207206 |

The 18 tokens were matched to exact condition/token identifiers in public Gamma records marked closed and resolved with one-hot outcomes; each held side lost. Their basis remains in local inventory, not booked as an exit or redemption loss. Cash accounting reconciles at six decimals: baseline cash plus locally realized P&L equals reported cash plus remaining basis. Confirmed maker fills were deduplicated by stable trade ID, associated with accepted managed orders, and replayed in match-time order with allocated SELL cost basis. An independent read-only reviewer reproduced the arithmetic and public resolution classification.

**Operational snapshot:** funded service running; completed cycles advancing; 0 active bot orders; 0 reported cash, position, or order mismatches; entry blocked by maximum drawdown (12.221040 against the unchanged 10 USDC limit). These are *worker-reported* reconciliation figures. The bot tracks 126 external positions separately; this is **not** whole-wallet P&L. Public outcomes and recorded redemption hashes do not independently establish current wallet balances, exact holdings, on-chain burns, or absence of other cash flows. Do not clear the breaker or promote paper research from this audit.

**Next evidence gate:** If separately authorized, obtain bounded authenticated read-only cash/position/order/trade history and independently verify redemption receipts and exact token holdings; reconcile that snapshot to local replay before any funded strategy or accounting change. For strategy diagnosis, group the confirmed fills and final outcomes by event day, side, bucket price, station/rule and decision-time forecast, explicitly recording missing point-in-time inputs and no-bid exits. Run isolated paper comparisons with finalized labels and executable-depth/fee/latency assumptions; no automatic live activation.
