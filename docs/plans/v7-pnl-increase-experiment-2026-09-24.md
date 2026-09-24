# V7 Net P&L Evaluation and Safe Experiment Plan

> **For Hermes:** Execute in the isolated `experiment/v7-pnl-uplift-20260924` worktree; do not alter or restart the main V7 or current mirror workers.

**Goal:** Improve the evidence and experiment loop behind V7 daily net P&L without increasing live or paper risk on the canonical control.

**Architecture:** Reuse V7's existing append-only point-in-time `forecast_snapshots.jsonl` and candidate/settlement logs rather than adding a duplicate tape. Add an offline, read-only report that joins finalized settlement rows to the exact decision-time trade snapshots, compares selected-side model/raw/entry-ask/base-rate Brier, reports cap/rejection pressure, and evaluates only those sizing/exit counterfactuals supported by the data. Keep all outputs create-only and outside campaign ledgers. Any strategy variant remains shadow-only until sufficient independent, chronological labels and executable-depth evidence exist.

**Tech Stack:** Python standard library, `Decimal`, pytest, existing V7 JSONL formats and fee/VWAP math.

---

## Safety and non-goals

- Preserve main control commit `a69d985`, its profile, data directory, service and ledger.
- Preserve the active mirror worker/data and treat its existing campaign as evidence, not a clean new treatment.
- Never enable live trading, account reads, authenticated clients, signed orders or order submission.
- Do not change canonical thresholds, caps, breakers, position sizing, or exit policy.
- Do not launch another public-data worker in this phase: main Open-Meteo quota was observed at 90/96 requests/24h. Offline work only until quota and provider budget are checked again.
- Do not run a maker/rebate treatment now; keep it deferred/shadow-only because actual fills, queue priority, inventory and rebates are unverified.
- Never treat repeated forecast snapshots/candidate rows from one condition/date as independent outcomes.
- Forecast snapshots are immutable; finalized truth belongs in a separate report/join artifact.

## Task 1: Establish the existing tape and label join

**Files:** `src/v3/pnl_eval.py`, `tests/v3/test_pnl_eval.py`, `scripts/v7_pnl_report.py`.

- Read existing forecast snapshots, candidate decisions, trades, exits and authoritative settlement rows without changing them.
- Join an executed trade to the exact decision-time snapshot by `forecast_snapshot_id` when the trade audit carries it. The current V7 trade rows do not carry that ID, so use only the exact legacy triple (condition ID, selected side, decision scan timestamp), label that join method, reject duplicate keys, and fail on ID/market mismatches.
- Require decision time earlier than settlement time, align YES/NO probabilities and selected-side outcomes, reject duplicate or ambiguous joins, and keep output fields including sample counts and exclusions.
- Build a create-only JSON report and a human-readable CLI summary; do not mutate original JSONL or publish labels back into snapshots.

**Acceptance:** Synthetic tests for YES/NO wins and losses, repeated scans, duplicate rows, missing/non-final outcomes, time ordering, and fee/bid-versus-ask semantics; run focused and full suite.

## Task 2: Compare model, market, and fixed-probability baseline forecasts

- For the exact eight matured main trades available, compute calibrated-selected-side, raw-selected-side, executable entry-ask and fixed-50% reference Brier scores, plus score deltas and sample counts.
- Also report settlement-count/side concentration for all executed directional trades and the settled score panel, unresolved/open exposure, and explicit `insufficient_data` promotion status.
- Calculate a Brier proper-betting position vector only as a shadow diagnostic. Report its hypothetical stake/edge and execution-data limitations; do not turn it into a position or claim causal profitability from the selected-trade sample.
- For untraded candidates, do not infer win/loss labels from repeated scans. If no finalized truth exists, mark threshold-policy evaluation as blocked rather than fabricate a counterfactual.

**Acceptance:** Decimal-based tests against hand-computed fixtures; summary stays explicitly non-promotable at current sample size.

## Task 3: Rejection and capital-turnover diagnostics

- Programmatically aggregate repeated candidate rejection rows into categories: no bid/stale or thin book, resolver/station/provider, signal/uncertainty/price, cash/exposure/position cap, and drawdown/loss breaker.
- Report both raw scan-row counts and unique condition/event counts so repeated scans cannot masquerade as independent opportunities.
- For the mirror, inspect the five current arms separately, detect cap-overridden entries, and refuse to declare an exit winner when the matched cohorts differ or lack resolved outcomes. Require nonempty source-audit, condition, market, side, event, strategy and scan-time identity fields; require final settlement to match condition + market + strategy + public Gamma source and occur after entry. Preserve current arm data.

**Acceptance:** Fixtures with duplicate scans and cap overrides; report has explicit `comparison_status` and blockers.

## Task 4: Keep exit and entry variants shadow-only

- Reuse the running mirror's existing 10/15/25%, hybrid and hold arms as preliminary history; do not restart or reset them.
- Add only a deterministic offline common-cohort/cap-comparability diagnostic if authoritative arm trade rows allow exact pairing. Otherwise report that the current mirror is not cap-comparable and list the additional data needed.
- Specify the future matched test using identical entries, fee/depth-aware bids, unchanged position/exposure caps, hold fallback settlement, and P&L per deployed dollar-day. No target or edge threshold changes in this implementation.

**Acceptance:** Tests reject cross-arm cohort mismatch, cap overrides, ambiguous settlement and duplicate event dates; no fake independent sample counts.

## Task 5: Verify, review and report

- Run focused tests, full suite, `git diff --check`, network-free paper-safety config validation, and a full diff review.
- Get an independent read-only review of the completed branch and reconcile it against actual tests/data.
- Do not start a new paper worker while public-provider headroom is uncertain. If later safe to start, require fresh ignored profile/data paths, entries-disabled smoke first, and full profile/status/safety round-trip before any isolated paper entries.
- Commit signed only if the implementation is correct and tested. Push only this isolated branch after review, not to `main`; do not merge it or claim increased profitability.

**Final decision gate:** Strategy promotion is evidence-blocked until there are enough independent, finalized weather outcomes, a chronological holdout that beats the executable market ask and simple baselines after fees/depth, and a cap-feasible matched exit comparison without unacceptable drawdown.
