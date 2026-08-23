from decimal import Decimal

from src.v3.risk import AccountRiskState, OrderIntent, RiskEngine, RiskLimits


def D(value: str) -> Decimal:
    return Decimal(value)


def limits() -> RiskLimits:
    return RiskLimits(
        max_capital=D("50"),
        reserve_fraction=D("0.25"),
        max_order_notional=D("2"),
        max_event_exposure=D("5"),
        max_open_orders=4,
        max_positions=5,
        daily_loss_limit=D("3"),
        max_drawdown_fraction=D("0.10"),
        max_quote_age_seconds=120,
        max_order_ttl_seconds=300,
    )


def state(**overrides) -> AccountRiskState:
    values = dict(
        equity=D("230"),
        cash=D("174"),
        total_exposure=D("0"),
        event_exposure={},
        open_orders=0,
        open_positions=0,
        daily_pnl=D("0"),
        peak_equity=D("230"),
        reconciled=True,
        unknown_remote_positions=0,
        unknown_remote_orders=0,
    )
    values.update(overrides)
    return AccountRiskState(**values)


def intent(**overrides) -> OrderIntent:
    values = dict(
        condition_id="condition",
        token_id="token",
        side="BUY",
        price=D("0.20"),
        shares=D("5"),
        estimated_fee=D("0"),
        post_only=True,
        ttl_seconds=180,
        quote_age_seconds=1,
    )
    values.update(overrides)
    return OrderIntent(**values)


def test_configured_cap_and_reserve_limit_real_cash_not_account_balance():
    result = RiskEngine(limits()).evaluate(intent(shares=D("10")), state(total_exposure=D("36")))
    assert not result.allowed
    assert "deployable capital" in result.reason
    assert result.capital_base == D("50")
    assert result.deployable_capital == D("37.50")


def test_order_cap_event_cap_and_reconciliation_are_hard_blocks():
    engine = RiskEngine(limits())
    assert not engine.evaluate(intent(shares=D("11")), state()).allowed
    assert not engine.evaluate(intent(), state(event_exposure={"condition": D("4.50")})).allowed
    assert not engine.evaluate(intent(), state(reconciled=False)).allowed
    assert not engine.evaluate(intent(), state(unknown_remote_positions=1)).allowed
    assert not engine.evaluate(intent(), state(unknown_remote_orders=1)).allowed


def test_initial_live_entries_must_be_post_only_fresh_short_lived_and_market_valid():
    engine = RiskEngine(limits())
    assert not engine.evaluate(intent(post_only=False), state()).allowed
    assert not engine.evaluate(intent(quote_age_seconds=121), state()).allowed
    assert not engine.evaluate(intent(ttl_seconds=301), state()).allowed
    assert not engine.evaluate(intent(price=D("0.413"), tick_size=D("0.0025")), state()).allowed
    assert not engine.evaluate(intent(shares=D("4"), min_order_size=D("5")), state()).allowed
    assert not engine.evaluate(intent(market_accepting_orders=False), state()).allowed
    assert not engine.evaluate(intent(rules_verified=False), state()).allowed
    assert not engine.evaluate(intent(disputed=True), state()).allowed
    assert engine.evaluate(intent(), state()).allowed


def test_loss_and_drawdown_breakers_block_new_risk():
    engine = RiskEngine(limits())
    assert not engine.evaluate(intent(), state(daily_pnl=D("-3"))).allowed
    assert not engine.evaluate(intent(), state(equity=D("206"), peak_equity=D("230"))).allowed
