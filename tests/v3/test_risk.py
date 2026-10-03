from decimal import Decimal

from src.v3.risk import AccountRiskState, OrderIntent, RiskEngine, RiskLimits


def D(value: str) -> Decimal:
    return Decimal(value)


def limits() -> RiskLimits:
    return RiskLimits(
        max_capital=D("100"),
        reserve_fraction=D("0.25"),
        max_order_notional=D("2"),
        max_event_exposure=D("5"),
        max_open_orders=4,
        max_positions=5,
        daily_loss_limit=D("10"),
        max_drawdown_amount=D("10"),
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
        rules_verified=True,
        market_accepting_orders=True,
    )
    values.update(overrides)
    return OrderIntent(**values)


def test_configured_cap_and_reserve_limit_real_cash_not_account_balance():
    result = RiskEngine(limits()).evaluate(intent(shares=D("10")), state(total_exposure=D("74")))
    assert not result.allowed
    assert "deployable capital" in result.reason
    assert result.capital_base == D("100")
    assert result.deployable_capital == D("75.00")


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
    assert not engine.evaluate(intent(), state(daily_pnl=D("-10"))).allowed
    assert not engine.evaluate(intent(), state(equity=D("220"), peak_equity=D("230"))).allowed


def test_legacy_fraction_can_only_tighten_absolute_drawdown():
    limits_with_fraction = RiskLimits(**{**limits().__dict__, "max_drawdown_fraction": D("0.02")})
    engine = RiskEngine(limits_with_fraction)
    assert not engine.evaluate(intent(), state(equity=D("225"), peak_equity=D("230"))).allowed
    assert engine.evaluate(intent(), state(equity=D("226"), peak_equity=D("230"))).allowed


def test_market_rules_default_to_unverified_and_fail_closed():
    unverified = OrderIntent(
        condition_id="condition", token_id="token", side="BUY", price=D("0.20"),
        shares=D("5"), estimated_fee=D("0"), post_only=True,
        ttl_seconds=180, quote_age_seconds=1,
    )
    assert not RiskEngine(limits()).evaluate(unverified, state()).allowed


def test_risk_limits_reject_nonfinite_values():
    import pytest

    with pytest.raises(ValueError, match="finite"):
        RiskLimits(**{**limits().__dict__, "max_capital": D("NaN")})


def test_nonfinite_order_values_are_rejected():
    engine = RiskEngine(limits())
    for bad_intent in (
        intent(price=D("NaN")),
        intent(shares=D("Infinity")),
        intent(estimated_fee=D("NaN")),
        intent(tick_size=D("NaN")),
        intent(min_order_size=D("NaN")),
    ):
        assert not engine.evaluate(bad_intent, state()).allowed


def test_wrong_runtime_types_in_risk_state_fail_closed():
    engine = RiskEngine(limits())
    assert not engine.evaluate(intent(), state(equity="230")).allowed
    assert not engine.evaluate(intent(), state(event_exposure={"condition": "0"})).allowed
    assert not engine.evaluate(intent(), state(open_orders="0")).allowed
