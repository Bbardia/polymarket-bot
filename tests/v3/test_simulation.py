import json
from decimal import Decimal

import pytest

from src.v3.simulation import (
    MakerQuote,
    PaperReplayEvent,
    QueueAwareMakerSimulator,
    ShadowCandidate,
    evaluate_shadow_candidates,
    load_replay_events,
    load_shadow_candidates,
    replay_maker_events,
)


def D(value: str) -> Decimal:
    return Decimal(value)


def test_queue_aware_maker_does_not_assume_touch_fill():
    simulator = QueueAwareMakerSimulator()
    quote = simulator.place(
        MakerQuote(token_id="token", side="BUY", price=D("0.40"), size=D("5"), queue_ahead=D("10"))
    )
    assert quote.remaining == D("5")
    assert simulator.on_trade(token_id="token", aggressor_side="SELL", price=D("0.40"), size=D("8")) == ()
    assert quote.queue_ahead == D("2")
    assert quote.remaining == D("5")

    fills = simulator.on_trade(token_id="token", aggressor_side="SELL", price=D("0.40"), size=D("4"))
    assert len(fills) == 1
    assert fills[0].size == D("2")
    assert quote.remaining == D("3")


def test_queue_aware_maker_requires_opposite_aggressor_and_exact_price():
    simulator = QueueAwareMakerSimulator()
    quote = simulator.place(
        MakerQuote(token_id="token", side="SELL", price=D("0.60"), size=D("5"), queue_ahead=D("0"))
    )
    assert simulator.on_trade(token_id="token", aggressor_side="SELL", price=D("0.60"), size=D("5")) == ()
    assert simulator.on_trade(token_id="token", aggressor_side="BUY", price=D("0.59"), size=D("5")) == ()
    fills = simulator.on_trade(token_id="token", aggressor_side="BUY", price=D("0.60"), size=D("5"))
    assert fills[0].size == D("5")
    assert quote.remaining == D("0")


def test_same_print_advances_each_quotes_modeled_queue_without_overfilling():
    simulator = QueueAwareMakerSimulator()
    first = simulator.place(
        MakerQuote("token", "BUY", D("0.40"), D("5"), D("3"), quote_id="q1")
    )
    second = simulator.place(
        MakerQuote("token", "BUY", D("0.40"), D("5"), D("8"), quote_id="q2")
    )
    fills = simulator.on_trade(
        token_id="token", aggressor_side="SELL", price=D("0.40"), size=D("10")
    )
    assert [fill.size for fill in fills] == [D("5"), D("2")]
    assert first.remaining == D("0")
    assert second.remaining == D("3")
    assert sum((fill.size for fill in fills), D("0")) <= D("10")


def test_shadow_evaluation_reports_only_observed_paper_results():
    result = evaluate_shadow_candidates([
        ShadowCandidate("a", expected_probability=D("0.70"), entry_price=D("0.60"), outcome=1, filled_size=D("2")),
        ShadowCandidate("b", expected_probability=D("0.60"), entry_price=D("0.65"), outcome=0, filled_size=D("0")),
    ])
    assert result.candidates == 2
    assert result.filled_candidates == 1
    assert result.brier_score == D("0.225")
    assert result.filled_brier_score == D("0.09")
    assert result.realized_pnl == D("0.80")
    assert result.unfilled_candidates == 1


def test_shadow_evaluation_subtracts_observed_fees():
    result = evaluate_shadow_candidates([
        ShadowCandidate(
            "a", expected_probability=D("0.70"), entry_price=D("0.60"),
            outcome=1, filled_size=D("2"), fees_paid=D("0.03"),
        ),
    ])
    assert result.realized_pnl == D("0.77")
    assert result.total_fees == D("0.03")


def test_point_in_time_replay_respects_queue_and_cancellation():
    result = replay_maker_events([
        PaperReplayEvent("quote", quote_id="q1", token_id="token", side="BUY", price=D("0.40"), size=D("5"), queue_ahead=D("3")),
        PaperReplayEvent("trade", token_id="token", side="SELL", price=D("0.40"), size=D("4")),
        PaperReplayEvent("cancel", quote_id="q1"),
        PaperReplayEvent("trade", token_id="token", side="SELL", price=D("0.40"), size=D("5")),
    ])
    assert result.quotes_placed == 1
    assert result.trades_seen == 2
    assert result.total_filled_size == D("1")
    assert len(result.fills) == 1


def test_replay_rejects_cancellation_after_quote_is_fully_filled():
    with pytest.raises(ValueError, match="unknown quote"):
        replay_maker_events([
            PaperReplayEvent("quote", quote_id="q1", token_id="token", side="BUY", price=D("0.40"), size=D("1"), queue_ahead=D("0")),
            PaperReplayEvent("trade", token_id="token", side="SELL", price=D("0.40"), size=D("1")),
            PaperReplayEvent("cancel", quote_id="q1"),
        ])


def test_jsonl_loaders_are_strict_and_decimal_safe(tmp_path):
    shadow_path = tmp_path / "shadow.jsonl"
    shadow_path.write_text(json.dumps({
        "candidate_id": "a", "expected_probability": "0.70", "entry_price": "0.60",
        "outcome": 1, "filled_size": "2", "fees_paid": "0.03",
    }) + "\n")
    replay_path = tmp_path / "replay.jsonl"
    replay_path.write_text("\n".join([
        json.dumps({"event_type": "quote", "quote_id": "q1", "token_id": "token", "side": "BUY", "price": "0.40", "size": "5", "queue_ahead": "3"}),
        json.dumps({"event_type": "trade", "token_id": "token", "side": "SELL", "price": "0.40", "size": "4"}),
    ]) + "\n")

    candidates = load_shadow_candidates(shadow_path)
    events = load_replay_events(replay_path)
    assert candidates[0].fees_paid == D("0.03")
    assert events[0].queue_ahead == D("3")
    assert replay_maker_events(events).total_filled_size == D("1")


def test_jsonl_loader_rejects_non_finite_decimal(tmp_path):
    path = tmp_path / "shadow.jsonl"
    path.write_text(json.dumps({
        "candidate_id": "a", "expected_probability": "NaN", "entry_price": "0.60",
        "outcome": 1, "filled_size": "2",
    }) + "\n")
    with pytest.raises(ValueError, match="finite"):
        load_shadow_candidates(path)


def test_shadow_loader_requires_economic_fields(tmp_path):
    path = tmp_path / "shadow.jsonl"
    path.write_text(json.dumps({
        "candidate_id": "a", "entry_price": "0.60", "outcome": 1,
    }) + "\n")
    with pytest.raises(ValueError, match="expected_probability"):
        load_shadow_candidates(path)
