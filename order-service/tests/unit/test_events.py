import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from order_service import contracts, events, risk
from tests.factories import new_order

PROCESSED_AT = datetime(2026, 10, 27, 9, 30, tzinfo=timezone.utc)


def test_parse_order_accepts_valid_bytes():
    order = new_order()
    assert events.parse_order(json.dumps(order).encode()) == order


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(b"\xff\xfe", id="not-utf8"),
        pytest.param(b"{not json", id="malformed-json"),
        pytest.param(b"[1, 2]", id="json-but-not-an-object"),
        pytest.param(json.dumps(new_order(qty=-5)).encode(), id="schema-invalid"),
        pytest.param(None, id="null-value"),
    ],
)
def test_parse_order_rejects_bad_input(raw):
    with pytest.raises(events.InvalidMessage):
        events.parse_order(raw)


def test_schema_error_message_names_the_field():
    with pytest.raises(events.InvalidMessage, match="qty"):
        events.parse_order(json.dumps(new_order(qty=-5)).encode())


def test_accepted_event_satisfies_the_shared_contract():
    order = new_order()
    decision = risk.Decision(status=risk.ACCEPTED, notional=Decimal("18925"))
    event = events.result_event(order, decision, PROCESSED_AT)
    assert contracts.validate(contracts.ORDER_ACCEPTED, event) == []


def test_rejected_event_carries_status_and_reason():
    decision = risk.Decision(status=risk.REJECTED, notional=Decimal("5"), reason="too big")
    event = events.result_event(new_order(), decision, PROCESSED_AT)
    assert event["status"] == risk.REJECTED
    assert event["reason"] == "too big"


def test_encode_is_compact_and_round_trips():
    event = {"b": 1, "a": "x"}
    raw = events.encode(event)
    assert b" " not in raw
    assert json.loads(raw) == event


def test_header_value_decodes_bytes_and_handles_missing():
    headers = [("correlation_id", b"abc"), ("other", b"1")]
    assert events.header_value(headers, "correlation_id") == "abc"
    assert events.header_value(headers, "missing") is None
    assert events.header_value(None, "correlation_id") is None
