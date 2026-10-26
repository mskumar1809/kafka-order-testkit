"""(De)serialisation of Kafka message values and headers."""

import json
from datetime import datetime

from order_service import contracts
from order_service.risk import Decision

CORRELATION_HEADER = "correlation_id"


class InvalidMessage(ValueError):
    """The message can never be processed (bad encoding, bad JSON, or contract violation)."""


def parse_order(raw: bytes | None) -> dict:
    if raw is None:
        raise InvalidMessage("message has no value")
    try:
        order = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidMessage(f"not valid UTF-8 JSON: {exc}") from exc
    errors = contracts.validate(contracts.ORDER_NEW, order)
    if errors:
        raise InvalidMessage("; ".join(errors))
    return order


def result_event(order: dict, decision: Decision, processed_at: datetime) -> dict:
    """The value published to orders.accepted / orders.rejected."""
    event = {
        **order,
        "status": decision.status,
        "notional": float(decision.notional),
        "processed_at": processed_at.isoformat(),
    }
    if decision.reason is not None:
        event["reason"] = decision.reason
    return event


def encode(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def header_value(headers: list[tuple[str, bytes]] | None, name: str) -> str | None:
    for key, value in headers or []:
        if key == name:
            return value.decode("utf-8") if value is not None else None
    return None
