"""Builders for test data shared by the unit and integration suites."""

import uuid
from datetime import datetime, timezone


def new_order(**overrides) -> dict:
    """A valid order-new payload. Override any field per test."""
    order = {
        "order_id": str(uuid.uuid4()),
        "client_order_id": f"cli-{uuid.uuid4().hex[:8]}",
        "account": "ACC-1",
        "symbol": "AAPL",
        "side": "BUY",
        "qty": 100,
        "price": 189.25,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    order.update(overrides)
    return order
