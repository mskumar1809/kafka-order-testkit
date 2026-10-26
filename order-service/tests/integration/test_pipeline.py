"""End-to-end flow: HTTP API -> Kafka -> processor -> Postgres + output topics."""

import json

from order_service import contracts
from tests.factories import new_order
from tests.integration.helpers import header, query, read_all


def test_happy_path_preserves_correlation_id_end_to_end(api, settings, processors):
    processors.start()
    order = new_order(account="HAPPY-1")

    resp = api.post("/orders", json=order, headers={"X-Correlation-ID": "corr-happy-1"})
    assert resp.status_code == 202
    processors.drain()

    [row] = query(settings.database_url,
                  "SELECT status, account, qty, correlation_id FROM orders WHERE order_id = %s",
                  (order["order_id"],))
    assert row == ("ACCEPTED", "HAPPY-1", 100, "corr-happy-1")

    [event] = read_all(settings.kafka_bootstrap, settings.topics.accepted)
    assert json.loads(event.value())["order_id"] == order["order_id"]
    assert event.key() == b"HAPPY-1"
    assert header(event, "correlation_id") == "corr-happy-1"


def test_order_over_risk_limit_is_rejected(api, settings, processors):
    processors.start()
    order = new_order(qty=1_000_000, price=50.0)  # notional 50M > default 1M limit

    assert api.post("/orders", json=order).status_code == 202
    processors.drain()

    [(status, reason)] = query(settings.database_url,
                               "SELECT status, reject_reason FROM orders WHERE order_id = %s",
                               (order["order_id"],))
    assert status == "REJECTED" and "exceeds limit" in reason
    [event] = read_all(settings.kafka_bootstrap, settings.topics.rejected)
    assert json.loads(event.value())["order_id"] == order["order_id"]
    assert read_all(settings.kafka_bootstrap, settings.topics.accepted) == []


def test_processor_output_satisfies_the_shared_contract(api, settings, processors):
    """Contract test on real output: what the Python producer actually emits must validate
    against contracts/order-accepted.schema.json, the schema the Java consumer tests use."""
    processors.start()
    orders = [new_order(account="CONTRACT-1", side=side, symbol=symbol, qty=qty, price=price)
              for side, symbol, qty, price in [("BUY", "AAPL", 1, 0.01), ("SELL", "MSFT", 500, 412.5),
                                               ("BUY", "BRK.B", 3, 455)]]
    for order in orders:
        assert api.post("/orders", json=order).status_code == 202
    processors.drain()

    events = read_all(settings.kafka_bootstrap, settings.topics.accepted)
    assert len(events) == len(orders)
    for event in events:
        assert contracts.validate(contracts.ORDER_ACCEPTED, json.loads(event.value())) == []
