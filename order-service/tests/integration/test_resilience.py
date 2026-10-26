"""Broker outage: the API must fail fast (503) rather than hang, and recover afterwards."""

import time

from confluent_kafka.admin import AdminClient

from tests.factories import new_order
from tests.integration.helpers import query, wait_until


def broker_reachable(bootstrap: str) -> bool:
    try:
        AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=1)
        return True
    except Exception:  # noqa: BLE001 -- any failure means "not yet"
        return False


def test_api_returns_503_within_timeout_during_outage_and_recovers(api, settings, kafka, processors):
    processor = processors.start()
    order = new_order(account="OUTAGE-1")
    container = kafka.get_wrapped_container()

    # `docker pause` freezes the broker: TCP connections stay open but nothing answers,
    # which is nastier than a clean connection refusal and is what a hung broker looks like.
    container.pause()
    try:
        started = time.monotonic()
        resp = api.post("/orders", json=order)
        elapsed = time.monotonic() - started
    finally:
        container.unpause()

    assert resp.status_code == 503
    assert elapsed < settings.publish_timeout_s + 1.5, f"API took {elapsed:.1f}s to fail"

    wait_until(lambda: broker_reachable(settings.kafka_bootstrap), message="broker to come back")
    # The client retries with the SAME order_id until the API accepts it again. Whether or not
    # the first attempt reached Kafka, idempotency on order_id means exactly one row.
    wait_until(lambda: api.post("/orders", json=order).status_code == 202, timeout=60,
               interval=0.5, message="API to accept orders again")
    processors.drain(timeout=60)

    assert processor.is_alive(), f"processor died during the outage: {processor.error!r}"
    assert query(settings.database_url, "SELECT count(*) FROM orders WHERE order_id = %s",
                 (order["order_id"],)) == [(1,)]
