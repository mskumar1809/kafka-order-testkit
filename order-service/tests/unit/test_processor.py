"""Processor control flow with Kafka and Postgres replaced by recording fakes.

The point of these tests is the ORDER of side effects: the offset must only be
committed after the DB write and the output event are durable.
"""

import json
from decimal import Decimal

import pytest

from order_service import risk
from order_service.config import Settings
from order_service.processor import Processor
from tests.factories import new_order


class FakeMessage:
    def __init__(self, value, key=b"ACC-1", headers=None, partition=0, offset=7):
        self._value, self._key = value, key
        self._headers = headers if headers is not None else [("correlation_id", b"corr-1")]
        self._partition, self._offset = partition, offset

    def value(self): return self._value
    def key(self): return self._key
    def headers(self): return self._headers
    def topic(self): return "orders.new"
    def partition(self): return self._partition
    def offset(self): return self._offset
    def error(self): return None


class FakeProducer:
    def __init__(self, ops, delivery_error=None):
        self.ops, self.delivery_error, self.sent, self._pending = ops, delivery_error, [], []

    def produce(self, topic, key, value, headers, on_delivery):
        self.ops.append(f"produce:{topic}")
        self.sent.append({"topic": topic, "key": key, "value": value, "headers": dict(headers)})
        self._pending.append(on_delivery)

    def flush(self, timeout):
        self.ops.append("flush")
        for callback in self._pending:
            callback(self.delivery_error, None)
        self._pending.clear()
        return 0


class FakeConsumer:
    def __init__(self, ops):
        self.ops = ops

    def commit(self, message, asynchronous):
        assert asynchronous is False
        self.ops.append("commit")


class FakeRepo:
    """Stores the first decision per order_id, like INSERT ... ON CONFLICT DO NOTHING."""

    def __init__(self, ops, fail=False):
        self.ops, self.fail, self.rows = ops, fail, {}

    def save(self, order, decision, correlation_id, partition, offset):
        self.ops.append("save")
        if self.fail:
            raise ConnectionError("db down")
        return self.rows.setdefault(order["order_id"], decision)


@pytest.fixture
def ops():
    return []


def make_processor(ops, producer=None, repo=None, max_notional="1000000"):
    return Processor(
        consumer=FakeConsumer(ops),
        producer=producer or FakeProducer(ops),
        repo=repo or FakeRepo(ops),
        settings=Settings(max_notional=Decimal(max_notional)),
    )


def test_valid_order_is_saved_then_published_then_committed(ops):
    producer = FakeProducer(ops)
    make_processor(ops, producer=producer).handle(FakeMessage(json.dumps(new_order()).encode()))

    assert ops == ["save", "produce:orders.accepted", "flush", "commit"]
    assert producer.sent[0]["key"] == b"ACC-1"
    assert producer.sent[0]["headers"]["correlation_id"] == "corr-1"


def test_order_over_risk_limit_goes_to_rejected_topic(ops):
    producer = FakeProducer(ops)
    msg = FakeMessage(json.dumps(new_order(qty=10, price=200.0)).encode())
    make_processor(ops, producer=producer, max_notional="1000").handle(msg)

    assert ops == ["save", "produce:orders.rejected", "flush", "commit"]
    assert json.loads(producer.sent[0]["value"])["status"] == risk.REJECTED


@pytest.mark.parametrize("raw", [b"{not json", json.dumps(new_order(qty=0)).encode()])
def test_poison_message_goes_to_dlq_and_is_committed(ops, raw):
    producer = FakeProducer(ops)
    make_processor(ops, producer=producer).handle(FakeMessage(raw))

    assert ops == ["produce:orders.dlq", "flush", "commit"]  # no DB write
    dlq = producer.sent[0]
    assert dlq["value"] == raw  # raw bytes preserved for later inspection/replay
    assert dlq["headers"]["error"]
    assert dlq["headers"]["source"] == "orders.new/0/7"


def test_failed_delivery_does_not_commit(ops):
    producer = FakeProducer(ops, delivery_error="broker down")
    with pytest.raises(RuntimeError):
        make_processor(ops, producer=producer).handle(FakeMessage(json.dumps(new_order()).encode()))
    assert "commit" not in ops


def test_database_failure_does_not_commit_or_dead_letter(ops):
    # An infrastructure error is transient: the message must be retried, not parked in the DLQ.
    with pytest.raises(ConnectionError):
        make_processor(ops, repo=FakeRepo(ops, fail=True)).handle(
            FakeMessage(json.dumps(new_order()).encode())
        )
    assert ops == ["save"]


def test_redelivered_order_reemits_the_originally_stored_decision(ops):
    # First delivery was rejected under an old limit; on redelivery the limit is higher,
    # but the output must repeat what was recorded, not re-decide.
    order = new_order(qty=10, price=200.0)
    repo = FakeRepo(ops)
    make_processor(ops, repo=repo, max_notional="1000").handle(FakeMessage(json.dumps(order).encode()))

    producer = FakeProducer(ops)
    make_processor(ops, producer=producer, repo=repo, max_notional="1000000").handle(
        FakeMessage(json.dumps(order).encode())
    )
    assert producer.sent[0]["topic"] == "orders.rejected"
