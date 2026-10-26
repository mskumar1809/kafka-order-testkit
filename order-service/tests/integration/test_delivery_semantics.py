"""At-least-once input + idempotent DB effect, proven under duplicates, crashes and replays."""

import json
from collections import Counter

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from order_service.processor import Processor
from tests.factories import new_order
from tests.integration.helpers import committed_offsets, produce_raw, query, read_all, wait_until


class SimulatedCrash(Exception):
    pass


class CrashBeforeCommit(Processor):
    """Dies after the DB write and the output event, but before committing the offset --
    the worst moment for an at-least-once consumer."""

    def _commit(self, msg):
        raise SimulatedCrash()


def count_rows(settings, order_ids) -> int:
    sql = "SELECT count(*) FROM orders WHERE order_id = ANY(%s::uuid[])"
    [(n,)] = query(settings.database_url, sql, (list(order_ids),))
    return n


def accepted_ids(settings) -> list[str]:
    events = read_all(settings.kafka_bootstrap, settings.topics.accepted)
    return [json.loads(m.value())["order_id"] for m in events]



def test_same_order_delivered_twice_creates_one_row(api, settings, processors):
    processors.start()
    order = new_order()

    # A client retry (e.g. after a timeout) sends the identical order again.
    assert api.post("/orders", json=order).status_code == 202
    assert api.post("/orders", json=order).status_code == 202
    processors.drain()

    assert count_rows(settings, [order["order_id"]]) == 1
    # Output is at-least-once: both deliveries emit an event, with the same order_id.
    assert accepted_ids(settings) == [order["order_id"]] * 2


def test_crash_after_side_effects_but_before_commit_is_replayed_safely(api, settings, processors):
    crashing = processors.start(CrashBeforeCommit)
    order = new_order()
    assert api.post("/orders", json=order).status_code == 202

    wait_until(lambda: not crashing.is_alive(), message="processor to crash")
    assert isinstance(crashing.error, SimulatedCrash)
    assert count_rows(settings, [order["order_id"]]) == 1, "DB write happened before the crash"
    assert all(offset < 0 for offset in committed_offsets(
        settings.kafka_bootstrap, settings.consumer_group, settings.topics.new).values()), \
        "nothing should have been committed"

    processors.start()  # restart: same consumer group, so the message is redelivered
    processors.drain()

    assert count_rows(settings, [order["order_id"]]) == 1
    assert accepted_ids(settings) == [order["order_id"]] * 2  # once before the crash, once on replay


def test_replay_from_earliest_creates_no_duplicate_rows(api, settings, processors):
    orders = [new_order(account=f"REPLAY-{i % 2}") for i in range(6)]
    for order in orders:
        assert api.post("/orders", json=order).status_code == 202
    first = processors.start()
    processors.drain()
    first.stop()  # leave the group so its offsets can be reset

    # Reset the (now inactive) consumer group to the beginning of every partition.
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    partitions = [TopicPartition(settings.topics.new, p, 0) for p in range(3)]
    for future in admin.alter_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(settings.consumer_group, partitions)]).values():
        future.result(timeout=10)

    processors.start()
    processors.drain()

    ids = [o["order_id"] for o in orders]
    assert count_rows(settings, ids) == len(orders)
    # Every order was really reprocessed (the replay was not a no-op) ...
    assert Counter(accepted_ids(settings)) == {order_id: 2 for order_id in ids}


def test_poison_messages_go_to_dlq_without_blocking_the_partition(settings, processors):
    key = b"POISON-1"  # same key -> same partition, so a stuck poison pill would block `valid`
    not_json = b"{this is not json"
    schema_invalid = json.dumps(new_order(account="POISON-1", qty=-5)).encode()
    valid = new_order(account="POISON-1")
    produce_raw(settings.kafka_bootstrap, settings.topics.new, [
        (key, not_json, [("correlation_id", b"corr-bad-json")]),
        (key, schema_invalid, []),
        (key, json.dumps(valid).encode(), []),
    ])
    processors.start()
    processors.drain()

    dead = read_all(settings.kafka_bootstrap, settings.topics.dlq)
    assert [m.value() for m in dead] == [not_json, schema_invalid]  # raw bytes, original order
    errors = [dict(m.headers())["error"].decode() for m in dead]
    assert "not valid UTF-8 JSON" in errors[0]
    assert "qty" in errors[1]
    assert dict(dead[0].headers())["correlation_id"] == b"corr-bad-json"

    # The valid message behind them was still processed.
    assert count_rows(settings, [valid["order_id"]]) == 1
    assert accepted_ids(settings) == [valid["order_id"]]
