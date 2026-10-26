"""Polling helpers for asynchronous assertions. No bare sleeps: every wait has a condition
and a deadline, and fails with a message saying what never happened."""

import threading
import time
import uuid
from collections.abc import Callable

import psycopg
from confluent_kafka import Consumer, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


def wait_until(condition: Callable[[], object], timeout: float = 30.0, interval: float = 0.1,
               message: str = "condition") -> object:
    deadline = time.monotonic() + timeout
    while True:
        result = condition()
        if result:
            return result
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for: {message}")
        time.sleep(interval)


def create_topics(bootstrap: str, names: list[str], partitions: int = 3) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    new_topics = [NewTopic(n, num_partitions=partitions, replication_factor=1) for n in names]
    futures = admin.create_topics(new_topics)
    for future in futures.values():
        future.result(timeout=30)


def produce_raw(bootstrap: str, topic: str, messages: list[tuple[bytes, bytes, list]]) -> None:
    """Write (key, value, headers) straight to a topic, bypassing the API's validation."""
    producer = Producer({"bootstrap.servers": bootstrap, "partitioner": "murmur2_random"})
    for key, value, headers in messages:
        producer.produce(topic, key=key, value=value, headers=headers)
    assert producer.flush(30) == 0, "test producer could not deliver"


def _partitions(consumer: Consumer, topic: str) -> list[int]:
    return sorted(consumer.list_topics(topic, timeout=10).topics[topic].partitions)


def read_all(bootstrap: str, topic: str, timeout: float = 30.0) -> list[Message]:
    """Every message currently in `topic`, in per-partition offset order.

    Reads up to the high watermark captured at call time, so the result is exact rather
    than "whatever arrived before a timeout"."""
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"reader-{uuid.uuid4()}",
                         "enable.auto.commit": False})
    try:
        ends = {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
                for p in _partitions(consumer, topic)}
        pending = {p: high for p, (low, high) in ends.items() if high > low}
        consumer.assign([TopicPartition(topic, p, ends[p][0]) for p in pending])
        messages: list[Message] = []
        deadline = time.monotonic() + timeout
        while pending:
            if time.monotonic() > deadline:
                raise AssertionError(f"timed out reading {topic}; partitions left: {pending}")
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            messages.append(msg)
            if msg.offset() + 1 >= pending.get(msg.partition(), 0):
                pending.pop(msg.partition(), None)
        return messages
    finally:
        consumer.close()


def wait_until_committed(bootstrap: str, group: str, topic: str, timeout: float = 30.0,
                         alive: Callable[[], bool] = lambda: True) -> None:
    """Wait until `group` has committed every message currently in `topic`.

    The processor commits only after its DB write and output event are durable, so once
    this returns, all side effects of those messages are observable. `alive` lets the
    caller fail fast if no consumer is left to make progress."""
    probe = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
    try:
        ends = {p: probe.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
                for p in _partitions(probe, topic)}

        def caught_up() -> bool:
            if not alive():
                raise AssertionError(f"no live consumer in group {group}; it can never catch up")
            committed = probe.committed([TopicPartition(topic, p) for p in ends], timeout=10)
            return all(tp.offset >= ends[tp.partition] or ends[tp.partition] == 0 for tp in committed)

        wait_until(caught_up, timeout, interval=0.2, message=f"group {group} to commit all of {topic}")
    finally:
        probe.close()


def committed_offsets(bootstrap: str, group: str, topic: str) -> dict[int, int]:
    probe = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
    try:
        tps = [TopicPartition(topic, p) for p in _partitions(probe, topic)]
        return {tp.partition: tp.offset for tp in probe.committed(tps, timeout=10)}
    finally:
        probe.close()


def query(database_url: str, sql: str, params: tuple = ()) -> list[tuple]:
    with psycopg.connect(database_url) as conn:
        return conn.execute(sql, params).fetchall()


def header(msg: Message, name: str) -> str | None:
    return next((v.decode() for k, v in msg.headers() or [] if k == name), None)


class ProcessorThread:
    """Runs a Processor in a background thread and captures any exception it dies with."""

    def __init__(self, processor):
        self.processor = processor
        self.error: BaseException | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        try:
            self.processor.run(self._stop)
        except BaseException as exc:  # noqa: BLE001 -- surfaced to the test via .error
            self.error = exc

    def start(self) -> "ProcessorThread":
        self._thread.start()
        return self

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def stop(self, timeout: float = 15.0) -> None:
        self._stop.set()
        self._thread.join(timeout)
        assert not self._thread.is_alive(), "processor did not stop"
