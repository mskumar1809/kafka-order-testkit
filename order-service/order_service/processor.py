"""Order-processor worker: orders.new -> risk check -> Postgres -> orders.accepted / orders.rejected.

Delivery semantics
------------------
* Input is AT-LEAST-ONCE. Offsets are committed manually (enable.auto.commit=false) and only
  after the DB write and the output event are both durable. A crash anywhere before the
  commit means the message is delivered again after restart.
* The DB effect is IDEMPOTENT. orders.order_id is the primary key and the insert uses
  ON CONFLICT DO NOTHING, so a redelivered message never creates a second row.
* Output events are AT-LEAST-ONCE. A redelivered message re-publishes its result (using the
  decision already stored in the DB, never a fresh one), so downstream consumers can see the
  same order_id more than once and MUST dedupe on order_id (streams-positions does).
* Poison messages (not JSON, or violating the contract) can never succeed, so they are copied
  as raw bytes to orders.dlq with an `error` header and committed -- one bad message must
  not block its partition.
* Infrastructure failures (DB or Kafka down) are NOT poison: the worker raises without
  committing and exits; on restart the message is retried.

Run with:  python -m order_service.processor
"""

import logging
import signal
import threading
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

from order_service import events, logs, risk
from order_service.config import Settings
from order_service.db import OrderRepository

log = logging.getLogger("order_service.processor")

# Flushing per message trades throughput for simplicity: nothing is committed until its
# output is acked. A higher-throughput design would batch and commit after each batch.
_FLUSH_TIMEOUT_S = 10.0


class Processor:
    def __init__(self, consumer, producer, repo, settings: Settings):
        self._consumer = consumer
        self._producer = producer
        self._repo = repo
        self._settings = settings
        self._topics = settings.topics

    def run(self, stop: threading.Event) -> None:
        self._consumer.subscribe([self._topics.new])
        try:
            while not stop.is_set():
                msg = self._consumer.poll(0.2)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().fatal():
                        raise RuntimeError(f"fatal consumer error: {msg.error()}")
                    log.warning("consumer error", extra={"error": str(msg.error())})
                    continue
                self.handle(msg)
        finally:
            # With auto-commit off, close() leaves the group without committing anything.
            self._consumer.close()

    def handle(self, msg) -> None:
        correlation_id = events.header_value(msg.headers(), events.CORRELATION_HEADER)
        ctx = {"correlation_id": correlation_id, "partition": msg.partition(), "offset": msg.offset()}
        try:
            order = events.parse_order(msg.value())
        except events.InvalidMessage as exc:
            log.warning("dead-lettering message", extra={**ctx, "error": str(exc)})
            self._publish(self._topics.dlq, msg.key(), msg.value(), {
                "error": str(exc),
                "source": f"{msg.topic()}/{msg.partition()}/{msg.offset()}",
                events.CORRELATION_HEADER: correlation_id or "",
            })
        else:
            decision = risk.evaluate(order, self._settings.max_notional)
            stored = self._repo.save(order, decision, correlation_id, msg.partition(), msg.offset())
            event = events.result_event(order, stored, datetime.now(timezone.utc))
            topic = self._topics.accepted if stored.status == risk.ACCEPTED else self._topics.rejected
            self._publish(topic, msg.key(), events.encode(event),
                          {events.CORRELATION_HEADER: correlation_id or ""})
            log.info("order processed", extra={**ctx, "order_id": order["order_id"], "topic": topic})
        self._commit(msg)

    def _publish(self, topic: str, key: bytes | None, value: bytes | None, headers: dict) -> None:
        """Produce and block until the broker acks. Raises if the event is not durable."""
        failures = []

        def on_delivery(err, _msg) -> None:
            if err is not None:
                failures.append(err)

        self._producer.produce(topic, key=key, value=value, headers=list(headers.items()),
                               on_delivery=on_delivery)
        remaining = self._producer.flush(_FLUSH_TIMEOUT_S)
        if remaining or failures:
            raise RuntimeError(f"publish to {topic} failed: {failures or 'flush timed out'}")

    def _commit(self, msg) -> None:
        # Synchronous: when this returns the offset is stored on the broker, so a restart
        # resumes after this message instead of redelivering it.
        self._consumer.commit(message=msg, asynchronous=False)


def build(settings: Settings, cls: type[Processor] = Processor) -> Processor:
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": settings.consumer_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "enable.idempotence": True,
        "partitioner": "murmur2_random",  # Java-compatible key hashing, see publisher.py
    })
    return cls(consumer, producer, OrderRepository.connect(settings.database_url), settings)


def main() -> None:
    logs.configure()
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    log.info("processor starting")
    build(Settings.from_env()).run(stop)


if __name__ == "__main__":
    main()
