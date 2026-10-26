"""Async-friendly Kafka publisher with a hard upper bound on how long a publish can take."""

import asyncio
import threading

from confluent_kafka import KafkaException, Producer

from order_service.config import Settings


class PublishError(RuntimeError):
    """The broker did not acknowledge the message in time. Outcome is unknown, so callers
    should retry with the same order_id (the processor is idempotent on order_id)."""


class KafkaOrderPublisher:
    def __init__(self, settings: Settings):
        timeout_ms = int(settings.publish_timeout_s * 1000)
        self._timeout_s = settings.publish_timeout_s
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                # Idempotent producer: acks=all, and broker-side dedupe of internal retries,
                # which also keeps per-partition (= per-account) ordering when retrying.
                "enable.idempotence": True,
                "linger.ms": 1,
                # Same key->partition hashing as the Java client (librdkafka's default is
                # CRC32), so Python and JVM producers agree on where an account lives.
                "partitioner": "murmur2_random",
                # librdkafka gives up on (and reports) any message not acked within this time.
                "message.timeout.ms": timeout_ms,
            }
        )
        # Delivery callbacks only fire inside poll(); a dedicated thread serves them so
        # request handlers never block on Kafka I/O.
        self._closed = threading.Event()
        self._poller = threading.Thread(target=self._poll_loop, name="kafka-poller", daemon=True)
        self._poller.start()

    def _poll_loop(self) -> None:
        while not self._closed.is_set():
            self._producer.poll(0.1)

    async def publish(self, topic: str, key: str, value: bytes, headers: list[tuple[str, str]]) -> None:
        loop = asyncio.get_running_loop()
        delivered: asyncio.Future = loop.create_future()

        def on_delivery(err, _msg) -> None:  # runs on the poller thread
            loop.call_soon_threadsafe(_resolve, delivered, err)

        try:
            self._producer.produce(topic, key=key, value=value, headers=headers, on_delivery=on_delivery)
        except (BufferError, KafkaException) as exc:  # local queue full / fatal producer error
            raise PublishError(str(exc)) from exc
        try:
            # Hard upper bound for the HTTP caller. librdkafka only scans for expired messages
            # about once a second, so this can fire before its own timeout report; either way
            # the message may still reach the broker later, which is why retries reuse order_id.
            err = await asyncio.wait_for(delivered, self._timeout_s + 0.5)
        except asyncio.TimeoutError as exc:
            raise PublishError("timed out waiting for broker ack") from exc
        if err is not None:
            raise PublishError(str(err))

    def close(self) -> None:
        self._closed.set()
        self._poller.join()
        self._producer.flush(self._timeout_s)


def _resolve(future: asyncio.Future, err) -> None:
    if not future.done():  # may already be cancelled by wait_for's timeout
        future.set_result(err)
