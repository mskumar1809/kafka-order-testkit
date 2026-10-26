"""Real Kafka and Postgres via Testcontainers.

Containers are session-scoped (started once, ~10 s). Isolation between tests comes from
unique topic names, consumer group ids and order ids per test, not from new containers."""

import uuid
from pathlib import Path

import psycopg
import pytest
from fastapi.testclient import TestClient
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer

from order_service.api import create_app
from order_service.config import Settings, Topics
from order_service.processor import Processor, build
from tests.integration.helpers import ProcessorThread, create_topics, wait_until_committed

KAFKA_IMAGE = "confluentinc/cp-kafka:7.6.1"  # multi-arch: runs natively on Apple Silicon and CI
POSTGRES_IMAGE = "postgres:16-alpine"
SCHEMA_SQL = Path(__file__).resolve().parents[2] / "sql" / "schema.sql"


@pytest.fixture(scope="session")
def kafka():
    with KafkaContainer(KAFKA_IMAGE).with_kraft() as container:
        yield container


@pytest.fixture(scope="session")
def kafka_bootstrap(kafka) -> str:
    return kafka.get_bootstrap_server()


@pytest.fixture(scope="session")
def database_url() -> str:
    # Throwaway local test credentials, not a secret.
    with PostgresContainer(POSTGRES_IMAGE, username="test", password="test", dbname="orders",
                           driver=None) as pg:
        url = pg.get_connection_url()
        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute(SCHEMA_SQL.read_text())
        yield url


@pytest.fixture
def settings(kafka_bootstrap, database_url) -> Settings:
    topics = Topics.with_suffix(uuid.uuid4().hex[:8])
    create_topics(kafka_bootstrap, topics.all(), partitions=3)
    return Settings(
        kafka_bootstrap=kafka_bootstrap,
        database_url=database_url,
        topics=topics,
        consumer_group=f"processor-{uuid.uuid4().hex[:8]}",
        publish_timeout_s=2.0,
    )


@pytest.fixture
def api(settings):
    with TestClient(create_app(settings)) as client:
        yield client


class Processors:
    """Starts processor threads for one test and waits for them to finish their input."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._started: list[ProcessorThread] = []

    def start(self, cls: type[Processor] = Processor) -> ProcessorThread:
        thread = ProcessorThread(build(self._settings, cls)).start()
        self._started.append(thread)
        return thread

    def drain(self, timeout: float = 30.0) -> None:
        """Block until everything on orders.new is committed, i.e. fully processed.
        Fails immediately (with the processor's own exception) if every processor has died."""
        def alive() -> bool:
            if any(t.is_alive() for t in self._started):
                return True
            raise AssertionError(f"all processors died: {[t.error for t in self._started]!r}")

        s = self._settings
        wait_until_committed(s.kafka_bootstrap, s.consumer_group, s.topics.new, timeout, alive)

    def stop_all(self) -> None:
        for thread in self._started:
            if thread.is_alive():
                thread.stop()


@pytest.fixture
def processors(settings):
    procs = Processors(settings)
    yield procs
    procs.stop_all()
