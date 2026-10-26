"""Runtime configuration, read from environment variables with local-dev defaults."""

import os
from dataclasses import dataclass, field
from decimal import Decimal


@dataclass(frozen=True)
class Topics:
    new: str = "orders.new"
    accepted: str = "orders.accepted"
    rejected: str = "orders.rejected"
    dlq: str = "orders.dlq"

    @classmethod
    def with_suffix(cls, suffix: str) -> "Topics":
        """Unique topic names, used by tests to isolate from each other."""
        return cls(*(f"{name}.{suffix}" for name in (cls.new, cls.accepted, cls.rejected, cls.dlq)))

    def all(self) -> list[str]:
        return [self.new, self.accepted, self.rejected, self.dlq]


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = "localhost:9092"
    database_url: str = "postgresql://orders:orders@localhost:5432/orders"
    topics: Topics = field(default_factory=Topics)
    consumer_group: str = "order-processor"
    max_notional: Decimal = Decimal("1000000")
    # Upper bound on how long POST /orders waits for a Kafka ack before answering 503.
    publish_timeout_s: float = 2.0

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.environ.get
        return cls(
            kafka_bootstrap=env("KAFKA_BOOTSTRAP", cls.kafka_bootstrap),
            database_url=env("DATABASE_URL", cls.database_url),
            consumer_group=env("CONSUMER_GROUP", cls.consumer_group),
            max_notional=Decimal(env("RISK_MAX_NOTIONAL", str(cls.max_notional))),
            publish_timeout_s=float(env("PUBLISH_TIMEOUT_S", str(cls.publish_timeout_s))),
        )
