"""Postgres access for the processor."""

import psycopg

from order_service.risk import Decision

_INSERT = """
INSERT INTO orders (order_id, client_order_id, account, symbol, side, qty, price, notional,
                    order_ts, status, reject_reason, correlation_id, kafka_partition, kafka_offset)
VALUES (%(order_id)s, %(client_order_id)s, %(account)s, %(symbol)s, %(side)s, %(qty)s, %(price)s,
        %(notional)s, %(ts)s, %(status)s, %(reason)s, %(correlation_id)s, %(partition)s, %(offset)s)
ON CONFLICT (order_id) DO NOTHING
RETURNING order_id
"""

_SELECT_DECISION = "SELECT status, notional, reject_reason FROM orders WHERE order_id = %s"


class OrderRepository:
    def __init__(self, conn: psycopg.Connection):
        self._conn = conn

    @classmethod
    def connect(cls, dsn: str) -> "OrderRepository":
        # autocommit: every statement is its own transaction, so a row is durable as soon
        # as execute() returns -- before we produce the output event or commit the offset.
        return cls(psycopg.connect(dsn, autocommit=True))

    def save(self, order: dict, decision: Decision, correlation_id: str | None,
             partition: int, offset: int) -> Decision:
        """Insert the order once. Returns the decision that is actually stored: the new one,
        or -- if this order_id was already processed -- the original one (first writer wins)."""
        params = {
            **order,
            "status": decision.status,
            "notional": decision.notional,
            "reason": decision.reason,
            "correlation_id": correlation_id,
            "partition": partition,
            "offset": offset,
        }
        with self._conn.cursor() as cur:
            cur.execute(_INSERT, params)
            if cur.fetchone() is not None:
                return decision
            cur.execute(_SELECT_DECISION, (order["order_id"],))
            status, notional, reason = cur.fetchone()
            return Decision(status, notional, reason)

    def close(self) -> None:
        self._conn.close()
