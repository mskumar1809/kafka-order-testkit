"""Pre-trade risk check: a single notional limit per order."""

from dataclasses import dataclass
from decimal import Decimal

ACCEPTED = "ACCEPTED"
REJECTED = "REJECTED"


@dataclass(frozen=True)
class Decision:
    status: str
    notional: Decimal
    reason: str | None = None


def evaluate(order: dict, max_notional: Decimal) -> Decision:
    # Decimal(str(price)) avoids binary float error: 3 * 0.1 must equal 0.3 exactly.
    notional = Decimal(order["qty"]) * Decimal(str(order["price"]))
    if notional <= max_notional:
        return Decision(ACCEPTED, notional)
    return Decision(REJECTED, notional, f"notional {notional} exceeds limit {max_notional}")
