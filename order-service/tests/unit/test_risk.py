from decimal import Decimal

from order_service import risk
from tests.factories import new_order


def test_order_at_the_limit_is_accepted():
    decision = risk.evaluate(new_order(qty=10, price=100.0), max_notional=Decimal("1000"))
    assert decision.status == risk.ACCEPTED
    assert decision.notional == Decimal("1000")
    assert decision.reason is None


def test_order_above_the_limit_is_rejected_with_reason():
    decision = risk.evaluate(new_order(qty=11, price=100.0), max_notional=Decimal("1000"))
    assert decision.status == risk.REJECTED
    assert "notional" in decision.reason


def test_notional_uses_exact_decimal_arithmetic():
    # 3 * 0.1 is 0.30000000000000004 in binary floating point.
    decision = risk.evaluate(new_order(qty=3, price=0.1), max_notional=Decimal("0.3"))
    assert decision.notional == Decimal("0.3")
    assert decision.status == risk.ACCEPTED
