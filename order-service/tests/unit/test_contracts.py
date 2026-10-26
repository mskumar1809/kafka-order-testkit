import json

import pytest

from order_service import contracts
from tests.factories import new_order


def test_valid_order_has_no_errors():
    assert contracts.validate(contracts.ORDER_NEW, new_order()) == []


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda o: o.pop("account"), id="missing-account"),
        pytest.param(lambda o: o.update(side="HOLD"), id="bad-side"),
        pytest.param(lambda o: o.update(qty=0), id="qty-zero"),
        pytest.param(lambda o: o.update(qty=1.5), id="qty-not-integer"),
        pytest.param(lambda o: o.update(price=0), id="price-zero"),
        pytest.param(lambda o: o.update(price=-1.0), id="price-negative"),
        pytest.param(lambda o: o.update(price="10"), id="price-string"),
        pytest.param(lambda o: o.update(order_id="not-a-uuid"), id="bad-uuid"),
        pytest.param(lambda o: o.update(ts="yesterday"), id="bad-timestamp"),
        pytest.param(lambda o: o.update(extra="x"), id="unknown-field"),
    ],
)
def test_invalid_order_is_rejected(mutate):
    order = new_order()
    mutate(order)
    assert contracts.validate(contracts.ORDER_NEW, order) != []


def test_shared_accepted_example_matches_contract():
    # The same example file is used by the Java consumer's tests, so both sides
    # agree on what a valid orders.accepted event looks like.
    example = json.loads((contracts.CONTRACTS_DIR / "examples" / "order-accepted.json").read_text())
    assert contracts.validate(contracts.ORDER_ACCEPTED, example) == []
