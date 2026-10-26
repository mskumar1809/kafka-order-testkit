"""The DB `positions` view must equal positions computed independently from what was sent."""

import random
import uuid
from collections import defaultdict
from decimal import Decimal

from tests.factories import new_order
from tests.integration.helpers import query


def test_positions_view_matches_independently_computed_positions(api, settings, processors):
    processors.start()
    rng = random.Random(42)  # fixed seed: same data every run
    run = uuid.uuid4().hex[:6]
    accounts = [f"RECON-{run}-{i}" for i in range(3)]
    expected: dict[tuple[str, str], int] = defaultdict(int)

    for _ in range(60):
        # ~1 in 6 orders is sized to breach the 1M notional limit and must not count.
        qty = rng.choice([rng.randint(1, 500)] * 5 + [50_000])
        order = new_order(account=rng.choice(accounts), symbol=rng.choice(["AAPL", "MSFT"]),
                          side=rng.choice(["BUY", "SELL"]), qty=qty, price=rng.choice([99.5, 250.25]))
        assert api.post("/orders", json=order).status_code == 202
        # Independent oracle: plain arithmetic, deliberately not calling order_service.risk.
        if Decimal(order["qty"]) * Decimal(str(order["price"])) <= settings.max_notional:
            expected[(order["account"], order["symbol"])] += qty if order["side"] == "BUY" else -qty

    processors.drain()

    rows = query(settings.database_url,
                 "SELECT account, symbol, net_qty FROM positions WHERE account = ANY(%s)", (accounts,))
    assert {(a, s): int(q) for a, s, q in rows} == dict(expected)
