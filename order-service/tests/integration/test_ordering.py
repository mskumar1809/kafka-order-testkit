"""Per-account ordering on a multi-partition topic."""

import json
from collections import defaultdict

from tests.factories import new_order
from tests.integration.helpers import read_all

# Fixed account names give a deterministic key->partition mapping (murmur2), so the
# precondition "accounts are spread over several partitions" is stable run to run.
ACCOUNTS = [f"ORDERING-{c}" for c in "ABCDEF"]
ORDERS_PER_ACCOUNT = 15


def test_orders_for_one_account_are_processed_in_sequence(api, settings, processors):
    processors.start()
    # Interleave accounts so ordering has to be preserved per key, not globally.
    for seq in range(ORDERS_PER_ACCOUNT):
        for account in ACCOUNTS:
            order = new_order(account=account, client_order_id=f"{account}-{seq:03d}")
            assert api.post("/orders", json=order).status_code == 202
    processors.drain()

    seen = defaultdict(list)
    partitions = defaultdict(set)
    for event in read_all(settings.kafka_bootstrap, settings.topics.accepted):
        body = json.loads(event.value())
        seen[body["account"]].append(body["client_order_id"])
        partitions[body["account"]].add(event.partition())

    assert len({p for ps in partitions.values() for p in ps}) > 1, "precondition: >1 partition used"
    for account in ACCOUNTS:
        assert partitions[account] == {next(iter(partitions[account]))}, f"{account} split across partitions"
        assert seen[account] == [f"{account}-{seq:03d}" for seq in range(ORDERS_PER_ACCOUNT)]
