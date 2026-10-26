"""API behaviour with Kafka replaced by an in-memory fake publisher."""

import json

import pytest
from fastapi.testclient import TestClient

from order_service.api import create_app
from order_service.config import Settings
from order_service.publisher import PublishError
from tests.factories import new_order


class FakePublisher:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.sent: list[dict] = []

    async def publish(self, topic, key, value, headers):
        if self.fail:
            raise PublishError("broker unavailable")
        self.sent.append({"topic": topic, "key": key, "value": value, "headers": dict(headers)})

    def close(self):
        pass


@pytest.fixture
def publisher():
    return FakePublisher()


@pytest.fixture
def client(publisher):
    app = create_app(Settings(), publisher_factory=lambda _settings: publisher)
    with TestClient(app) as c:
        yield c


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_valid_order_is_published_keyed_by_account(client, publisher):
    order = new_order(account="ACC-42")
    resp = client.post("/orders", json=order)

    assert resp.status_code == 202
    assert resp.json() == {"order_id": order["order_id"]}
    [sent] = publisher.sent
    assert sent["topic"] == "orders.new"
    assert sent["key"] == "ACC-42"
    assert json.loads(sent["value"]) == order


def test_correlation_id_is_generated_when_absent(client, publisher):
    resp = client.post("/orders", json=new_order())
    generated = resp.headers["X-Correlation-ID"]
    assert generated
    assert publisher.sent[0]["headers"]["correlation_id"] == generated


def test_correlation_id_is_propagated_when_present(client, publisher):
    resp = client.post("/orders", json=new_order(), headers={"X-Correlation-ID": "corr-123"})
    assert resp.headers["X-Correlation-ID"] == "corr-123"
    assert publisher.sent[0]["headers"]["correlation_id"] == "corr-123"


def test_schema_invalid_order_returns_400_and_is_not_published(client, publisher):
    resp = client.post("/orders", json=new_order(side="HOLD"))
    assert resp.status_code == 400
    assert resp.json()["errors"]
    assert publisher.sent == []


def test_malformed_json_returns_400(client, publisher):
    resp = client.post("/orders", content=b"{not json", headers={"Content-Type": "application/json"})
    assert resp.status_code == 400
    assert publisher.sent == []


def test_publish_failure_returns_503():
    app = create_app(Settings(), publisher_factory=lambda _settings: FakePublisher(fail=True))
    with TestClient(app) as client:
        resp = client.post("/orders", json=new_order())
    assert resp.status_code == 503
