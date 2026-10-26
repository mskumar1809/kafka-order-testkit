"""Order-entry API: validate against the shared contract, publish to Kafka, answer 202.

Run with:  python -m order_service.api
"""

import json
import logging
import uuid
from collections.abc import Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from order_service import contracts, logs
from order_service.config import Settings
from order_service.events import CORRELATION_HEADER, encode
from order_service.publisher import KafkaOrderPublisher, PublishError

log = logging.getLogger("order_service.api")
CORRELATION_HTTP_HEADER = "X-Correlation-ID"


def create_app(
    settings: Settings | None = None,
    publisher_factory: Callable[[Settings], object] = KafkaOrderPublisher,
) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.publisher = publisher_factory(settings)
        yield
        app.state.publisher.close()

    app = FastAPI(title="order-service", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.post("/orders")
    async def submit_order(request: Request) -> JSONResponse:
        correlation_id = request.headers.get(CORRELATION_HTTP_HEADER) or str(uuid.uuid4())
        reply_headers = {CORRELATION_HTTP_HEADER: correlation_id}

        # Validate with the JSON Schema itself (not a parallel Pydantic model) so the API
        # and the processor can never disagree about what a valid order is.
        try:
            order = json.loads(await request.body())
        except (UnicodeDecodeError, json.JSONDecodeError):
            return JSONResponse({"errors": ["body is not valid JSON"]}, 400, reply_headers)
        errors = contracts.validate(contracts.ORDER_NEW, order)
        if errors:
            return JSONResponse({"errors": errors}, 400, reply_headers)

        ctx = {"correlation_id": correlation_id, "order_id": order["order_id"]}
        try:
            # Key = account: Kafka orders messages within a partition, and the same key
            # always maps to the same partition, so each account's orders stay in sequence.
            await request.app.state.publisher.publish(
                settings.topics.new,
                key=order["account"],
                value=encode(order),
                headers=[(CORRELATION_HEADER, correlation_id)],
            )
        except PublishError as exc:
            log.warning("publish failed", extra={**ctx, "error": str(exc)})
            body = {"error": "order broker unavailable, retry with the same order_id"}
            return JSONResponse(body, 503, reply_headers)

        log.info("order accepted for processing", extra=ctx)
        return JSONResponse({"order_id": order["order_id"]}, 202, reply_headers)

    return app


def run() -> None:
    """Container entrypoint: JSON logs, uvicorn's own access log off (we log per order)."""
    import uvicorn

    logs.configure()
    uvicorn.run(create_app(), host="0.0.0.0", port=8000, log_config=None, access_log=False)


if __name__ == "__main__":
    run()
