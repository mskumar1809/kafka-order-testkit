# Build context is the repo root (see docker-compose.yml) so the shared contracts/ can be copied in.
FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:0.10.11 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    CONTRACTS_DIR=/app/contracts

WORKDIR /app/order-service
# Dependencies first (cached layer), then the code.
COPY order-service/pyproject.toml order-service/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY order-service/order_service ./order_service
RUN uv sync --frozen --no-dev
COPY contracts /app/contracts

USER nobody
EXPOSE 8000
# Default: the API. docker-compose overrides the command for the processor.
CMD ["python", "-m", "order_service.api"]
