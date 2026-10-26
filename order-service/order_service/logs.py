"""One-line JSON logs so correlation_id / order_id can be searched in any log store."""

import json
import logging
import sys
from datetime import datetime, timezone

# Fields callers may attach with logger.info("...", extra={...}).
_CONTEXT_FIELDS = ("correlation_id", "order_id", "topic", "partition", "offset", "error")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update({k: getattr(record, k) for k in _CONTEXT_FIELDS if hasattr(record, k)})
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def configure(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)
