"""Loads the shared JSON Schemas in <repo>/contracts and validates documents against them."""

import json
import os
from functools import cache
from pathlib import Path

from jsonschema import Draft202012Validator

# In the repo the contracts live two levels above this package; the Docker image sets CONTRACTS_DIR.
CONTRACTS_DIR = Path(os.environ.get("CONTRACTS_DIR", Path(__file__).resolve().parents[2] / "contracts"))

ORDER_NEW = "order-new.schema.json"
ORDER_ACCEPTED = "order-accepted.schema.json"


@cache
def _validator(schema_name: str) -> Draft202012Validator:
    schema = json.loads((CONTRACTS_DIR / schema_name).read_text())
    Draft202012Validator.check_schema(schema)
    # "format" (uuid, date-time) is annotation-only in 2020-12 unless a FormatChecker is passed.
    return Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)


def validate(schema_name: str, document: object) -> list[str]:
    """Return human-readable validation errors; an empty list means valid."""
    errors = _validator(schema_name).iter_errors(document)
    return [f"{'/'.join(map(str, e.absolute_path)) or '<root>'}: {e.message}" for e in errors]
