"""Integration-owned finite shim at the first exception-to-text boundary."""
import json

import httpx
import requests

_STAGES = frozenset({
    "data", "http", "tool", "missing_data", "graph_initialization", "graph_execution",
    "engine_finalization", "report_publication", "job_commit", "unknown",
})


def safe_error(error, stage="data"):
    """Preserve the generic marker; append only a fixed, type-based diagnostic category."""
    # Do not inspect exception text, names, causes or request/response attributes.
    stage = stage if type(stage) is str and stage in _STAGES else "data"
    if isinstance(error, (TimeoutError, requests.exceptions.Timeout, httpx.TimeoutException)):
        category = "timeout"
    elif isinstance(error, (ConnectionError, requests.exceptions.ConnectionError, httpx.NetworkError)):
        category = "connection_error"
    elif isinstance(error, (requests.exceptions.HTTPError, httpx.HTTPStatusError)):
        category = "http_error"
    elif isinstance(error, (json.JSONDecodeError, requests.exceptions.JSONDecodeError)):
        category = "json_decode_error"
    elif isinstance(error, TypeError):
        category = "type_error"
    elif isinstance(error, AttributeError):
        category = "attribute_error"
    elif isinstance(error, KeyError):
        category = "key_error"
    else:
        category = None
    prefix = f"Error: external_operation_failed ({stage})"
    return f"{prefix} [category={category}]" if category else prefix
