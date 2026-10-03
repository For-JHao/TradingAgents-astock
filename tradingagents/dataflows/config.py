"""Run-local vendor configuration, including ToolNode worker context."""
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from threading import Event
from tradingagents.default_config import DEFAULT_CONFIG

_config = ContextVar("tradingagents_config", default=None)
_tracking_failure = ContextVar("missing_data_tracking_failure", default=None)

def initialize_config():
    if _config.get() is None:
        _config.set(deepcopy(DEFAULT_CONFIG))

def set_config(config):
    merged = deepcopy(DEFAULT_CONFIG)
    merged.update(deepcopy(config))
    _config.set(merged)

def get_config():
    return deepcopy(_config.get() or DEFAULT_CONFIG)

def mark_tracking_failed():
    flag = _tracking_failure.get()
    if flag is not None:
        flag.set()

def assert_tracking_healthy():
    flag = _tracking_failure.get()
    if flag is not None and flag.is_set():
        raise RuntimeError("missing_data_tracking_failed")

@contextmanager
def runtime_context(config):
    merged = deepcopy(DEFAULT_CONFIG)
    merged.update(deepcopy(config))
    token = _config.set(merged)
    flag_token = _tracking_failure.set(Event())
    try:
        yield
    finally:
        _tracking_failure.reset(flag_token)
        _config.reset(token)
