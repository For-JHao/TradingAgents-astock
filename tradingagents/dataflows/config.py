from __future__ import annotations

import copy
from contextvars import ContextVar
from typing import Dict

import tradingagents.default_config as default_config

# LangGraph executes nodes and tools in context-propagating worker threads. A
# ContextVar keeps each research job's provider, language, vendor and cache
# settings isolated while still flowing into those child workers.
_config: ContextVar[Dict | None] = ContextVar("tradingagents_config", default=None)


def initialize_config():
    """Initialize the configuration with default values."""
    if _config.get() is None:
        _config.set(copy.deepcopy(default_config.DEFAULT_CONFIG))


def set_config(config: Dict):
    """Set configuration for the current job execution context."""
    current = copy.deepcopy(_config.get() or default_config.DEFAULT_CONFIG)
    current.update(copy.deepcopy(config))
    _config.set(current)


def get_config() -> Dict:
    """Get the current configuration."""
    if _config.get() is None:
        initialize_config()
    return copy.deepcopy(_config.get())


# Initialize with default config
initialize_config()
