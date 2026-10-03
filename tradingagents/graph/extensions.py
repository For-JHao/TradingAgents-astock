"""Finite trusted Python extension seam; absent options retain native behavior."""
from dataclasses import dataclass, field
from typing import Any, Callable

ANALYST_ROLES = frozenset({"market", "social", "news", "fundamentals", "policy", "hot_money", "lockup"})

@dataclass(frozen=True)
class AnalystExtension:
    tools: tuple[Any, ...] | None = None
    instruction: str = ""

@dataclass(frozen=True)
class GraphExtensions:
    analysts: dict[str, AnalystExtension] = field(default_factory=dict)
    instrument_context: Callable[[str], str] | None = None
    quality_precheck: Callable[[dict], dict] | None = None
    memory_factory: Callable[[dict], Any] | None = None

    def __post_init__(self):
        if set(self.analysts) - ANALYST_ROLES:
            raise ValueError("unsupported_extension_role")
        for options in self.analysts.values():
            if options.tools is not None:
                names = [tool.name for tool in options.tools]
                if len(names) != len(set(names)):
                    raise ValueError("duplicate_extension_tool")

def effective_tools(native, extension):
    return list(extension.tools) if extension and extension.tools is not None else native
