import os, re
from pathlib import Path
from typing import Any
from fastapi import HTTPException
from research_api.http.dto import ResearchJobRequest

ROOT_DIR = Path(__file__).resolve().parents[4]
ALLOWED_ANALYSTS = {
    "market",
    "social",
    "news",
    "fundamentals",
    "policy",
    "hot_money",
    "lockup",
}

DEFAULT_MODELS = {
    "minimax": ("MiniMax-M2.7-highspeed", "MiniMax-M2.7"),
    "deepseek": ("deepseek-v4-flash", "deepseek-v4-pro"),
    "qwen": ("qwen-plus", "qwen3.6-plus"),
    "glm": ("glm-5", "glm-5.1"),
    "openai": ("gpt-5.4-mini", "gpt-5.4"),
    "anthropic": ("claude-sonnet-4-6", "claude-sonnet-4-6"),
    "google": ("gemini-2.5-flash", "gemini-2.5-pro"),
    "xai": ("grok-4-1-fast-non-reasoning", "grok-4-0709"),
    "ollama": ("qwen3:latest", "qwen3:latest"),
}


def _choose_provider(request_provider: str | None) -> str:
    provider = (
        request_provider
        or os.getenv("ASTOCK_LLM_PROVIDER")
        or os.getenv("TRADINGAGENTS_LLM_PROVIDER")
    )
    if provider:
        return provider.lower()
    if os.getenv("MINIMAX_API_KEY"):
        return "minimax"
    if os.getenv("DEEPSEEK_API_KEY"):
        return "deepseek"
    return "deepseek"


def _model_defaults(provider: str) -> tuple[str, str]:
    return DEFAULT_MODELS.get(
        provider.lower(),
        DEFAULT_MODELS["deepseek"],
    )


def _build_config(request: ResearchJobRequest) -> dict[str, Any]:
    config = {}
    provider = _choose_provider(request.llm_provider)
    if provider not in DEFAULT_MODELS:
        raise HTTPException(status_code=400, detail="Unsupported model provider")
    default_quick, default_deep = _model_defaults(provider)

    config["llm_provider"] = provider
    config["quick_think_llm"] = (
        request.quick_think_llm
        or os.getenv("ASTOCK_QUICK_THINK_LLM")
        or os.getenv("TRADINGAGENTS_QUICK_THINK_LLM")
        or default_quick
    )
    config["deep_think_llm"] = (
        request.deep_think_llm
        or os.getenv("ASTOCK_DEEP_THINK_LLM")
        or os.getenv("TRADINGAGENTS_DEEP_THINK_LLM")
        or default_deep
    )
    backend_url = (
        request.backend_url
        or os.getenv("ASTOCK_BACKEND_URL")
        or os.getenv("BACKEND_URL")
        or ""
    ).strip()
    trusted_backends = {value.strip().rstrip("/") for value in (os.getenv("ASTOCK_BACKEND_URL", ""), os.getenv("BACKEND_URL", ""), *os.getenv("ASTOCK_ALLOWED_BACKEND_URLS", "").split(",")) if value.strip()}
    if request.backend_url and request.backend_url.rstrip("/") not in trusted_backends:
        raise HTTPException(status_code=400, detail="backend_url is not configured for this service")
    for key in ("quick_think_llm", "deep_think_llm"):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}", config[key]):
            raise HTTPException(status_code=400, detail="Invalid model identifier")
    if request.output_language not in {"Chinese", "English"}:
        raise HTTPException(status_code=400, detail="Unsupported output_language")
    config["backend_url"] = backend_url or None
    config["max_debate_rounds"] = request.research_depth
    config["max_risk_discuss_rounds"] = request.research_depth
    config["output_language"] = request.output_language
    config["checkpoint_enabled"] = request.checkpoint_enabled
    return config


def _select_analysts(request: ResearchJobRequest, ticker: str | None = None) -> list[str]:
    if request.selected_analysts:
        analysts = request.selected_analysts
    elif ticker and re.fullmatch(r"[15]\d{5}", ticker):
        analysts = [
            "market",
            "social",
            "news",
            "fundamentals",
            "policy",
            "hot_money",
        ]
    else:
        analysts = [
            "market",
            "social",
            "news",
            "fundamentals",
            "policy",
            "hot_money",
            "lockup",
        ]
    invalid = sorted(set(analysts) - ALLOWED_ANALYSTS)
    if invalid:
        raise HTTPException(status_code=400, detail=f"Unsupported analysts: {invalid}")
    if len(analysts) != len(set(analysts)):
        raise HTTPException(status_code=400, detail="Duplicate analysts")
    return analysts
