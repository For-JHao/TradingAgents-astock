#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -x ".venv/bin/tradingagents-research-api" ]]; then
  echo "未找到虚拟环境或 API 命令，请先执行："
  echo "  python3 -m venv .venv && source .venv/bin/activate && pip install -e ."
  exit 1
fi

exec ".venv/bin/tradingagents-research-api" "$@"
