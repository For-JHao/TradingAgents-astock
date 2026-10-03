#!/bin/sh
set -eu
RESEARCH_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$RESEARCH_ROOT/integrations/ata_research/src"
export PYTHONPATH="$RESEARCH_ROOT"
exec "$RESEARCH_ROOT/.venv/bin/python" -m research_api.app
