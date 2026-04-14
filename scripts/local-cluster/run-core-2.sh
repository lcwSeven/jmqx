#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/common.sh"

run_node_foreground "core-2" "core" "1884" "8084" "7901" "7801" "17801" "false"
