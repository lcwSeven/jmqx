#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/common.sh"

run_node_foreground "core-3" "core" "1885" "8085" "7902" "7802" "17802" "false"
