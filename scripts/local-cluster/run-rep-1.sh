#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/common.sh"

run_node_foreground "rep-1" "replicant" "1886" "8086" "7903" "" "" "false"
