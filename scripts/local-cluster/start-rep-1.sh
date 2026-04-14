#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/common.sh"

start_node "rep-1" "replicant" "1886" "8086" "7903" "" "" "false"
