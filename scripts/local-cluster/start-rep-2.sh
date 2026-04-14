#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/common.sh"

start_node "rep-2" "replicant" "1887" "8087" "7904" "" "" "false"
