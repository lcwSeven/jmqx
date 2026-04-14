#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/common.sh"

start_node "core-1" "core" "1883" "8083" "7900" "7800" "17800" "true"
