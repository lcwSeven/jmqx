#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/common.sh"

build_project

start_node "core-1" "core" "1883" "8083" "7900" "7800" "17800" "true"
sleep 2
start_node "core-2" "core" "1884" "8084" "7901" "7801" "17801" "false"
sleep 2
start_node "core-3" "core" "1885" "8085" "7902" "7802" "17802" "false"
sleep 5
start_node "rep-1" "replicant" "1886" "8086" "7903" "" "" "false"
sleep 2
start_node "rep-2" "replicant" "1887" "8087" "7904" "" "" "false"

echo
echo "[start-all] admin console: http://127.0.0.1:18081/admin/"
echo "[start-all] admin login: $ADMIN_USERNAME / $ADMIN_PASSWORD"
