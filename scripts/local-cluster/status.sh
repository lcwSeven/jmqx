#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/common.sh"

for node in "${NODES[@]}"; do
    status_node "$node"
    echo "  workdir: $(node_dir "$node")"
    echo "  log: $(stdout_log_file "$node")"
done
