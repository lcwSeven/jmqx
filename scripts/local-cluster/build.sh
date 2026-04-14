#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/common.sh"

build_project

echo
echo "[build] runtime classpath file: $RUNTIME_CP_FILE"
echo "[build] cluster root: $CLUSTER_ROOT"
