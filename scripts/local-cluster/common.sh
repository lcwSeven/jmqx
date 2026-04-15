#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CLUSTER_ROOT="${CLUSTER_ROOT:-/tmp/jmqx-local}"
MAVEN_BIN="${MAVEN_BIN:-}"
JAVA_BIN="${JAVA_BIN:-java}"
MAVEN_REPO_LOCAL="${MAVEN_REPO_LOCAL:-/tmp/jmqx-m2}"
JMQX_JAVA_OPTS="${JMQX_JAVA_OPTS:---add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED}"

CLUSTER_ID="${CLUSTER_ID:-local-lab}"
ADMIN_USERNAME="${ADMIN_USERNAME:-admin}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-public}"
ADMIN_ROLE="${ADMIN_ROLE:-super_admin}"
ADMIN_URL="${ADMIN_URL:-http://127.0.0.1:18081}"

CORE_ENDPOINTS="127.0.0.1:7800,127.0.0.1:7801,127.0.0.1:7802"
RAFT_INITIAL_CONF="127.0.0.1:17800,127.0.0.1:17801,127.0.0.1:17802"
NODE_ENDPOINTS="core-1=127.0.0.1:7900,core-2=127.0.0.1:7901,core-3=127.0.0.1:7902,rep-1=127.0.0.1:7903,rep-2=127.0.0.1:7904"

DEPENDENCY_CP_FILE="$CLUSTER_ROOT/dependency-classpath.txt"
RUNTIME_CP_FILE="$CLUSTER_ROOT/runtime-classpath.txt"
NODES=(core-1 core-2 core-3 rep-1 rep-2)

resolve_maven_bin() {
    if [[ -n "${MAVEN_BIN:-}" ]]; then
        if [[ -x "$MAVEN_BIN" ]]; then
            echo "$MAVEN_BIN"
            return 0
        fi
        echo "[error] MAVEN_BIN is set but not executable: $MAVEN_BIN" >&2
        return 1
    fi

    if command -v mvn >/dev/null 2>&1; then
        command -v mvn
        return 0
    fi

    local -a candidates=(
        "$REPO_ROOT/mvnw"
        "/Users/liucaiwen/Documents/maven/apache-maven-3.9.9/bin/mvn"
        "/opt/homebrew/bin/mvn"
        "/usr/local/bin/mvn"
    )
    local candidate
    for candidate in "${candidates[@]}"; do
        if [[ -x "$candidate" ]]; then
            echo "$candidate"
            return 0
        fi
    done

    cat >&2 <<EOF
[error] Maven executable not found.
        You can fix this in one of these ways:
        1. export MAVEN_BIN=/absolute/path/to/mvn
        2. add mvn to PATH
        3. put ./mvnw in the repository root
EOF
    return 1
}

node_dir() {
    echo "$CLUSTER_ROOT/$1"
}

pid_file() {
    echo "$(node_dir "$1")/run/jmqx.pid"
}

stdout_log_file() {
    echo "$(node_dir "$1")/logs/stdout.log"
}

ensure_cluster_root() {
    mkdir -p "$CLUSTER_ROOT"
}

prepare_node_dir() {
    local node_name="$1"
    local dir
    dir="$(node_dir "$node_name")"
    mkdir -p "$dir"/data "$dir"/logs "$dir"/run
}

build_runtime_classpath_file() {
    local module_cp
    module_cp="$REPO_ROOT/jmqx-common/target/classes:$REPO_ROOT/jmqx-protocol/target/classes:$REPO_ROOT/jmqx-plugin/target/classes:$REPO_ROOT/jmqx-cluster/target/classes:$REPO_ROOT/jmqx-core/target/classes:$REPO_ROOT/jmqx-transport/target/classes:$REPO_ROOT/jmqx-app/target/classes"
    local dependency_cp=""
    if [[ -f "$DEPENDENCY_CP_FILE" ]]; then
        dependency_cp="$(tr -d '\n' < "$DEPENDENCY_CP_FILE")"
    fi
    if [[ -n "$dependency_cp" ]]; then
        printf '%s:%s\n' "$module_cp" "$dependency_cp" > "$RUNTIME_CP_FILE"
    else
        printf '%s\n' "$module_cp" > "$RUNTIME_CP_FILE"
    fi
}

build_project() {
    ensure_cluster_root
    local mvn_bin
    mvn_bin="$(resolve_maven_bin)"
    echo "[build] install jmqx-app and dependencies into local Maven repo"
    "$mvn_bin" -Dmaven.repo.local="$MAVEN_REPO_LOCAL" -pl jmqx-app -am -DskipTests install
    echo "[build] resolve runtime classpath"
    local raw_cp_file
    raw_cp_file="$CLUSTER_ROOT/runtime-classpath.raw"
    "$mvn_bin" -q -Dmaven.repo.local="$MAVEN_REPO_LOCAL" -pl jmqx-app -am -DskipTests -Dexec.classpathScope=runtime -Dexec.executable=echo -Dexec.args=%classpath exec:exec > "$raw_cp_file"
    awk 'NF { last = $0 } END { print last }' "$raw_cp_file" > "$DEPENDENCY_CP_FILE"
    rm -f "$raw_cp_file"
    build_runtime_classpath_file
}

ensure_runtime_classpath() {
    if [[ ! -f "$RUNTIME_CP_FILE" ]]; then
        build_project
    fi
}

clean_pid_if_dead() {
    local node_name="$1"
    local file
    file="$(pid_file "$node_name")"
    if [[ ! -f "$file" ]]; then
        return
    fi
    local pid
    pid="$(cat "$file")"
    if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
        rm -f "$file"
    fi
}

is_running() {
    local node_name="$1"
    clean_pid_if_dead "$node_name"
    [[ -f "$(pid_file "$node_name")" ]]
}

start_node() {
    local node_name="$1"
    local role="$2"
    local broker_port="$3"
    local ws_port="$4"
    local message_port="$5"
    local core_port="${6:-}"
    local raft_port="${7:-}"
    local panel_enabled="${8:-false}"

    ensure_runtime_classpath
    prepare_node_dir "$node_name"

    if is_running "$node_name"; then
        echo "[start] $node_name is already running (pid=$(cat "$(pid_file "$node_name")"))"
        return 0
    fi

    local node_home
    node_home="$(node_dir "$node_name")"

    local -a java_opts
    IFS=' ' read -r -a java_opts <<< "$JMQX_JAVA_OPTS"

    local -a cmd
    cmd=("$JAVA_BIN")
    cmd+=("${java_opts[@]}")
    cmd+=(
        "-Djmqx.node.id=$node_name"
        "-Djmqx.cluster.role=$role"
        "-Djmqx.cluster.coreEndpoints=$CORE_ENDPOINTS"
        "-Djmqx.cluster.message.bindHost=127.0.0.1"
        "-Djmqx.cluster.message.port=$message_port"
        "-Djmqx.cluster.nodeEndpoints=$NODE_ENDPOINTS"
        "-Djmqx.broker.host=127.0.0.1"
        "-Djmqx.broker.port=$broker_port"
        "-Djmqx.broker.websocket.enabled=true"
        "-Djmqx.broker.websocket.host=127.0.0.1"
        "-Djmqx.broker.websocket.port=$ws_port"
        "-Djmqx.retained.rocksdb.path=$node_home/data/retained-rocksdb"
        "-Djmqx.admin.enabled=true"
        "-Djmqx.admin.url=$ADMIN_URL"
        "-Djmqx.admin.clusterId=$CLUSTER_ID"
        "-Djmqx.admin.nodeIp=127.0.0.1"
        "-Djmqx.admin.auth.username=$ADMIN_USERNAME"
        "-Djmqx.admin.auth.password=$ADMIN_PASSWORD"
        "-Djmqx.admin.auth.role=$ADMIN_ROLE"
        "-Djmqx.admin.panel.enabled=$panel_enabled"
        "-Djmqx.admin.panel.host=127.0.0.1"
        "-Djmqx.admin.panel.port=18081"
        "-Djmqx.admin.panel.persistence.enabled=true"
        "-Djmqx.admin.panel.persistence.rocksdb.path=$node_home/data/admin-state-rocksdb"
    )

    if [[ "$role" == "core" ]]; then
        cmd+=(
            "-Djmqx.cluster.core.bindHost=127.0.0.1"
            "-Djmqx.cluster.core.port=$core_port"
            "-Djmqx.cluster.raft.serverId=127.0.0.1:$raft_port"
            "-Djmqx.cluster.raft.initialConf=$RAFT_INITIAL_CONF"
            "-Djmqx.cluster.raft.dataPath=$node_home/data/raft-metadata"
        )
    fi

    cmd+=(
        "-cp" "$(cat "$RUNTIME_CP_FILE")"
        "com.jmqx.JmqxApplication"
    )

    local log_file
    log_file="$(stdout_log_file "$node_name")"
    (
        cd "$node_home"
        nohup "${cmd[@]}" > "$log_file" 2>&1 &
        echo $! > "$(pid_file "$node_name")"
    )

    echo "[start] $node_name started"
    echo "        pid: $(cat "$(pid_file "$node_name")")"
    echo "        log: $log_file"
    echo "        workdir: $node_home"
}

run_node_foreground() {
    local node_name="$1"
    local role="$2"
    local broker_port="$3"
    local ws_port="$4"
    local message_port="$5"
    local core_port="${6:-}"
    local raft_port="${7:-}"
    local panel_enabled="${8:-false}"

    ensure_runtime_classpath
    prepare_node_dir "$node_name"

    local node_home
    node_home="$(node_dir "$node_name")"

    local -a java_opts
    IFS=' ' read -r -a java_opts <<< "$JMQX_JAVA_OPTS"

    local -a cmd
    cmd=("$JAVA_BIN")
    cmd+=("${java_opts[@]}")
    cmd+=(
        "-Djmqx.node.id=$node_name"
        "-Djmqx.cluster.role=$role"
        "-Djmqx.cluster.coreEndpoints=$CORE_ENDPOINTS"
        "-Djmqx.cluster.message.bindHost=127.0.0.1"
        "-Djmqx.cluster.message.port=$message_port"
        "-Djmqx.cluster.nodeEndpoints=$NODE_ENDPOINTS"
        "-Djmqx.broker.host=127.0.0.1"
        "-Djmqx.broker.port=$broker_port"
        "-Djmqx.broker.websocket.enabled=true"
        "-Djmqx.broker.websocket.host=127.0.0.1"
        "-Djmqx.broker.websocket.port=$ws_port"
        "-Djmqx.retained.rocksdb.path=$node_home/data/retained-rocksdb"
        "-Djmqx.admin.enabled=true"
        "-Djmqx.admin.url=$ADMIN_URL"
        "-Djmqx.admin.clusterId=$CLUSTER_ID"
        "-Djmqx.admin.nodeIp=127.0.0.1"
        "-Djmqx.admin.auth.username=$ADMIN_USERNAME"
        "-Djmqx.admin.auth.password=$ADMIN_PASSWORD"
        "-Djmqx.admin.auth.role=$ADMIN_ROLE"
        "-Djmqx.admin.panel.enabled=$panel_enabled"
        "-Djmqx.admin.panel.host=127.0.0.1"
        "-Djmqx.admin.panel.port=18081"
        "-Djmqx.admin.panel.persistence.enabled=true"
        "-Djmqx.admin.panel.persistence.rocksdb.path=$node_home/data/admin-state-rocksdb"
    )

    if [[ "$role" == "core" ]]; then
        cmd+=(
            "-Djmqx.cluster.core.bindHost=127.0.0.1"
            "-Djmqx.cluster.core.port=$core_port"
            "-Djmqx.cluster.raft.serverId=127.0.0.1:$raft_port"
            "-Djmqx.cluster.raft.initialConf=$RAFT_INITIAL_CONF"
            "-Djmqx.cluster.raft.dataPath=$node_home/data/raft-metadata"
        )
    fi

    cmd+=(
        "-cp" "$(cat "$RUNTIME_CP_FILE")"
        "com.jmqx.JmqxApplication"
    )

    cd "$node_home"
    exec "${cmd[@]}"
}

stop_node() {
    local node_name="$1"
    local file
    file="$(pid_file "$node_name")"
    clean_pid_if_dead "$node_name"
    if [[ ! -f "$file" ]]; then
        echo "[stop] $node_name is not running"
        return 0
    fi

    local pid
    pid="$(cat "$file")"
    echo "[stop] stopping $node_name (pid=$pid)"
    kill "$pid" 2>/dev/null || true

    local retries=20
    while kill -0 "$pid" 2>/dev/null; do
        retries=$((retries - 1))
        if [[ "$retries" -le 0 ]]; then
            echo "[stop] force killing $node_name (pid=$pid)"
            kill -9 "$pid" 2>/dev/null || true
            break
        fi
        sleep 1
    done

    rm -f "$file"
}

status_node() {
    local node_name="$1"
    clean_pid_if_dead "$node_name"
    local file
    file="$(pid_file "$node_name")"
    if [[ -f "$file" ]]; then
        echo "$node_name: running (pid=$(cat "$file"))"
    else
        echo "$node_name: stopped"
    fi
}
