#!/usr/bin/env bash
# Manage Arni development database containers via Podman.
#
# Each container is named arni-dev-<service> and uses credentials from
# scripts/init-*.sql / scripts/init-*.js so integration tests can connect
# without extra configuration.
#
# Usage:
#   ./scripts/dev-containers.sh start [SERVICE...]   # start all or named services
#   ./scripts/dev-containers.sh stop  [SERVICE...]   # stop all or named services
#   ./scripts/dev-containers.sh rm    [SERVICE...]   # remove all or named containers
#   ./scripts/dev-containers.sh status               # show running state of all containers
#   ./scripts/dev-containers.sh logs  <SERVICE>      # tail logs for a service
#
# Supported services: postgres  mysql  mssql  mongodb  oracle

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ARNI_DATA_DIR="${HOME}/.arni/data"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

ALL_SERVICES=(postgres mysql mssql mongodb oracle)

# ── Helpers ───────────────────────────────────────────────────────────────────

msg()  { echo -e "${GREEN}[arni-containers]${NC} $*"; }
warn() { echo -e "${YELLOW}[arni-containers]${NC} $*"; }
err()  { echo -e "${RED}[arni-containers]${NC} $*" >&2; }

require_podman() {
    if ! command -v podman &>/dev/null; then
        err "podman is not installed or not on PATH."
        err "Install: https://podman.io/getting-started/installation"
        exit 1
    fi
}

# Ensure persistent data directories exist
ensure_data_dirs() {
    for svc in "${ALL_SERVICES[@]}"; do
        mkdir -p "${ARNI_DATA_DIR}/${svc}"
    done
}

# Return 0 if a container exists (running or stopped), 1 otherwise
container_exists() {
    local name="$1"
    podman container exists "${name}" 2>/dev/null
}

# Return 0 if a container is currently running
container_running() {
    local name="$1"
    local state
    state=$(podman inspect --format '{{.State.Status}}' "${name}" 2>/dev/null || echo "missing")
    [[ "$state" == "running" ]]
}

# ── Per-service start functions ───────────────────────────────────────────────

start_postgres() {
    local name="arni-dev-postgres"
    if container_running "${name}"; then
        msg "postgres  already running (${name})"
        return 0
    fi
    if container_exists "${name}"; then
        msg "postgres  starting stopped container..."
        podman start "${name}"
        return 0
    fi
    msg "postgres  creating container..."
    podman run -d \
        --name "${name}" \
        -e POSTGRES_USER=test_user \
        -e POSTGRES_PASSWORD=test_password \
        -e POSTGRES_DB=test_db \
        -p 5432:5432 \
        -v "${ARNI_DATA_DIR}/postgres:/var/lib/postgresql/data" \
        -v "${REPO_ROOT}/scripts/init-postgres.sql:/docker-entrypoint-initdb.d/init.sql:ro" \
        docker.io/library/postgres:16-alpine
    msg "postgres  ready on :5432"
}

start_mysql() {
    local name="arni-dev-mysql"
    if container_running "${name}"; then
        msg "mysql     already running (${name})"
        return 0
    fi
    if container_exists "${name}"; then
        msg "mysql     starting stopped container..."
        podman start "${name}"
        return 0
    fi
    msg "mysql     creating container..."
    podman run -d \
        --name "${name}" \
        -e MYSQL_USER=test_user \
        -e MYSQL_PASSWORD=test_password \
        -e MYSQL_DATABASE=test_db \
        -e MYSQL_ROOT_PASSWORD=root_password \
        -p 3306:3306 \
        -v "${ARNI_DATA_DIR}/mysql:/var/lib/mysql" \
        -v "${REPO_ROOT}/scripts/init-mysql.sql:/docker-entrypoint-initdb.d/init.sql:ro" \
        docker.io/library/mysql:8.0
    msg "mysql     ready on :3306"
}

start_mssql() {
    local name="arni-dev-mssql"
    if container_running "${name}"; then
        msg "mssql     already running (${name})"
        return 0
    fi
    if container_exists "${name}"; then
        msg "mssql     starting stopped container..."
        podman start "${name}"
        return 0
    fi
    msg "mssql     creating container..."
    podman run -d \
        --name "${name}" \
        -e ACCEPT_EULA=Y \
        -e MSSQL_SA_PASSWORD=Test_password1! \
        -e MSSQL_PID=Developer \
        -p 1434:1433 \
        -v "${ARNI_DATA_DIR}/mssql:/var/opt/mssql" \
        mcr.microsoft.com/azure-sql-edge:latest
    msg "mssql     ready on :1434"
}

start_mongodb() {
    local name="arni-dev-mongodb"
    if container_running "${name}"; then
        msg "mongodb   already running (${name})"
        return 0
    fi
    if container_exists "${name}"; then
        msg "mongodb   starting stopped container..."
        podman start "${name}"
        return 0
    fi
    msg "mongodb   creating container..."
    podman run -d \
        --name "${name}" \
        -e MONGO_INITDB_ROOT_USERNAME=test_user \
        -e MONGO_INITDB_ROOT_PASSWORD=test_password \
        -e MONGO_INITDB_DATABASE=test_db \
        -p 27018:27017 \
        -v "${ARNI_DATA_DIR}/mongodb:/data/db" \
        -v "${REPO_ROOT}/scripts/init-mongodb.js:/docker-entrypoint-initdb.d/init.js:ro" \
        docker.io/library/mongo:7
    msg "mongodb   ready on :27018"
}

start_oracle() {
    local name="arni-dev-oracle"
    if container_running "${name}"; then
        msg "oracle    already running (${name})"
        return 0
    fi
    if container_exists "${name}"; then
        msg "oracle    starting stopped container..."
        podman start "${name}"
        return 0
    fi
    msg "oracle    creating container (first-start may take several minutes)..."
    podman run -d \
        --name "${name}" \
        -e ORACLE_PWD=Test_password1 \
        -p 1522:1521 \
        -p 5501:5500 \
        -v "${ARNI_DATA_DIR}/oracle:/opt/oracle/oradata" \
        -v "${REPO_ROOT}/scripts/init-oracle.sql:/opt/oracle/scripts/startup/init.sql:ro" \
        container-registry.oracle.com/database/free:latest
    msg "oracle    ready on :1522"
}

# ── Command dispatch ──────────────────────────────────────────────────────────

cmd_start() {
    local services=("$@")
    [[ ${#services[@]} -eq 0 ]] && services=("${ALL_SERVICES[@]}")

    ensure_data_dirs

    for svc in "${services[@]}"; do
        case "${svc}" in
            postgres) start_postgres ;;
            mysql)    start_mysql    ;;
            mssql)    start_mssql   ;;
            mongodb)  start_mongodb  ;;
            oracle)   start_oracle   ;;
            *) err "Unknown service: ${svc}. Valid: ${ALL_SERVICES[*]}"; exit 1 ;;
        esac
    done
}

cmd_stop() {
    local services=("$@")
    [[ ${#services[@]} -eq 0 ]] && services=("${ALL_SERVICES[@]}")

    for svc in "${services[@]}"; do
        local name="arni-dev-${svc}"
        if container_running "${name}"; then
            msg "${svc}  stopping..."
            podman stop "${name}"
        else
            warn "${svc}  not running (${name})"
        fi
    done
}

cmd_rm() {
    local services=("$@")
    [[ ${#services[@]} -eq 0 ]] && services=("${ALL_SERVICES[@]}")

    for svc in "${services[@]}"; do
        local name="arni-dev-${svc}"
        if container_exists "${name}"; then
            msg "${svc}  removing ${name}..."
            podman rm -f "${name}"
        else
            warn "${svc}  container ${name} does not exist"
        fi
    done
}

cmd_status() {
    printf "%-12s  %-30s  %s\n" "SERVICE" "CONTAINER" "STATUS"
    printf "%-12s  %-30s  %s\n" "-------" "---------" "------"
    for svc in "${ALL_SERVICES[@]}"; do
        local name="arni-dev-${svc}"
        local status
        status=$(podman inspect --format '{{.State.Status}}' "${name}" 2>/dev/null || echo "absent")
        printf "%-12s  %-30s  %s\n" "${svc}" "${name}" "${status}"
    done
}

cmd_logs() {
    local svc="${1:-}"
    if [[ -z "${svc}" ]]; then
        err "Usage: $0 logs <SERVICE>"
        exit 1
    fi
    local name="arni-dev-${svc}"
    if ! container_exists "${name}"; then
        err "Container ${name} does not exist"
        exit 1
    fi
    podman logs -f "${name}"
}

usage() {
    cat <<EOF
Usage: $(basename "$0") <COMMAND> [SERVICE...]

Manage Arni development database containers via Podman.

COMMANDS
  start  [SERVICE...]   Start containers (default: all)
  stop   [SERVICE...]   Stop containers (default: all)
  rm     [SERVICE...]   Remove containers (default: all)
  status                Show status of all containers
  logs   <SERVICE>      Tail logs for a service

SERVICES
  postgres  MySQL  mssql  mongodb  oracle

EXAMPLES
  $(basename "$0") start                  # start all services
  $(basename "$0") start postgres mysql   # start only postgres and mysql
  $(basename "$0") stop postgres          # stop only postgres
  $(basename "$0") status                 # show all container states
  $(basename "$0") logs mysql             # tail mysql logs

PORTS
  postgres  :5432    mysql    :3306
  mssql     :1434    mongodb  :27018
  oracle    :1522

DATA
  Persistent data lives under ~/.arni/data/<service>/
  Re-creating a container reuses existing data volumes.

EOF
    exit 0
}

# ── Main ──────────────────────────────────────────────────────────────────────

require_podman

COMMAND="${1:-}"
shift || true

case "${COMMAND}" in
    start)  cmd_start  "$@" ;;
    stop)   cmd_stop   "$@" ;;
    rm)     cmd_rm     "$@" ;;
    status) cmd_status      ;;
    logs)   cmd_logs   "$@" ;;
    -h|--help|help|"") usage ;;
    *) err "Unknown command: ${COMMAND}"; usage ;;
esac
