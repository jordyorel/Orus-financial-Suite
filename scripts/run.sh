#!/usr/bin/env bash
# Orus Suite — supervision script (no Docker required).
#
# Usage:
#   zig build -Doptimize=ReleaseSafe   # build release binaries (required before first run)
#   cp .env.example .env && vi .env    # fill in tokens and hosts
#   ./scripts/run.sh                   # start all services
#
# Stop:  Ctrl-C  or  kill $(cat /tmp/orus.pid)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
BIN_DIR="$ROOT_DIR/zig-out/bin"
LOG_DIR="${ORUS_LOG_DIR:-$ROOT_DIR/logs}"

mkdir -p "$LOG_DIR"

# ── Load .env ─────────────────────────────────────────────────────────────────
if [ -f "$ROOT_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$ROOT_DIR/.env"
    set +a
    echo "[run.sh] loaded $ROOT_DIR/.env"
fi

# ── Helpers ───────────────────────────────────────────────────────────────────
log() { echo "[$(date -u +%FT%TZ)] $*"; }

# Wait until a TCP port is accepting connections (timeout = 10s).
wait_for_port() {
    local name="$1" port="$2" i
    for i in $(seq 1 20); do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then
            log "[$name] port $port ready"
            return 0
        fi
        sleep 0.5
    done
    log "[$name] WARNING: port $port not ready after 10s"
    return 1
}

# health_check <name> <health_port>
health_check() {
    local name="$1" port="$2"
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$port/" 2>/dev/null || echo "000")
    if [ "$code" = "200" ]; then
        log "[$name] health OK (HTTP $code)"
    else
        log "[$name] health FAIL (HTTP $code)"
    fi
}

# Restart a binary forever with exponential backoff.
# Logs are appended to $LOG_DIR/<name>.log.
restart_forever() {
    local name="$1" bin="$BIN_DIR/$1"
    local backoff=1

    if [ ! -x "$bin" ]; then
        log "[$name] ERROR: binary not found at $bin — run 'zig build' first"
        exit 1
    fi

    while true; do
        log "[$name] starting"
        "$bin" >> "$LOG_DIR/$name.log" 2>&1
        local code=$?
        log "[$name] exited (code=$code), restarting in ${backoff}s"
        sleep "$backoff"
        backoff=$(( backoff < 30 ? backoff * 2 : 30 ))
    done
}

# ── Stop handler ──────────────────────────────────────────────────────────────
stop_all() {
    log "Stopping Orus Suite (SIGTERM received)..."
    # Kill the entire process group spawned by this script.
    kill -- -$$ 2>/dev/null || true
    wait 2>/dev/null || true
    log "All services stopped."
    exit 0
}
trap stop_all SIGTERM SIGINT

# ── Save PID for external stop ────────────────────────────────────────────────
echo $$ > /tmp/orus.pid
log "Orus Suite starting (supervisor pid=$$). Logs: $LOG_DIR"
log "Stop with: kill \$(cat /tmp/orus.pid)"

# ── Start services ────────────────────────────────────────────────────────────
# Broker must be up before gateway and connect try to connect.
restart_forever orusbroker &
wait_for_port orusbroker "${BROKER_PORT:-7770}" || true
health_check   orusbroker "${BROKER_HEALTH_PORT:-7771}"

restart_forever orusgateway &
restart_forever orusconnect &

log "All services started."

# Optional: periodic health log every 60s.
while true; do
    sleep 60
    health_check orusbroker  "${BROKER_HEALTH_PORT:-7771}"
    health_check orusgateway "${GATEWAY_HEALTH_PORT:-7781}"
    health_check orusconnect "${CONNECT_LISTEN_PORT:-8080}"
done &

wait
