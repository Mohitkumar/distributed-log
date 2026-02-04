#!/usr/bin/env bash
# Stop the local 3-node cluster started by scripts/start-local-cluster.sh.

set -e
cd "$(dirname "$0")/.."
DATA_ROOT="${DATA_ROOT:-/tmp/mlog-local}"
PID_FILE="${PID_FILE:-$DATA_ROOT/pids}"

PORTS=(9092 9093 9094 9095 9096 9097 9098 9099 9100)

kill_by_ports() {
  if ! command -v lsof >/dev/null 2>&1; then
    echo "lsof not found; can't kill by ports."
    return 1
  fi
  echo "Killing listeners on cluster ports via lsof..."
  for p in "${PORTS[@]}"; do
    # macOS: -ti gives PIDs; LISTEN only.
    pids="$(lsof -n -P -tiTCP:"$p" -sTCP:LISTEN 2>/dev/null || true)"
    if [[ -n "$pids" ]]; then
      for pid in $pids; do
        echo "  SIGKILL $pid (port $p)"
        kill -KILL "$pid" 2>/dev/null || true
      done
    fi
  done
}

if [[ ! -f "$PID_FILE" ]]; then
  echo "No PID file at $PID_FILE; falling back to killing by ports."
  kill_by_ports || true
  exit 0
fi

echo "Stopping local cluster..."
PIDS=()
while IFS= read -r pid; do
  [[ -z "$pid" ]] && continue
  PIDS+=("$pid")
done < "$PID_FILE"

if [[ ${#PIDS[@]} -eq 0 ]]; then
  echo "PID file is empty; nothing to stop."
  rm -f "$PID_FILE"
  exit 0
fi

# First try graceful shutdown.
for pid in "${PIDS[@]}"; do
  [[ -z "$pid" ]] && continue
  if kill -0 "$pid" 2>/dev/null; then
    echo "  SIGTERM $pid"
    kill -TERM "$pid" 2>/dev/null || true
  fi
done

# Wait up to ~10s for exit.
for _ in {1..20}; do
  alive=0
  for pid in "${PIDS[@]}"; do
    [[ -z "$pid" ]] && continue
    if kill -0 "$pid" 2>/dev/null; then
      alive=1
    fi
  done
  [[ "$alive" -eq 0 ]] && break
  sleep 0.5
done

# Force kill anything still running.
for pid in "${PIDS[@]}"; do
  [[ -z "$pid" ]] && continue
  if kill -0 "$pid" 2>/dev/null; then
    echo "  SIGKILL $pid"
    kill -KILL "$pid" 2>/dev/null || true
  fi
done

rm -f "$PID_FILE"
echo "Done."

# Safety net: if anything is still listening on the expected ports, kill it.
kill_by_ports || true
