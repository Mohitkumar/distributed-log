#!/usr/bin/env bash
# Start a 3-node mlog cluster locally. Run from repo root.
# Nodes listen on 127.0.0.1 with distinct ports; node1 bootstraps, node2/node3 join via Serf.
# Stop with: scripts/stop-local-cluster.sh

set -e
cd "$(dirname "$0")/.."
BIN="${BIN:-./bin/server}"
DATA_ROOT="${DATA_ROOT:-/tmp/mlog-local}"
PID_FILE="${PID_FILE:-$DATA_ROOT/pids}"

mkdir -p "$DATA_ROOT"
mkdir -p "$(dirname "$PID_FILE")"
rm -f "$PID_FILE"
touch "$PID_FILE"
if [[ ! -x "$BIN" ]]; then
  echo "Building server (make build-server)..."
  make build-server
  BIN=./bin/server
fi

# Clean old data for a fresh cluster (optional; comment out to keep state)
# rm -rf "$DATA_ROOT/node1" "$DATA_ROOT/node2" "$DATA_ROOT/node3"
mkdir -p "$DATA_ROOT/node1" "$DATA_ROOT/node2" "$DATA_ROOT/node3"

echo "Starting node1 (Serf 9092, Raft 9093, RPC 9094)..."
"$BIN" server \
  --bind-addr 127.0.0.1:9092 \
  --raft-addr 127.0.0.1:9093 \
  --rpc-port 9094 \
  --data-dir "$DATA_ROOT/node1" \
  --node-id node-1 \
  --bootstrap true &
echo $! >> "$PID_FILE"
sleep 3

echo "Starting node2 (Serf 9095, Raft 9096, RPC 9097)..."
"$BIN" server \
  --bind-addr 127.0.0.1:9095 \
  --raft-addr 127.0.0.1:9096 \
  --rpc-port 9097 \
  --data-dir "$DATA_ROOT/node2" \
  --node-id node-2 \
  --peer "node-1=127.0.0.1:9092" &
echo $! >> "$PID_FILE"
sleep 1

echo "Starting node3 (Serf 9098, Raft 9099, RPC 9100)..."
"$BIN" server \
  --bind-addr 127.0.0.1:9098 \
  --raft-addr 127.0.0.1:9099 \
  --rpc-port 9100 \
  --data-dir "$DATA_ROOT/node3" \
  --node-id node-3 \
  --peer "node-1=127.0.0.1:9092" &
echo $! >> "$PID_FILE"

echo ""
echo "Local 3-node cluster started. RPC endpoints:"
echo "  node1: 127.0.0.1:9094"
echo "  node2: 127.0.0.1:9097"
echo "  node3: 127.0.0.1:9100"
echo ""
echo "To stop: scripts/stop-local-cluster.sh"
echo "PIDs saved to: $PID_FILE"
