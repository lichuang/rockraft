#!/bin/bash
#
# entrypoint.sh - Kubernetes entrypoint for rockraft cluster nodes
#
# This script generates the appropriate TOML configuration based on the
# StatefulSet pod ordinal and starts the cluster_example binary.
#
# Pod 0 (node_id=1): Initializes the cluster (join=[])
# Pod 1 (node_id=2): Waits for pod 0, then joins the cluster
# Pod 2 (node_id=3): Waits for pod 0, then joins the cluster
#
set -e

# Extract ordinal from hostname: rockraft-0 -> 0, rockraft-1 -> 1, etc.
ORDINAL=${HOSTNAME##*-}
NODE_ID=$((ORDINAL + 1))

RAFT_PORT=7000
HTTP_PORT=8000

# Kubernetes DNS configuration
NAMESPACE=${NAMESPACE:-rockraft}
SERVICE_NAME=${SERVICE_NAME:-rockraft-headless}
DNS_BASE="${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"

# Pod count (defaults to 3)
POD_COUNT=${POD_COUNT:-3}

MY_HOSTNAME="rockraft-${ORDINAL}.${DNS_BASE}"

echo "=== rockraft cluster node starting ==="
echo "  Hostname:    ${HOSTNAME}"
echo "  Ordinal:     ${ORDINAL}"
echo "  Node ID:     ${NODE_ID}"
echo "  Namespace:   ${NAMESPACE}"
echo "  DNS Base:    ${DNS_BASE}"

# Build the join list (all pod DNS addresses)
JOIN_LIST=""
for i in $(seq 0 $((POD_COUNT - 1))); do
  if [ -n "$JOIN_LIST" ]; then
    JOIN_LIST="${JOIN_LIST}, "
  fi
  JOIN_LIST="${JOIN_LIST}\"rockraft-${i}.${DNS_BASE}:${RAFT_PORT}\""
done

# Generate configuration file
CONF_FILE="/tmp/node.toml"

if [ "$ORDINAL" = "0" ]; then
  echo "  Role:        INITIALIZER (will create cluster)"

  # Node 0 initializes the cluster with an empty join list
  cat > "$CONF_FILE" <<EOF
node_id = ${NODE_ID}
http_addr = "0.0.0.0:${HTTP_PORT}"

[raft]
address = "0.0.0.0:${RAFT_PORT}"
advertise_host = "${MY_HOSTNAME}"
join = []

[rocksdb]
data_path = "/data"
max_open_files = 10000

[log]
level = "info"
EOF

else
  echo "  Role:        JOINER (will join existing cluster)"

  # Wait for pod 0 to be ready before attempting to join
  echo "Waiting for rockraft-0.${DNS_BASE} to be ready..."
  MAX_WAIT=120
  WAITED=0
  until curl -sf "http://rockraft-0.${DNS_BASE}:${HTTP_PORT}/health" > /dev/null 2>&1; do
    sleep 2
    WAITED=$((WAITED + 2))
    if [ $WAITED -ge $MAX_WAIT ]; then
      echo "ERROR: Timed out waiting for rockraft-0 after ${MAX_WAIT}s"
      exit 1
    fi
  done
  echo "rockraft-0 is ready! (waited ${WAITED}s)"

  cat > "$CONF_FILE" <<EOF
node_id = ${NODE_ID}
http_addr = "0.0.0.0:${HTTP_PORT}"

[raft]
address = "0.0.0.0:${RAFT_PORT}"
advertise_host = "${MY_HOSTNAME}"
join = [${JOIN_LIST}]

[rocksdb]
data_path = "/data"
max_open_files = 10000

[log]
level = "info"
EOF

fi

echo ""
echo "=== Generated configuration ==="
cat "$CONF_FILE"
echo ""
echo "=== Starting cluster_example ==="

exec /usr/local/bin/cluster_example --conf "$CONF_FILE"
