#!/usr/bin/env bash
# Stands up the OrderFlow kind cluster with a local image registry wired
# into the kind network, following kind's official local-registry pattern:
# https://kind.sigs.k8s.io/docs/user/local-registry/
#
# Idempotent: safe to re-run if the cluster or registry already exist.
set -euo pipefail

REG_NAME='kind-registry'
REG_PORT='5001'
CLUSTER_NAME='orderflow'
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 1. Start the local registry container if it isn't already running.
if [ "$(docker inspect -f '{{.State.Running}}' "${REG_NAME}" 2>/dev/null || true)" != 'true' ]; then
  docker run -d --restart=always -p "127.0.0.1:${REG_PORT}:5000" --network bridge --name "${REG_NAME}" registry:2
  echo "started ${REG_NAME} on 127.0.0.1:${REG_PORT}"
else
  echo "${REG_NAME} already running"
fi

# 2. Create the kind cluster (config wires containerd to mirror through the registry,
#    and disables the default CNI so Calico can enforce NetworkPolicy for real).
CALICO_VERSION='v3.32.1'
if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  kind create cluster --config "${SCRIPT_DIR}/../kind-cluster.yaml"
  echo "installing Calico ${CALICO_VERSION} (kindnetd doesn't enforce NetworkPolicy)"
  kubectl apply -f "https://raw.githubusercontent.com/projectcalico/calico/${CALICO_VERSION}/manifests/calico.yaml"
  echo "waiting for Calico to roll out..."
  kubectl -n kube-system rollout status daemonset/calico-node --timeout=180s
  kubectl -n kube-system rollout status deployment/calico-kube-controllers --timeout=180s
else
  echo "kind cluster '${CLUSTER_NAME}' already exists"
fi

# 3. Connect the registry to the kind network so nodes can reach it by name.
if [ "$(docker inspect -f='{{json .NetworkSettings.Networks.kind}}' "${REG_NAME}")" = 'null' ]; then
  docker network connect kind "${REG_NAME}"
  echo "connected ${REG_NAME} to the kind network"
else
  echo "${REG_NAME} already on the kind network"
fi

# 4. Document the registry for cluster tooling that looks for it
#    (kubectl, Tilt, etc. read this convention).
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${REG_PORT}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF

echo
echo "Cluster '${CLUSTER_NAME}' ready. Push images to localhost:${REG_PORT}/<name>:<tag>,"
echo "then reference them in-cluster as localhost:${REG_PORT}/<name>:<tag>."
