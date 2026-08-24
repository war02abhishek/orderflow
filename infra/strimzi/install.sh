#!/usr/bin/env bash
# Installs the Strimzi cluster operator (which then manages Kafka itself)
# into the `kafka` namespace. Strimzi 1.x is KRaft-only -- no ZooKeeper --
# and Kafka clusters are declared as KafkaNodePool + Kafka custom resources,
# applied separately after this operator is running (deploy/kafka/).
set -euo pipefail

STRIMZI_VERSION='1.2.0'
NAMESPACE='kafka'

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

TMP_MANIFEST="$(mktemp)"
curl -sL "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml" \
  -o "${TMP_MANIFEST}"
sed -i.bak "s/namespace: myproject/namespace: ${NAMESPACE}/g" "${TMP_MANIFEST}"

kubectl apply -f "${TMP_MANIFEST}" -n "${NAMESPACE}"
rm -f "${TMP_MANIFEST}" "${TMP_MANIFEST}.bak"

echo "waiting for the Strimzi cluster operator to roll out..."
kubectl -n "${NAMESPACE}" rollout status deployment/strimzi-cluster-operator --timeout=180s
