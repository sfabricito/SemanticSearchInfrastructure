#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATABASES_CHART="$SCRIPT_DIR/databases"
BATCH_CHART="$SCRIPT_DIR/batch"
APPLICATION_CHART="$SCRIPT_DIR/application"

if ! helm repo list | tail -n +2 | awk '{print $1}' | grep -qx spark-operator; then
  helm repo add spark-operator https://kubeflow.github.io/spark-operator
fi

# if ! helm repo list | tail -n +2 | awk '{print $1}' | grep -qx qdrant; then
#   helm repo add qdrant https://qdrant.github.io/qdrant-helm
# fi
# if ! helm repo list | tail -n +2 | awk '{print $1}' | grep -qx bitnami; then
#   helm repo add bitnami https://charts.bitnami.com/bitnami
# fi

helm repo update
helm dependency update "$DATABASES_CHART"
helm dependency update "$BATCH_CHART"

# helm upgrade --install databases "$DATABASES_CHART"
helm upgrade --install batch "$BATCH_CHART"
# helm upgrade --install application "$APPLICATION_CHART"
