#!/usr/bin/env bash
# Copy the observability sources of truth into the Helm chart (files/ is read via .Files.Get).
# deploy/observability/check.sh fails if the copies drift.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chart="$here/../helm/qd"
mkdir -p "$chart/files"
cp "$here/prometheus-rules.yaml" "$chart/files/prometheus-rules.yaml"
cp "$here/grafana-dashboard.json" "$chart/files/grafana-dashboard.json"
echo "synced prometheus-rules.yaml and grafana-dashboard.json into $chart/files/"
