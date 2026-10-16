#!/usr/bin/env bash
# Kubernetes resilience test: kind cluster + CloudNativePG (3 instances, synchronous replication)
# + qd via the Helm chart in its three roles: migrate Job (qd-migrate-<rev>),
# scheduler Deployment (qd-scheduler, 2 replicas) and server Deployment
# (qd, 3 replicas, behind Service qd). An in-cluster verifier Job
# drives standard and FIFO load through the Service while this script performs, in order:
#
#   1. kubectl rollout restart of server + scheduler (rolling restart)
#   2. helm upgrade with a new config  (new migrate Job + rolling upgrade; set QD_UPGRADE_FROM_TAG
#                                       to start from an older image and upgrade to the new one)
#   3. force-delete a server pod       (crash)
#   4. force-delete ALL scheduler pods (they come back; the API keeps serving meanwhile and the
#                                       servers see a fresh scheduler heartbeat again)
#   5. force-delete the Postgres primary pod (CloudNativePG fails over to the sync standby)
#   6. helm rollback                   (rollback)
#
# The verifier then drains and fails if any acknowledged message was lost or FIFO order broke.
#
# Requirements: docker (privileged), kind, kubectl, helm. Not part of CI (the runners have no
# Docker-in-Docker); run it manually, e.g. on a workstation before a release. Environment:
#   CNPG_VERSION (default 1.25.1)  KIND_CLUSTER (default qd-resilience)  KEEP_CLUSTER=1
#   QD_UPGRADE_FROM_TAG            image tag of a previous release to start from
#   VERIFY_SECONDS (default 420)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER="${KIND_CLUSTER:-qd-resilience}"
CNPG_VERSION="${CNPG_VERSION:-1.25.1}"
NS=qd
IMAGE=qd
NEW_TAG="test-$(date +%s)"
VERIFY_SECONDS="${VERIFY_SECONDS:-420}"
log() { printf '\033[1;36m[k8s %s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { log "FAILED: $*"; kubectl -n "$NS" get pods -o wide || true; exit 1; }

cleanup() {
  if [[ "${KEEP_CLUSTER:-0}" != 1 ]]; then kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

log "creating kind cluster $CLUSTER (1 control plane + 3 workers)"
cat <<KIND | kind create cluster --name "$CLUSTER" --wait 180s --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
KIND

log "installing CloudNativePG $CNPG_VERSION"
kubectl apply --server-side -f \
  "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-${CNPG_VERSION%.*}/releases/cnpg-${CNPG_VERSION}.yaml" >/dev/null
kubectl -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=180s

log "building images"
docker build -q -t "$IMAGE:$NEW_TAG" "$ROOT" >/dev/null
docker build -q -t qd-verifier:latest -f "$ROOT/e2e/k8s/Dockerfile.verifier" "$ROOT" >/dev/null
kind load docker-image --name "$CLUSTER" "$IMAGE:$NEW_TAG" qd-verifier:latest >/dev/null
START_TAG="$NEW_TAG"
if [[ -n "${QD_UPGRADE_FROM_TAG:-}" ]]; then
  docker pull -q "$QD_UPGRADE_FROM_TAG" >/dev/null || true
  docker tag "$QD_UPGRADE_FROM_TAG" "$IMAGE:previous"
  kind load docker-image --name "$CLUSTER" "$IMAGE:previous" >/dev/null
  START_TAG=previous
fi

kubectl create namespace "$NS" >/dev/null
helm_install() { # $1 = tag, rest = extra args
  local tag="$1"; shift
  helm upgrade --install qd "$ROOT/deploy/helm/qd" -n "$NS" --wait --timeout 10m \
    --set image.repository="$IMAGE" --set image.tag="$tag" --set image.pullPolicy=Never \
    --set replicaCount=3 --set scheduler.replicaCount=2 \
    --set postgresql.cnpg.enabled=true --set postgresql.cnpg.instances=3 "$@"
}
SERVER=deployment/qd
SCHEDULER=deployment/qd-scheduler
MIGRATE_SELECTOR=app.kubernetes.io/instance=qd,app.kubernetes.io/name=qd-migrate
SCHEDULER_SELECTOR=app.kubernetes.io/instance=qd,app.kubernetes.io/name=qd-scheduler
# The current revision's migrate Job (qd-migrate-<revision>) must complete; Helm
# deletes the previous revision's Job, so waiting on the label covers exactly that one.
wait_migrations() {
  kubectl -n "$NS" wait --for=condition=complete job -l "$MIGRATE_SELECTOR" --timeout=600s \
    || { kubectl -n "$NS" logs -l "$MIGRATE_SELECTOR" --tail=40 || true; die "migrate Job did not complete"; }
}
wait_rollouts() {
  kubectl -n "$NS" rollout status "$SCHEDULER" --timeout=300s
  kubectl -n "$NS" rollout status "$SERVER" --timeout=300s
}
# Age of the freshest scheduler heartbeat as reported by a server pod (via the API server's
# service proxy, so no port-forward or shell in the distroless image is needed).
heartbeat_age() {
  kubectl get --raw "/api/v1/namespaces/$NS/services/qd:admin/proxy/metrics" 2>/dev/null \
    | awk '/^qd_scheduler_heartbeat_age_seconds[ {]/ {print int($NF); exit}' || true
}
wait_heartbeat() { # fresh (< 30s) heartbeat within 180s
  local age=""
  for _ in $(seq 1 90); do
    age="$(heartbeat_age)"
    [[ -n "$age" && "$age" -lt 30 ]] && { log "scheduler heartbeat age ${age}s"; return 0; }
    sleep 2
  done
  die "no fresh scheduler heartbeat (last age: ${age:-absent})"
}

log "installing qd ($START_TAG) with a 3-instance CNPG cluster"
helm_install "$START_TAG"
wait_migrations
wait_rollouts
SVC="http://qd.$NS.svc.cluster.local:9324"
kubectl -n "$NS" wait --for=condition=Ready cluster.postgresql.cnpg.io --all --timeout=600s
wait_heartbeat

log "starting in-cluster verifier for ${VERIFY_SECONDS}s"
kubectl -n "$NS" apply -f - <<JOB
apiVersion: batch/v1
kind: Job
metadata: { name: verifier }
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: verify
          image: qd-verifier:latest
          imagePullPolicy: Never
          args: ["$SVC", "$VERIFY_SECONDS", "--allow-client-errors"]
JOB
kubectl -n "$NS" wait --for=condition=Ready pod -l job-name=verifier --timeout=120s
sleep 30

log "1/6 rolling restart (server + scheduler)"
kubectl -n "$NS" rollout restart "$SCHEDULER" "$SERVER"
wait_rollouts
sleep 20

log "2/6 rolling upgrade to $NEW_TAG (config change)"
helm_install "$NEW_TAG" --set config.janitorIntervalSecs=15
wait_migrations
wait_rollouts
sleep 20

log "3/6 crash a server pod"
POD=$(kubectl -n "$NS" get pods -l app.kubernetes.io/instance=qd,app.kubernetes.io/name=qd -o jsonpath='{.items[0].metadata.name}')
kubectl -n "$NS" delete pod "$POD" --grace-period=0 --force >/dev/null 2>&1
kubectl -n "$NS" rollout status "$SERVER" --timeout=300s
sleep 20

log "4/6 kill ALL scheduler pods (the API must keep serving)"
kubectl -n "$NS" delete pod -l "$SCHEDULER_SELECTOR" --grace-period=0 --force >/dev/null 2>&1
sleep 5
kubectl -n "$NS" rollout status "$SCHEDULER" --timeout=300s
wait_heartbeat
sleep 20

log "5/6 kill the Postgres primary (CNPG failover)"
PRIMARY=$(kubectl -n "$NS" get cluster.postgresql.cnpg.io -o jsonpath='{.items[0].status.currentPrimary}')
log "primary is $PRIMARY"
kubectl -n "$NS" delete pod "$PRIMARY" --grace-period=0 --force >/dev/null 2>&1
for _ in $(seq 1 120); do
  NEWP=$(kubectl -n "$NS" get cluster.postgresql.cnpg.io -o jsonpath='{.items[0].status.currentPrimary}')
  [[ -n "$NEWP" && "$NEWP" != "$PRIMARY" ]] && break
  sleep 2
done
[[ "$NEWP" != "$PRIMARY" ]] || die "CNPG did not fail over"
log "new primary is $NEWP"
kubectl -n "$NS" wait --for=condition=Ready cluster.postgresql.cnpg.io --all --timeout=600s
sleep 20

log "6/6 helm rollback"
helm rollback qd -n "$NS" --wait --timeout 10m
wait_migrations
wait_rollouts

log "waiting for the verifier to drain and check"
kubectl -n "$NS" wait --for=condition=complete job/verifier --timeout="$((VERIFY_SECONDS + 600))s" || {
  kubectl -n "$NS" logs job/verifier | tail -40
  die "verifier failed"
}
kubectl -n "$NS" logs job/verifier | tail -10
kubectl -n "$NS" logs job/verifier | grep -q "VERIFY PASSED" || die "verifier did not pass"
wait_heartbeat
log "PASSED: no acknowledged message lost and FIFO order held across restart, upgrade, crash, scheduler loss, DB failover and rollback"
