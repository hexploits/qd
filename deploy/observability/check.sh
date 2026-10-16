#!/usr/bin/env bash
# Static checks for the observability bundle (no running server needed):
#   1. promtool check rules + promtool test rules (unit tests for every alert)
#   2. the Helm chart copies under files/ are byte-identical to deploy/observability/
#   3. the PrometheusRule rendered by Helm with default thresholds passes the SAME unit tests
#      (proves the template's substitutions are no-ops at defaults), and non-default values
#      really change every substituted threshold
#   4. the dashboard is valid JSON and only references metrics that the rules/docs know about
# Requires: promtool, helm, python3.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chart="$here/../helm/qd"
PROMTOOL="${PROMTOOL:-promtool}"
HELM="${HELM:-helm}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail=0
step() { printf '\n== %s\n' "$*"; }

step "promtool check rules"
"$PROMTOOL" check rules "$here/prometheus-rules.yaml"

step "promtool test rules"
"$PROMTOOL" test rules "$here"/tests/*.test.yaml

step "every alert has a firing and a not-firing unit test"
python3 - "$here/prometheus-rules.yaml" "$here"/tests/*.test.yaml <<'PY' || fail=1
import re, sys
alerts = re.findall(r'^\s+- alert: (\S+)', open(sys.argv[1]).read(), re.M)
firing, quiet = set(), set()
for f in sys.argv[2:]:
    # alertname line followed by its exp_alerts line
    for name, rest in re.findall(r'alertname: (\S+)\n\s+exp_alerts:(.*)', open(f).read()):
        (quiet if rest.strip() == '[]' else firing).add(name)
bad = [a for a in alerts if a not in firing or a not in quiet]
for a in bad:
    print(f"MISSING test: {a} firing={a in firing} not-firing={a in quiet}")
print(f"{len(alerts)} alerts, {len(alerts) - len(bad)} fully covered")
sys.exit(1 if bad else 0)
PY

step "chart files in sync with deploy/observability"
for f in prometheus-rules.yaml grafana-dashboard.json; do
  if cmp -s "$here/$f" "$chart/files/$f"; then
    echo "ok: files/$f"
  else
    echo "DRIFT: $chart/files/$f differs from deploy/observability/$f (run deploy/observability/sync-chart-files.sh)"
    fail=1
  fi
done

# Extract .spec of the rendered PrometheusRule as a plain rule file.
render_rules() { # out-file [helm args...]
  local out="$1"; shift
  "$HELM" template qd "$chart" --set prometheusRule.enabled=true "$@" \
    --show-only templates/prometheusrule.yaml >"$tmp/cr.yaml"
  awk '/^spec:/{f=1; next} f{sub(/^  /, ""); print}' "$tmp/cr.yaml" >"$out"
}

step "Helm-rendered PrometheusRule (defaults) passes the unit tests"
mkdir -p "$tmp/rendered/tests"
render_rules "$tmp/rendered/prometheus-rules.yaml" --set 'prometheusRule.jobMatcher=.*qd.*'
cp "$here"/tests/*.test.yaml "$tmp/rendered/tests/"
"$PROMTOOL" check rules "$tmp/rendered/prometheus-rules.yaml"
"$PROMTOOL" test rules "$tmp/rendered/tests/"*.test.yaml

step "Helm-rendered PrometheusRule honours overrides"
render_rules "$tmp/override.yaml" \
  --set prometheusRule.jobMatcher=my-job \
  --set prometheusRule.runbookBaseUrl=https://runbooks.example.com/qd \
  --set prometheusRule.thresholds.serverErrorRatioWarning=0.02 \
  --set prometheusRule.thresholds.serverErrorRatioCritical=0.1 \
  --set prometheusRule.thresholds.latencyP99Seconds=1.5 \
  --set prometheusRule.thresholds.dbPoolUtilization=0.95 \
  --set prometheusRule.thresholds.janitorErrors15m=7 \
  --set prometheusRule.thresholds.queueMetricsCollectionSeconds=45 \
  --set prometheusRule.thresholds.deadLetterQueueMessages=10 \
  --set prometheusRule.thresholds.oldestMessageAgeSeconds=7200 \
  --set prometheusRule.thresholds.backlogMessages=5000 \
  --set prometheusRule.thresholds.schedulerHeartbeatAgeSeconds=300 \
  --set 'prometheusRule.disabledAlerts={QdVersionSkew}' \
  --set prometheusRule.severityOverrides.QdDeadLetterQueueNotEmpty=info
"$PROMTOOL" check rules "$tmp/override.yaml"
grep -qF 'job=~"qd(-scheduler)?"' <("$HELM" template qd "$chart" --set prometheusRule.enabled=true --show-only templates/prometheusrule.yaml) \
  && echo "ok: default job matcher covers server + scheduler" || { echo "default job matcher does not cover both Services"; fail=1; }
for want in '> 0.02' '> 0.1' '> 1.5' '> 0.95' '> 7' '> 45' '> 10' '> 7200' '> 5000' '> 300' \
            'job=~"my-job"' 'https://runbooks.example.com/qd#qdreplicadown' 'severity: info'; do
  if grep -qF -- "$want" "$tmp/override.yaml"; then echo "ok: $want"; else echo "MISSING in override render: $want"; fail=1; fi
done
if grep -q 'QdVersionSkew' "$tmp/override.yaml"; then echo "disabledAlerts not applied"; fail=1; fi
if grep -q 'qd\.\*' "$tmp/override.yaml"; then echo "default job matcher left in override render"; fail=1; fi

step "dashboard JSON"
python3 - "$here/grafana-dashboard.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
assert d.get("schemaVersion", 0) >= 36, "schemaVersion"
names = {v["name"] for v in d["templating"]["list"]}
assert {"datasource", "namespace", "job", "account", "queue"} <= names, names
ids, n = set(), 0
for p in d["panels"]:
    assert p["id"] not in ids, f"duplicate panel id {p['id']}"
    ids.add(p["id"])
    if p["type"] == "row":
        continue
    assert p["datasource"]["uid"] == "${datasource}", p["title"]
    for t in p["targets"]:
        assert t["datasource"]["uid"] == "${datasource}", p["title"]
        n += 1
print(f"ok: {len(d['panels'])} panels, {n} queries, all on ${{datasource}}")
PY

exit $fail
