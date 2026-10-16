#!/usr/bin/env bash
# AWS CLI v2 smoke test for any SQS-compatible endpoint.
#
#   QD_E2E_ENDPOINT=http://127.0.0.1:9324 e2e/cli/smoke.sh
#   QD_E2E_TARGET=aws e2e/cli/smoke.sh          # real AWS, default credentials/region
#
# Requires: aws (v2), python3 (for JSON parsing).
set -uo pipefail

if [[ "${QD_E2E_TARGET:-}" == "aws" ]]; then
  AWS=(aws --output json)
else
  : "${QD_E2E_ENDPOINT:?set QD_E2E_ENDPOINT (or QD_E2E_TARGET=aws)}"
  export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
  export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
  AWS=(aws --output json --endpoint-url "$QD_E2E_ENDPOINT")
fi
export AWS_PAGER="" AWS_MAX_ATTEMPTS=1

PASS=0 FAIL=0
RUN="smoke-$(date +%s)-$RANDOM"
CREATED=()

green() { printf '\033[32m%s\033[0m' "$*"; }
red() { printf '\033[31m%s\033[0m' "$*"; }
ok() { PASS=$((PASS + 1)); echo "  $(green PASS) $*"; }
bad() { FAIL=$((FAIL + 1)); echo "  $(red FAIL) $*"; }
jq_py() { python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }
check() { # description expected actual
  if [[ "$2" == "$3" ]]; then ok "$1"; else bad "$1 (expected '$2', got '$3')"; fi
}
sqs() { "${AWS[@]}" sqs "$@"; }

cleanup() {
  for u in "${CREATED[@]:-}"; do [[ -n "$u" ]] && sqs delete-queue --queue-url "$u" >/dev/null 2>&1; done
}
trap cleanup EXIT

echo "== standard queue"
Q=$(sqs create-queue --queue-name "$RUN-std" --attributes VisibilityTimeout=2 | jq_py 'd["QueueUrl"]') || Q=""
[[ -n "$Q" ]] && CREATED+=("$Q") && ok "create-queue -> $Q" || bad "create-queue"
check "get-queue-url" "$Q" "$(sqs get-queue-url --queue-name "$RUN-std" | jq_py 'd["QueueUrl"]')"
check "list-queues by prefix" "$Q" "$(sqs list-queues --queue-name-prefix "$RUN-std" | jq_py '" ".join(d.get("QueueUrls", []))')"
check "attribute VisibilityTimeout" "2" "$(sqs get-queue-attributes --queue-url "$Q" --attribute-names All | jq_py 'd["Attributes"]["VisibilityTimeout"]')"
sqs set-queue-attributes --queue-url "$Q" --attributes DelaySeconds=0,MaximumMessageSize=4096 >/dev/null && ok "set-queue-attributes" || bad "set-queue-attributes"
check "attribute MaximumMessageSize" "4096" "$(sqs get-queue-attributes --queue-url "$Q" --attribute-names MaximumMessageSize | jq_py 'd["Attributes"]["MaximumMessageSize"]')"

MD5=$(printf '%s' "hello cli" | md5sum 2>/dev/null | cut -d' ' -f1 || md5 -q -s "hello cli")
SENT=$(sqs send-message --queue-url "$Q" --message-body "hello cli" \
  --message-attributes '{"kind":{"DataType":"String","StringValue":"smoke"}}')
check "send-message MD5OfMessageBody" "$MD5" "$(echo "$SENT" | jq_py 'd["MD5OfMessageBody"]')"
RECV=$(sqs receive-message --queue-url "$Q" --wait-time-seconds 2 --message-attribute-names All --attribute-names All)
check "receive-message body" "hello cli" "$(echo "$RECV" | jq_py 'd["Messages"][0]["Body"]')"
check "receive-message attribute" "smoke" "$(echo "$RECV" | jq_py 'd["Messages"][0]["MessageAttributes"]["kind"]["StringValue"]')"
check "receive count" "1" "$(echo "$RECV" | jq_py 'd["Messages"][0]["Attributes"]["ApproximateReceiveCount"]')"
RH=$(echo "$RECV" | jq_py 'd["Messages"][0]["ReceiptHandle"]')
sqs delete-message --queue-url "$Q" --receipt-handle "$RH" >/dev/null && ok "delete-message" || bad "delete-message"
check "queue empty after delete" "0" "$(sqs receive-message --queue-url "$Q" --wait-time-seconds 1 | jq_py 'len(d.get("Messages", []))' 2>/dev/null || echo 0)"

BATCH=$(sqs send-message-batch --queue-url "$Q" --entries Id=a,MessageBody=one Id=b,MessageBody=two)
check "send-message-batch successful" "2" "$(echo "$BATCH" | jq_py 'len(d["Successful"])')"
sqs purge-queue --queue-url "$Q" >/dev/null && ok "purge-queue" || bad "purge-queue"
sqs tag-queue --queue-url "$Q" --tags env=smoke >/dev/null && ok "tag-queue" || bad "tag-queue"
check "list-queue-tags" "smoke" "$(sqs list-queue-tags --queue-url "$Q" | jq_py 'd["Tags"]["env"]')"

ERR=$(sqs get-queue-url --queue-name "$RUN-missing" 2>&1 >/dev/null)
if echo "$ERR" | grep -q "NonExistentQueue\|QueueDoesNotExist"; then ok "missing queue error"; else bad "missing queue error: $ERR"; fi

echo "== FIFO queue"
F=$(sqs create-queue --queue-name "$RUN-f.fifo" --attributes FifoQueue=true,ContentBasedDeduplication=true | jq_py 'd["QueueUrl"]') || F=""
[[ -n "$F" ]] && CREATED+=("$F") && ok "create fifo queue" || bad "create fifo queue"
ID1=$(sqs send-message --queue-url "$F" --message-body first --message-group-id g | jq_py 'd["MessageId"]')
ID2=$(sqs send-message --queue-url "$F" --message-body first --message-group-id g | jq_py 'd["MessageId"]')
check "content-based dedup returns same MessageId" "$ID1" "$ID2"
sqs send-message --queue-url "$F" --message-body second --message-group-id g >/dev/null
FR=$(sqs receive-message --queue-url "$F" --max-number-of-messages 10 --wait-time-seconds 1)
check "fifo order" "first,second" "$(echo "$FR" | jq_py '",".join(m["Body"] for m in d["Messages"])')"

echo "== dead-letter queue"
D=$(sqs create-queue --queue-name "$RUN-dlq" | jq_py 'd["QueueUrl"]') || D=""
[[ -n "$D" ]] && CREATED+=("$D")
DARN=$(sqs get-queue-attributes --queue-url "$D" --attribute-names QueueArn | jq_py 'd["Attributes"]["QueueArn"]')
POLICY=$(python3 -c "import json,sys;print(json.dumps({'RedrivePolicy':json.dumps({'deadLetterTargetArn':sys.argv[1],'maxReceiveCount':1}),'VisibilityTimeout':'1'}))" "$DARN")
S=$(sqs create-queue --queue-name "$RUN-src" --attributes "$POLICY" | jq_py 'd["QueueUrl"]') || S=""
[[ -n "$S" ]] && CREATED+=("$S") && ok "create source queue with RedrivePolicy" || bad "create source queue"
sqs send-message --queue-url "$S" --message-body poison >/dev/null
sqs receive-message --queue-url "$S" --wait-time-seconds 1 >/dev/null
sleep 1.5
sqs receive-message --queue-url "$S" --wait-time-seconds 1 >/dev/null
DR=$(sqs receive-message --queue-url "$D" --wait-time-seconds 5 --attribute-names All)
check "message moved to DLQ" "poison" "$(echo "$DR" | jq_py 'd["Messages"][0]["Body"]' 2>/dev/null)"
check "list-dead-letter-source-queues" "$S" "$(sqs list-dead-letter-source-queues --queue-url "$D" | jq_py '" ".join(d.get("queueUrls", []))')"

echo
echo "smoke: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
