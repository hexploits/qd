# AWS SQS compatibility

qd implements the SQS API version `2012-11-05` over both wire protocols:

- **AWS JSON 1.0** (`X-Amz-Target: AmazonSQS.<Action>`) — current AWS SDKs and CLI v2. Errors
  include the `x-amzn-query-error` header so SDKs report the same legacy error codes as AWS
  (e.g. `AWS.SimpleQueueService.NonExistentQueue`) and raise the same modeled exceptions.
- **AWS Query** (`Action=...` form/query parameters, XML responses) — older SDKs (e.g. botocore
  ≤ 1.31.80, AWS SDK for Java v1, older Go/PHP/Ruby SDKs). Requests may be sent to the service
  endpoint or directly to a queue URL.

Verified with: AWS SDK for Rust, boto3 (latest, JSON) and boto3 1.28.80/botocore 1.31.80
(Query), AWS CLI v2.

## Actions

| Action | Status |
|---|---|
| CreateQueue, DeleteQueue, GetQueueUrl, ListQueues | ✅ (idempotent create, conflict detection, prefix + pagination) |
| GetQueueAttributes, SetQueueAttributes | ✅ all attributes, incl. computed counts |
| PurgeQueue | ✅ with 60s cooldown (configurable) |
| SendMessage, SendMessageBatch | ✅ attributes, system attributes (AWSTraceHeader), delays, FIFO fields, MD5s |
| ReceiveMessage | ✅ long polling, attribute filters (`All`, `.*`, `prefix.*`), FIFO `ReceiveRequestAttemptId` |
| DeleteMessage, DeleteMessageBatch | ✅ |
| ChangeMessageVisibility, ChangeMessageVisibilityBatch | ✅ |
| TagQueue, UntagQueue, ListQueueTags | ✅ |
| AddPermission, RemovePermission | ✅ stored in the `Policy` attribute (not enforced) |
| ListDeadLetterSourceQueues | ✅ |
| StartMessageMoveTask, CancelMessageMoveTask, ListMessageMoveTasks | ✅ to source queues or an explicit destination, rate-limited |

## Queue attributes

`DelaySeconds`, `MaximumMessageSize` (up to 1 MiB), `MessageRetentionPeriod`,
`ReceiveMessageWaitTimeSeconds`, `VisibilityTimeout`, `RedrivePolicy`, `RedriveAllowPolicy`,
`Policy`, `KmsMasterKeyId`, `KmsDataKeyReusePeriodSeconds`, `SqsManagedSseEnabled`, `FifoQueue`,
`ContentBasedDeduplication`, `DeduplicationScope`, `FifoThroughputLimit`, and read-only
`ApproximateNumberOfMessages`, `ApproximateNumberOfMessagesNotVisible`,
`ApproximateNumberOfMessagesDelayed`, `CreatedTimestamp`, `LastModifiedTimestamp`, `QueueArn`.
Defaults and valid ranges match AWS.

## Intentional differences

| Area | qd | AWS |
|---|---|---|
| IAM / queue policies | Policies are stored and returned, not evaluated. Access control is per account via SigV4 credentials; cross-account access is off unless `QD_ALLOW_CROSS_ACCOUNT=true`. | Full IAM evaluation. |
| KMS / SSE attributes | Accepted and returned; encryption at rest is provided by your Postgres storage (e.g. encrypted volumes). | Messages encrypted with SQS/KMS keys. |
| Queue deletion | Immediate; a queue with the same name can be recreated right away. | Takes up to 60s; recreating sooner returns `QueueDeletedRecently`. |
| Purge | Synchronous. | Up to 60s. |
| Counts (`ApproximateNumberOf*`) | Exact at the time of the call. | Approximate, eventually consistent. |
| Short polling | Returns all available messages up to `MaxNumberOfMessages`. | May sample a subset of servers and return fewer. |
| Attribute propagation | ≤ `QD_QUEUE_CACHE_TTL_MS` (1s) across replicas. | Up to 60s. |
| Standard-queue ordering | Best-effort oldest-visible-first. | Best-effort. |
| Exactly-once / duplicates | Standard queues deliver each message once per visibility timeout (no spurious duplicates). | Standard queues may occasionally deliver duplicates. |
| In-flight limits | Not enforced (120k / 20k `OverLimit`). | Enforced. |
| Throughput limits | No artificial throttling. FIFO receives on one queue are serialised by design. | Per-queue quotas. |
| `ChangeMessageVisibility` | Allowed for the current receipt handle even if the visibility timeout already lapsed (as long as the message wasn't received again); no 12-hour cap. | May return `MessageNotInflight`; 12h cap from receipt. |
| DLQ receive count | Preserved when moved to the DLQ; reset to 0 by redrive. | Same. |

## Reporting incompatibilities

The Python suite in `e2e/python` can run against real AWS (`QD_E2E_TARGET=aws`). If you find a
behavioural difference, add a test there that passes against AWS, then fix the server until it
passes against qd too.
