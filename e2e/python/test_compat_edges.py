"""Smaller AWS-parity details that real-world clients / IaC tools rely on."""

import json
import time
import uuid

import pytest

from sqhelpers import expect_error, queue_arn, queue_attrs, receive


def test_create_existing_without_attributes_returns_url(sqs, queues):
    # Re-creating with *no* attributes compares nothing: AWS returns the existing URL.
    name = queues.name("noattr")
    url = queues.track(sqs.create_queue(QueueName=name, Attributes={"VisibilityTimeout": "9"})["QueueUrl"])
    assert sqs.create_queue(QueueName=name)["QueueUrl"] == url
    assert queue_attrs(sqs, url, ["VisibilityTimeout"])["VisibilityTimeout"] == "9"


def test_create_existing_with_subset_of_attributes(sqs, queues):
    name = queues.name("subset")
    url = queues.track(
        sqs.create_queue(QueueName=name, Attributes={"VisibilityTimeout": "9", "DelaySeconds": "2"})["QueueUrl"]
    )
    assert sqs.create_queue(QueueName=name, Attributes={"DelaySeconds": "2"})["QueueUrl"] == url


def test_max_length_queue_names(sqs, queues):
    base = queues.name("len")
    std = base + "x" * (80 - len(base))
    assert len(std) == 80
    queues.track(sqs.create_queue(QueueName=std)["QueueUrl"])
    fifo = base + "y" * (80 - len(base) - 5) + ".fifo"
    assert len(fifo) == 80
    queues.track(sqs.create_queue(QueueName=fifo, Attributes={"FifoQueue": "true"})["QueueUrl"])


def test_fifo_all_attributes_include_throughput_settings(sqs, fifo_queue):
    a = queue_attrs(sqs, fifo_queue)
    assert a["DeduplicationScope"] == "queue"
    assert a["FifoThroughputLimit"] == "perQueue"


def test_fifo_high_throughput_settings(sqs, fifo_queue):
    sqs.set_queue_attributes(
        QueueUrl=fifo_queue, Attributes={"DeduplicationScope": "messageGroup", "FifoThroughputLimit": "perMessageGroupId"}
    )
    a = queue_attrs(sqs, fifo_queue, ["DeduplicationScope", "FifoThroughputLimit"])
    assert a == {"DeduplicationScope": "messageGroup", "FifoThroughputLimit": "perMessageGroupId"}


def test_policy_attribute_roundtrip(sqs, queue):
    arn = queue_arn(sqs, queue)
    policy = {
        "Version": "2012-10-17",
        "Id": "e2e",
        "Statement": [{"Sid": "s1", "Effect": "Allow", "Principal": "*", "Action": "sqs:SendMessage", "Resource": arn}],
    }
    sqs.set_queue_attributes(QueueUrl=queue, Attributes={"Policy": json.dumps(policy)})
    assert json.loads(queue_attrs(sqs, queue, ["Policy"])["Policy"]) == policy
    sqs.set_queue_attributes(QueueUrl=queue, Attributes={"Policy": ""})
    assert not queue_attrs(sqs, queue, ["Policy"]).get("Policy")


def test_sse_attributes_roundtrip(sqs, queue):
    sqs.set_queue_attributes(QueueUrl=queue, Attributes={"SqsManagedSseEnabled": "false"})
    assert queue_attrs(sqs, queue, ["SqsManagedSseEnabled"])["SqsManagedSseEnabled"] == "false"
    sqs.set_queue_attributes(
        QueueUrl=queue, Attributes={"KmsMasterKeyId": "alias/aws/sqs", "KmsDataKeyReusePeriodSeconds": "600"}
    )
    a = queue_attrs(sqs, queue, ["KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds"])
    assert a == {"KmsMasterKeyId": "alias/aws/sqs", "KmsDataKeyReusePeriodSeconds": "600"}


def test_redrive_policy_with_string_max_receive_count(sqs, queues):
    # CloudFormation/Terraform frequently send maxReceiveCount as a JSON string.
    dlq = queues.create("dlq")
    src = queues.create(
        "src",
        attributes={"RedrivePolicy": json.dumps({"deadLetterTargetArn": queue_arn(sqs, dlq), "maxReceiveCount": "4"})},
    )
    rp = json.loads(queue_attrs(sqs, src, ["RedrivePolicy"])["RedrivePolicy"])
    assert int(rp["maxReceiveCount"]) == 4


def test_empty_string_attribute_value_rejected(sqs, queue):
    expect_error(
        sqs.send_message,
        "InvalidParameterValue",
        QueueUrl=queue,
        MessageBody="x",
        MessageAttributes={"e": {"DataType": "String", "StringValue": ""}},
    )


def test_empty_binary_attribute_value_rejected(sqs, queue):
    expect_error(
        sqs.send_message,
        "InvalidParameterValue",
        QueueUrl=queue,
        MessageBody="x",
        MessageAttributes={"e": {"DataType": "Binary", "BinaryValue": b""}},
    )


def test_sent_timestamp_is_epoch_millis(sqs, queue):
    before = int(time.time() * 1000)
    sqs.send_message(QueueUrl=queue, MessageBody="t")
    m = receive(sqs, queue, WaitTimeSeconds=1, AttributeNames=["SentTimestamp"])[0]
    ts = int(m["Attributes"]["SentTimestamp"])
    assert before - 5000 <= ts <= int(time.time() * 1000) + 5000


@pytest.mark.parametrize("bad", [0, 1001])
def test_list_queues_max_results_range(sqs, bad):
    expect_error(sqs.list_queues, "InvalidParameterValue", MaxResults=bad)


def test_purge_removes_in_flight(sqs, queues):
    url = queues.create("purgeinflight", attributes={"VisibilityTimeout": "1"})
    sqs.send_message(QueueUrl=url, MessageBody="x")
    assert len(receive(sqs, url, WaitTimeSeconds=1)) == 1
    sqs.purge_queue(QueueUrl=url)
    time.sleep(1.3)
    assert receive(sqs, url, WaitTimeSeconds=1) == []
    assert queue_attrs(sqs, url)["ApproximateNumberOfMessagesNotVisible"] == "0"


def test_fifo_group_id_validation(sqs, fifo_queue):
    expect_error(
        sqs.send_message,
        "InvalidParameterValue",
        QueueUrl=fifo_queue,
        MessageBody="x",
        MessageGroupId="g" * 129,
        MessageDeduplicationId="d",
    )
    expect_error(
        sqs.send_message,
        "InvalidParameterValue",
        QueueUrl=fifo_queue,
        MessageBody="x",
        MessageGroupId="bad group",
        MessageDeduplicationId="d",
    )


def test_fifo_dedup_id_validation(sqs, fifo_queue):
    expect_error(
        sqs.send_message,
        "InvalidParameterValue",
        QueueUrl=fifo_queue,
        MessageBody="x",
        MessageGroupId="g",
        MessageDeduplicationId="d" * 129,
    )


def test_fifo_group_id_punctuation_allowed(sqs, fifo_queue):
    # Allowed: alphanumerics and !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
    gid = "a-Z_0.9:!#$%&'()*+,/;<=>?@[\\]^`{|}~\""
    sqs.send_message(QueueUrl=fifo_queue, MessageBody="x", MessageGroupId=gid, MessageDeduplicationId=gid)
    m = receive(sqs, fifo_queue, WaitTimeSeconds=1, AttributeNames=["MessageGroupId"])[0]
    assert m["Attributes"]["MessageGroupId"] == gid


def test_standard_queue_accepts_message_group_id(sqs, queue):
    # Since the 2025 "fair queues" launch, standard queues accept MessageGroupId.
    sqs.send_message(QueueUrl=queue, MessageBody="x", MessageGroupId="tenant-1")
    m = receive(sqs, queue, WaitTimeSeconds=1, AttributeNames=["All"])
    assert [x["Body"] for x in m] == ["x"]


def test_delete_queue_removes_messages(sqs, queues):
    name = queues.name("del")
    url = queues.track(sqs.create_queue(QueueName=name)["QueueUrl"])
    sqs.send_message(QueueUrl=url, MessageBody="x")
    sqs.delete_queue(QueueUrl=url)
    with pytest.raises(sqs.exceptions.QueueDoesNotExist):
        sqs.send_message(QueueUrl=url, MessageBody="y")


def test_unique_message_ids(sqs, queue):
    ids = {sqs.send_message(QueueUrl=queue, MessageBody=str(i))["MessageId"] for i in range(20)}
    assert len(ids) == 20
    for i in ids:
        uuid.UUID(i)  # AWS message ids are UUIDs


def test_queue_url_with_foreign_host_resolves_by_path(sqs, queue):
    # Apps often keep AWS-style queue URLs in config while overriding only the endpoint.
    region = sqs.meta.region_name
    acct, name = queue.rstrip("/").split("/")[-2:]
    aws_style = f"https://sqs.{region}.amazonaws.com/{acct}/{name}"
    sqs.send_message(QueueUrl=aws_style, MessageBody="via-aws-style-url")
    m = receive(sqs, queue, WaitTimeSeconds=1)
    assert [x["Body"] for x in m] == ["via-aws-style-url"]
