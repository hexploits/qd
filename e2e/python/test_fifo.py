"""FIFO queues: naming, ordering, group locking, deduplication, sequence numbers, attempt ids."""

import time
import uuid

import pytest

from botocore.exceptions import ClientError

from sqhelpers import err_code, expect_error, md5_hex, queue_attrs, receive, receive_all


def send(sqs, url, body, group="g", dedup=None, **kw):
    return sqs.send_message(
        QueueUrl=url, MessageBody=body, MessageGroupId=group, MessageDeduplicationId=dedup or uuid.uuid4().hex, **kw
    )


def test_fifo_attributes(sqs, fifo_queue):
    a = queue_attrs(sqs, fifo_queue)
    assert a["FifoQueue"] == "true"
    assert a["ContentBasedDeduplication"] == "false"
    assert fifo_queue.endswith(".fifo")


def test_fifo_name_requires_attribute(sqs, queues):
    # .fifo suffix without FifoQueue=true, and FifoQueue=true without the suffix, are both rejected.
    expect_error(sqs.create_queue, "InvalidParameterValue", QueueName=queues.name("nofifoattr", fifo=True))
    expect_error(
        sqs.create_queue,
        "InvalidParameterValue",
        "InvalidAttributeValue",
        QueueName=queues.name("nosuffix"),
        Attributes={"FifoQueue": "true"},
    )


def test_fifo_only_attribute_on_standard_queue(sqs, queues):
    expect_error(
        sqs.create_queue,
        "InvalidAttributeName",
        "InvalidAttributeValue",
        QueueName=queues.name("std"),
        Attributes={"ContentBasedDeduplication": "true"},
    )


def test_fifo_requires_group_id(sqs, fifo_queue):
    expect_error(
        sqs.send_message, "MissingParameter", QueueUrl=fifo_queue, MessageBody="x", MessageDeduplicationId="d1"
    )


def test_fifo_requires_dedup_id_without_content_based(sqs, fifo_queue):
    expect_error(sqs.send_message, "InvalidParameterValue", QueueUrl=fifo_queue, MessageBody="x", MessageGroupId="g")


def test_fifo_rejects_per_message_delay(sqs, fifo_queue):
    expect_error(
        sqs.send_message,
        "InvalidParameterValue",
        QueueUrl=fifo_queue,
        MessageBody="x",
        MessageGroupId="g",
        MessageDeduplicationId="d",
        DelaySeconds=5,
    )


def test_fifo_ordering_within_groups(sqs, fifo_queue):
    expected = {"A": [], "B": [], "C": []}
    for i in range(4):
        for g in ("A", "B", "C"):
            body = f"{g}{i}"
            send(sqs, fifo_queue, body, group=g)
            expected[g].append(body)
    got = {"A": [], "B": [], "C": []}
    deadline = time.time() + 15
    while sum(map(len, got.values())) < 12 and time.time() < deadline:
        msgs = receive(sqs, fifo_queue, WaitTimeSeconds=1, AttributeNames=["MessageGroupId"])
        for m in msgs:
            got[m["Attributes"]["MessageGroupId"]].append(m["Body"])
        if msgs:
            sqs.delete_message_batch(
                QueueUrl=fifo_queue,
                Entries=[{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)],
            )
    assert got == expected


def test_fifo_group_blocking(sqs, fifo_queue):
    send(sqs, fifo_queue, "a1", group="A")
    send(sqs, fifo_queue, "a2", group="A")
    first = receive(sqs, fifo_queue, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert [m["Body"] for m in first] == ["a1"]
    # Group A is locked while a1 is in flight.
    assert receive(sqs, fifo_queue, WaitTimeSeconds=1) == []
    # Other groups are not blocked.
    send(sqs, fifo_queue, "b1", group="B")
    assert [m["Body"] for m in receive(sqs, fifo_queue, WaitTimeSeconds=1)] == ["b1"]
    sqs.delete_message(QueueUrl=fifo_queue, ReceiptHandle=first[0]["ReceiptHandle"])
    third = receive(sqs, fifo_queue, WaitTimeSeconds=1)
    assert [m["Body"] for m in third] == ["a2"]


def test_fifo_redelivery_keeps_order(sqs, queues):
    url = queues.create("fiforedeliver", fifo=True, attributes={"VisibilityTimeout": "1"})
    send(sqs, url, "1")
    send(sqs, url, "2")
    m = receive(sqs, url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert [x["Body"] for x in m] == ["1"]
    time.sleep(1.3)
    # The un-deleted head message comes back before "2".
    msgs = receive(sqs, url, WaitTimeSeconds=1)
    assert [x["Body"] for x in msgs][:1] == ["1"]


def test_fifo_explicit_dedup(sqs, fifo_queue):
    r1 = send(sqs, fifo_queue, "one", dedup="same")
    r2 = send(sqs, fifo_queue, "two-different-body", dedup="same")
    assert r1["MessageId"] == r2["MessageId"]
    msgs = receive_all(sqs, fifo_queue, 2, timeout=3)
    assert [m["Body"] for m in msgs] == ["one"]


def test_fifo_dedup_after_delete(sqs, fifo_queue):
    # Dedup interval applies even after the original was consumed and deleted.
    r1 = send(sqs, fifo_queue, "x", dedup="d")
    m = receive(sqs, fifo_queue, WaitTimeSeconds=1)[0]
    sqs.delete_message(QueueUrl=fifo_queue, ReceiptHandle=m["ReceiptHandle"])
    r2 = send(sqs, fifo_queue, "x", dedup="d")
    assert r2["MessageId"] == r1["MessageId"]
    assert receive(sqs, fifo_queue, WaitTimeSeconds=1) == []


def test_fifo_content_based_dedup(sqs, queues):
    url = queues.create("cbd", fifo=True, attributes={"ContentBasedDeduplication": "true"})
    assert queue_attrs(sqs, url)["ContentBasedDeduplication"] == "true"
    r1 = sqs.send_message(QueueUrl=url, MessageBody="same body", MessageGroupId="g")
    r2 = sqs.send_message(QueueUrl=url, MessageBody="same body", MessageGroupId="g")
    r3 = sqs.send_message(QueueUrl=url, MessageBody="other body", MessageGroupId="g")
    assert r1["MessageId"] == r2["MessageId"] != r3["MessageId"]
    msgs = receive_all(sqs, url, 3, timeout=3)
    assert [m["Body"] for m in msgs] == ["same body", "other body"]


def test_fifo_dedup_attribute_on_receive(sqs, fifo_queue):
    send(sqs, fifo_queue, "x", group="grp", dedup="ddd")
    m = receive(sqs, fifo_queue, WaitTimeSeconds=1, AttributeNames=["All"])[0]
    a = m["Attributes"]
    assert a["MessageGroupId"] == "grp"
    assert a["MessageDeduplicationId"] == "ddd"
    assert a["SequenceNumber"].isdigit()


def test_fifo_sequence_numbers_monotonic(sqs, fifo_queue):
    seqs = []
    for i in range(5):
        r = send(sqs, fifo_queue, f"m{i}", group="A" if i % 2 else "B")
        assert r["SequenceNumber"].isdigit()
        seqs.append(int(r["SequenceNumber"]))
    assert seqs == sorted(seqs) and len(set(seqs)) == 5


def test_fifo_send_batch(sqs, fifo_queue):
    entries = [
        {"Id": f"e{i}", "MessageBody": f"m{i}", "MessageGroupId": "g", "MessageDeduplicationId": f"d{i}"} for i in range(5)
    ]
    r = sqs.send_message_batch(QueueUrl=fifo_queue, Entries=entries)
    assert r.get("Failed", []) == []
    seqs = [int(e["SequenceNumber"]) for e in sorted(r["Successful"], key=lambda e: e["Id"])]
    assert seqs == sorted(seqs)
    msgs = receive_all(sqs, fifo_queue, 5, timeout=5)
    assert [m["Body"] for m in msgs] == [f"m{i}" for i in range(5)]
    assert all(m["MD5OfBody"] == md5_hex(m["Body"]) for m in msgs)


def test_fifo_batch_missing_group_id(sqs, fifo_queue):
    # Real AWS behaviour here (per-entry failure vs. whole-request error) is not documented
    # precisely; accept either form as long as the code is MissingParameter.
    try:
        r = sqs.send_message_batch(
            QueueUrl=fifo_queue,
            Entries=[
                {"Id": "ok", "MessageBody": "x", "MessageGroupId": "g", "MessageDeduplicationId": "1"},
                {"Id": "nogroup", "MessageBody": "y", "MessageDeduplicationId": "2"},
            ],
        )
    except ClientError as e:
        assert err_code(e) == "MissingParameter"
        return
    assert [e["Id"] for e in r["Successful"]] == ["ok"]
    assert [(f["Id"], f["Code"]) for f in r["Failed"]] == [("nogroup", "MissingParameter")]


def test_fifo_receive_request_attempt_id(sqs, fifo_queue):
    for i in range(3):
        send(sqs, fifo_queue, f"m{i}")
    attempt = uuid.uuid4().hex
    r1 = receive(sqs, fifo_queue, ReceiveRequestAttemptId=attempt, WaitTimeSeconds=1)
    assert [m["Body"] for m in r1] == ["m0", "m1", "m2"]
    r2 = receive(sqs, fifo_queue, ReceiveRequestAttemptId=attempt, WaitTimeSeconds=1)
    assert [m["MessageId"] for m in r2] == [m["MessageId"] for m in r1]
    # A different attempt id gets nothing: the group is in flight.
    assert receive(sqs, fifo_queue, ReceiveRequestAttemptId=uuid.uuid4().hex) == []
    # Receipt handles returned by the replay are usable.
    r = sqs.delete_message_batch(
        QueueUrl=fifo_queue, Entries=[{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(r2)]
    )
    assert r.get("Failed", []) == []


def test_fifo_change_visibility_zero_releases_group(sqs, fifo_queue):
    send(sqs, fifo_queue, "a1", group="A")
    send(sqs, fifo_queue, "a2", group="A")
    m = receive(sqs, fifo_queue, MaxNumberOfMessages=1, WaitTimeSeconds=1)[0]
    sqs.change_message_visibility(QueueUrl=fifo_queue, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
    again = receive(sqs, fifo_queue, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert [x["Body"] for x in again] == ["a1"]


def test_fifo_queue_delay(sqs, queues):
    url = queues.create("fifodelay", fifo=True, attributes={"DelaySeconds": "2"})
    send(sqs, url, "x")
    assert receive(sqs, url) == []
    time.sleep(2.2)
    assert [m["Body"] for m in receive(sqs, url, WaitTimeSeconds=2)] == ["x"]
