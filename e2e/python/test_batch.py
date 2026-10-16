"""SendMessageBatch / DeleteMessageBatch / ChangeMessageVisibilityBatch."""

import time

import pytest

from conftest import TARGET_AWS
from sqhelpers import expect_error, md5_hex, md5_of_attributes, receive, receive_all

MAX = 1048576


def test_send_batch(sqs, queue):
    attrs = {"k": {"DataType": "String", "StringValue": "v"}}
    entries = [{"Id": f"e{i}", "MessageBody": f"body-{i}"} for i in range(10)]
    entries[3]["MessageAttributes"] = attrs
    entries[4]["DelaySeconds"] = 0
    r = sqs.send_message_batch(QueueUrl=queue, Entries=entries)
    assert r.get("Failed", []) == []
    ok = {e["Id"]: e for e in r["Successful"]}
    assert set(ok) == {f"e{i}" for i in range(10)}
    for i in range(10):
        assert ok[f"e{i}"]["MD5OfMessageBody"] == md5_hex(f"body-{i}")
        assert ok[f"e{i}"]["MessageId"]
    assert ok["e3"]["MD5OfMessageAttributes"] == md5_of_attributes(attrs)
    got = receive_all(sqs, queue, 10)
    assert sorted(m["Body"] for m in got) == sorted(f"body-{i}" for i in range(10))


def test_send_batch_partial_failure(sqs, queue):
    entries = [
        {"Id": "good1", "MessageBody": "ok"},
        {"Id": "bad", "MessageBody": "bad\x01char"},
        {"Id": "good2", "MessageBody": "ok2"},
    ]
    r = sqs.send_message_batch(QueueUrl=queue, Entries=entries)
    assert {e["Id"] for e in r["Successful"]} == {"good1", "good2"}
    assert len(r["Failed"]) == 1
    f = r["Failed"][0]
    assert f["Id"] == "bad"
    assert f["SenderFault"] is True
    assert f["Code"] == "InvalidMessageContents"


def test_send_batch_too_many(sqs, queue):
    entries = [{"Id": f"e{i}", "MessageBody": "x"} for i in range(11)]
    expect_error(sqs.send_message_batch, "AWS.SimpleQueueService.TooManyEntriesInBatchRequest", QueueUrl=queue, Entries=entries)


def test_send_batch_duplicate_ids(sqs, queue):
    entries = [{"Id": "same", "MessageBody": "x"}, {"Id": "same", "MessageBody": "y"}]
    expect_error(sqs.send_message_batch, "AWS.SimpleQueueService.BatchEntryIdsNotDistinct", QueueUrl=queue, Entries=entries)


@pytest.mark.parametrize("op", ["send_message_batch", "delete_message_batch", "change_message_visibility_batch"])
def test_batch_empty(sqs, queue, protocol, request, op):
    # Under the Query protocol botocore serializes an empty list as a bare "Entries=" parameter.
    codes = ["AWS.SimpleQueueService.EmptyBatchRequest"]
    if protocol == "query" and TARGET_AWS:
        codes.append("MissingParameter")
    expect_error(getattr(sqs, op), *codes, QueueUrl=queue, Entries=[])


@pytest.mark.parametrize("bad_id", ["has space", "a" * 81, "dot.id"])
def test_send_batch_invalid_entry_id(sqs, queue, bad_id):
    expect_error(
        sqs.send_message_batch,
        "AWS.SimpleQueueService.InvalidBatchEntryId",
        QueueUrl=queue,
        Entries=[{"Id": bad_id, "MessageBody": "x"}],
    )


def test_send_batch_total_too_long(sqs, queue):
    big = "x" * (MAX // 2 + 10)
    expect_error(
        sqs.send_message_batch,
        "AWS.SimpleQueueService.BatchRequestTooLong",
        QueueUrl=queue,
        Entries=[{"Id": "a", "MessageBody": big}, {"Id": "b", "MessageBody": big}],
    )


def test_delete_batch_with_partial_failure(sqs, queue):
    sqs.send_message_batch(QueueUrl=queue, Entries=[{"Id": f"e{i}", "MessageBody": f"m{i}"} for i in range(3)])
    msgs = receive_all(sqs, queue, 3)
    assert len(msgs) == 3
    entries = [{"Id": f"d{i}", "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)]
    entries.append({"Id": "bogus", "ReceiptHandle": "not-a-real-handle"})
    r = sqs.delete_message_batch(QueueUrl=queue, Entries=entries)
    assert {e["Id"] for e in r["Successful"]} == {"d0", "d1", "d2"}
    assert len(r["Failed"]) == 1
    assert r["Failed"][0]["Id"] == "bogus"
    assert r["Failed"][0]["Code"] == "ReceiptHandleIsInvalid"
    assert r["Failed"][0]["SenderFault"] is True


def test_delete_batch_errors(sqs, queue):
    expect_error(
        sqs.delete_message_batch,
        "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
        QueueUrl=queue,
        Entries=[{"Id": f"e{i}", "ReceiptHandle": "x"} for i in range(11)],
    )
    expect_error(
        sqs.delete_message_batch,
        "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
        QueueUrl=queue,
        Entries=[{"Id": "a", "ReceiptHandle": "x"}, {"Id": "a", "ReceiptHandle": "y"}],
    )


def test_change_visibility_batch(sqs, queues):
    url = queues.create("cvb", attributes={"VisibilityTimeout": "30"})
    sqs.send_message_batch(QueueUrl=url, Entries=[{"Id": f"e{i}", "MessageBody": f"m{i}"} for i in range(2)])
    msgs = receive_all(sqs, url, 2)
    assert len(msgs) == 2
    byb = {m["Body"]: m for m in msgs}
    r = sqs.change_message_visibility_batch(
        QueueUrl=url,
        Entries=[
            {"Id": "zero", "ReceiptHandle": byb["m0"]["ReceiptHandle"], "VisibilityTimeout": 0},
            {"Id": "long", "ReceiptHandle": byb["m1"]["ReceiptHandle"], "VisibilityTimeout": 60},
            {"Id": "bad", "ReceiptHandle": "garbage", "VisibilityTimeout": 0},
        ],
    )
    assert {e["Id"] for e in r["Successful"]} == {"zero", "long"}
    assert [(f["Id"], f["Code"]) for f in r["Failed"]] == [("bad", "ReceiptHandleIsInvalid")]
    again = receive(sqs, url, WaitTimeSeconds=1)
    assert [m["Body"] for m in again] == ["m0"]


def test_change_visibility_batch_errors(sqs, queue):
    expect_error(
        sqs.change_message_visibility_batch,
        "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
        QueueUrl=queue,
        Entries=[{"Id": "a", "ReceiptHandle": "x"}, {"Id": "a", "ReceiptHandle": "y"}],
    )
