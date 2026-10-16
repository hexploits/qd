"""Visibility timeout, ChangeMessageVisibility, delays and long polling."""

import threading
import time

import pytest

from sqhelpers import expect_error, queue_attrs, receive, wait_for


def test_visibility_timeout_redelivery(sqs, queues):
    url = queues.create("vis", attributes={"VisibilityTimeout": "1"})
    sqs.send_message(QueueUrl=url, MessageBody="again")
    m1 = receive(sqs, url, AttributeNames=["All"], WaitTimeSeconds=1)[0]
    assert m1["Attributes"]["ApproximateReceiveCount"] == "1"
    assert receive(sqs, url) == []  # in flight
    time.sleep(1.3)
    m2 = receive(sqs, url, AttributeNames=["All"], WaitTimeSeconds=2)[0]
    assert m2["MessageId"] == m1["MessageId"]
    assert m2["Attributes"]["ApproximateReceiveCount"] == "2"
    assert m2["ReceiptHandle"] != m1["ReceiptHandle"]
    # first receive timestamp stays the same
    assert m2["Attributes"]["ApproximateFirstReceiveTimestamp"] == m1["Attributes"]["ApproximateFirstReceiveTimestamp"]
    time.sleep(1.3)
    m3 = receive(sqs, url, AttributeNames=["ApproximateReceiveCount"], WaitTimeSeconds=2)[0]
    assert m3["Attributes"]["ApproximateReceiveCount"] == "3"
    sqs.delete_message(QueueUrl=url, ReceiptHandle=m3["ReceiptHandle"])
    time.sleep(1.3)
    assert receive(sqs, url, WaitTimeSeconds=1) == []


def test_receive_visibility_timeout_override(sqs, queue):
    # Queue default is 30s; override per receive.
    sqs.send_message(QueueUrl=queue, MessageBody="x")
    m = receive(sqs, queue, VisibilityTimeout=1, WaitTimeSeconds=1)[0]
    assert receive(sqs, queue) == []
    time.sleep(1.3)
    assert receive(sqs, queue, WaitTimeSeconds=2)[0]["MessageId"] == m["MessageId"]


def test_change_visibility_to_zero(sqs, queue):
    sqs.send_message(QueueUrl=queue, MessageBody="x")
    m = receive(sqs, queue, WaitTimeSeconds=1)[0]
    assert receive(sqs, queue) == []
    sqs.change_message_visibility(QueueUrl=queue, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
    again = receive(sqs, queue, WaitTimeSeconds=1, AttributeNames=["ApproximateReceiveCount"])
    assert again[0]["MessageId"] == m["MessageId"]
    assert again[0]["Attributes"]["ApproximateReceiveCount"] == "2"


def test_change_visibility_extend(sqs, queues):
    url = queues.create("ext", attributes={"VisibilityTimeout": "1"})
    sqs.send_message(QueueUrl=url, MessageBody="x")
    m = receive(sqs, url, WaitTimeSeconds=1)[0]
    sqs.change_message_visibility(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=4)
    time.sleep(1.8)
    assert receive(sqs, url) == []
    # The original receipt handle remains valid for deletion.
    sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
    time.sleep(2.5)
    assert receive(sqs, url, WaitTimeSeconds=1) == []


def test_change_visibility_invalid(sqs, queue):
    expect_error(
        sqs.change_message_visibility, "ReceiptHandleIsInvalid", QueueUrl=queue, ReceiptHandle="junk", VisibilityTimeout=5
    )
    sqs.send_message(QueueUrl=queue, MessageBody="x")
    m = receive(sqs, queue, WaitTimeSeconds=1)[0]
    expect_error(
        sqs.change_message_visibility,
        "InvalidParameterValue",
        QueueUrl=queue,
        ReceiptHandle=m["ReceiptHandle"],
        VisibilityTimeout=43201,
    )


def test_queue_delay(sqs, queues):
    url = queues.create("delay", attributes={"DelaySeconds": "2"})
    sqs.send_message(QueueUrl=url, MessageBody="later")
    assert receive(sqs, url) == []
    a = queue_attrs(sqs, url)
    assert a["ApproximateNumberOfMessagesDelayed"] == "1"
    assert a["ApproximateNumberOfMessages"] == "0"
    time.sleep(2.2)
    msgs = receive(sqs, url, WaitTimeSeconds=2)
    assert [m["Body"] for m in msgs] == ["later"]


def test_per_message_delay(sqs, queue):
    sqs.send_message(QueueUrl=queue, MessageBody="delayed", DelaySeconds=2)
    sqs.send_message(QueueUrl=queue, MessageBody="now")
    assert [m["Body"] for m in receive(sqs, queue, WaitTimeSeconds=1)] == ["now"]
    assert queue_attrs(sqs, queue)["ApproximateNumberOfMessagesDelayed"] == "1"
    t0 = time.time()
    msgs = receive(sqs, queue, WaitTimeSeconds=5)
    assert [m["Body"] for m in msgs] == ["delayed"]
    assert time.time() - t0 < 4.5


def test_per_message_delay_overrides_queue_delay(sqs, queues):
    url = queues.create("delay2", attributes={"DelaySeconds": "10"})
    sqs.send_message(QueueUrl=url, MessageBody="zero", DelaySeconds=0)
    assert [m["Body"] for m in receive(sqs, url, WaitTimeSeconds=1)] == ["zero"]


def test_long_poll_returns_early_on_send(sqs, queue):
    def later():
        time.sleep(1.0)
        sqs.send_message(QueueUrl=queue, MessageBody="wakeup")

    t = threading.Thread(target=later)
    t0 = time.time()
    t.start()
    msgs = receive(sqs, queue, WaitTimeSeconds=10)
    elapsed = time.time() - t0
    t.join()
    assert [m["Body"] for m in msgs] == ["wakeup"]
    assert 0.8 < elapsed < 4, elapsed


def test_long_poll_waits_when_empty(sqs, queue):
    t0 = time.time()
    assert receive(sqs, queue, WaitTimeSeconds=2) == []
    elapsed = time.time() - t0
    assert 1.8 <= elapsed < 5, elapsed


def test_queue_level_wait_time(sqs, queues):
    url = queues.create("lp", attributes={"ReceiveMessageWaitTimeSeconds": "2"})
    t0 = time.time()
    r = sqs.receive_message(QueueUrl=url)
    elapsed = time.time() - t0
    assert r.get("Messages", []) == []
    assert 1.8 <= elapsed < 5, elapsed
    # Explicit WaitTimeSeconds=0 overrides the queue default (short poll).
    t0 = time.time()
    sqs.receive_message(QueueUrl=url, WaitTimeSeconds=0)
    assert time.time() - t0 < 1.5


def test_long_poll_wakes_on_visibility_expiry(sqs, queues):
    url = queues.create("lpvis", attributes={"VisibilityTimeout": "2"})
    sqs.send_message(QueueUrl=url, MessageBody="x")
    receive(sqs, url, WaitTimeSeconds=1)
    t0 = time.time()
    msgs = receive(sqs, url, WaitTimeSeconds=10)
    elapsed = time.time() - t0
    assert len(msgs) == 1
    assert elapsed < 6, elapsed


def test_long_poll_wakes_on_delay_expiry(sqs, queue):
    sqs.send_message(QueueUrl=queue, MessageBody="x", DelaySeconds=2)
    t0 = time.time()
    msgs = receive(sqs, queue, WaitTimeSeconds=10)
    assert len(msgs) == 1
    assert time.time() - t0 < 6
