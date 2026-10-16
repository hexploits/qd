"""Dead-letter queues, redrive policies and message move tasks."""

import json
import time

import pytest

from conftest import TARGET_AWS
from sqhelpers import expect_error, queue_arn, queue_attrs, receive, receive_all, wait_for

BAD_POLICY = ("InvalidParameterValue", "InvalidAttributeValue")


def make_pair(sqs, queues, max_receive=2, fifo=False):
    dlq = queues.create("dlq", fifo=fifo)
    dlq_arn = queue_arn(sqs, dlq)
    src = queues.create(
        "src",
        fifo=fifo,
        attributes={
            "VisibilityTimeout": "1",
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": max_receive}),
        },
    )
    return src, dlq, dlq_arn


def exhaust(sqs, src, times):
    """Receive the single message `times` times, letting visibility expire in between."""
    for i in range(times):
        msgs = receive(sqs, src, WaitTimeSeconds=2, AttributeNames=["ApproximateReceiveCount"])
        assert len(msgs) == 1, f"receive #{i + 1}"
        assert msgs[0]["Attributes"]["ApproximateReceiveCount"] == str(i + 1)
        time.sleep(1.2)


def test_redrive_policy_attribute(sqs, queues):
    src, dlq, dlq_arn = make_pair(sqs, queues, max_receive=3)
    rp = json.loads(queue_attrs(sqs, src)["RedrivePolicy"])
    assert rp["deadLetterTargetArn"] == dlq_arn
    assert int(rp["maxReceiveCount"]) == 3


def test_redrive_to_dlq(sqs, queues):
    src, dlq, dlq_arn = make_pair(sqs, queues, max_receive=2)
    src_arn = queue_arn(sqs, src)
    sent = sqs.send_message(QueueUrl=src, MessageBody="poison", MessageAttributes={"k": {"DataType": "String", "StringValue": "v"}})
    exhaust(sqs, src, 2)
    # Third receive attempt exceeds maxReceiveCount -> moved to the DLQ, not delivered.
    assert receive(sqs, src, WaitTimeSeconds=1) == []
    moved = receive_all(sqs, dlq, 1, timeout=5, AttributeNames=["All"], MessageAttributeNames=["All"])
    assert len(moved) == 1
    m = moved[0]
    assert m["Body"] == "poison"
    assert m["MessageId"] == sent["MessageId"]
    assert m["Attributes"]["DeadLetterQueueSourceArn"] == src_arn
    assert m["MessageAttributes"]["k"]["StringValue"] == "v"
    assert receive(sqs, src, WaitTimeSeconds=1) == []


def test_list_dead_letter_source_queues(sqs, queues):
    src, dlq, dlq_arn = make_pair(sqs, queues)
    src2 = queues.create(
        "src2", attributes={"RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 5})}
    )
    r = sqs.list_dead_letter_source_queues(QueueUrl=dlq)
    assert sorted(r["queueUrls"]) == sorted([src, src2])
    page = sqs.list_dead_letter_source_queues(QueueUrl=dlq, MaxResults=1)
    assert len(page["queueUrls"]) == 1 and page.get("NextToken")
    page2 = sqs.list_dead_letter_source_queues(QueueUrl=dlq, MaxResults=1, NextToken=page["NextToken"])
    assert sorted(page["queueUrls"] + page2["queueUrls"]) == sorted([src, src2])
    assert sqs.list_dead_letter_source_queues(QueueUrl=src).get("queueUrls", []) == []


def test_remove_redrive_policy(sqs, queues):
    src, dlq, _ = make_pair(sqs, queues)
    sqs.set_queue_attributes(QueueUrl=src, Attributes={"RedrivePolicy": ""})
    assert "RedrivePolicy" not in queue_attrs(sqs, src)
    assert sqs.list_dead_letter_source_queues(QueueUrl=dlq).get("queueUrls", []) == []


def test_redrive_policy_nonexistent_target(sqs, queue):
    acct = queue.rstrip("/").split("/")[-2]
    arn = f"arn:aws:sqs:us-east-1:{acct}:does-not-exist-{int(time.time())}"
    expect_error(
        sqs.set_queue_attributes,
        *BAD_POLICY,
        QueueUrl=queue,
        Attributes={"RedrivePolicy": json.dumps({"deadLetterTargetArn": arn, "maxReceiveCount": 2})},
    )


@pytest.mark.parametrize("bad", ["not json", '{"maxReceiveCount": 2}', '{"deadLetterTargetArn": "%s", "maxReceiveCount": 0}'])
def test_redrive_policy_invalid(sqs, queues, bad):
    dlq = queues.create("dlq")
    src = queues.create("src")
    policy = bad % queue_arn(sqs, dlq) if "%s" in bad else bad
    expect_error(sqs.set_queue_attributes, *BAD_POLICY, QueueUrl=src, Attributes={"RedrivePolicy": policy})


def test_fifo_requires_fifo_dlq(sqs, queues):
    dlq = queues.create("stddlq")
    expect_error(
        sqs.create_queue,
        *BAD_POLICY,
        QueueName=queues.name("fsrc", fifo=True),
        Attributes={
            "FifoQueue": "true",
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": queue_arn(sqs, dlq), "maxReceiveCount": 2}),
        },
    )


def test_redrive_allow_policy(sqs, queues):
    dlq = queues.create("dlq")
    dlq_arn = queue_arn(sqs, dlq)
    allowed = queues.create("allowed")
    denied = queues.create("denied")
    rp = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 2})

    sqs.set_queue_attributes(QueueUrl=dlq, Attributes={"RedriveAllowPolicy": json.dumps({"redrivePermission": "denyAll"})})
    assert json.loads(queue_attrs(sqs, dlq)["RedriveAllowPolicy"])["redrivePermission"] == "denyAll"
    expect_error(sqs.set_queue_attributes, *BAD_POLICY, QueueUrl=allowed, Attributes={"RedrivePolicy": rp})

    sqs.set_queue_attributes(
        QueueUrl=dlq,
        Attributes={
            "RedriveAllowPolicy": json.dumps(
                {"redrivePermission": "byQueue", "sourceQueueArns": [queue_arn(sqs, allowed)]}
            )
        },
    )
    sqs.set_queue_attributes(QueueUrl=allowed, Attributes={"RedrivePolicy": rp})
    expect_error(sqs.set_queue_attributes, *BAD_POLICY, QueueUrl=denied, Attributes={"RedrivePolicy": rp})

    sqs.set_queue_attributes(QueueUrl=dlq, Attributes={"RedriveAllowPolicy": json.dumps({"redrivePermission": "allowAll"})})
    sqs.set_queue_attributes(QueueUrl=denied, Attributes={"RedrivePolicy": rp})


def test_redrive_allow_policy_invalid(sqs, queue):
    expect_error(
        sqs.set_queue_attributes,
        *BAD_POLICY,
        QueueUrl=queue,
        Attributes={"RedriveAllowPolicy": json.dumps({"redrivePermission": "sometimes"})},
    )


def test_fifo_redrive(sqs, queues):
    src, dlq, dlq_arn = make_pair(sqs, queues, max_receive=1, fifo=True)
    sqs.send_message(QueueUrl=src, MessageBody="f", MessageGroupId="g", MessageDeduplicationId="d")
    exhaust(sqs, src, 1)
    assert receive(sqs, src, WaitTimeSeconds=1) == []
    moved = receive_all(sqs, dlq, 1, timeout=5, AttributeNames=["All"])
    assert [m["Body"] for m in moved] == ["f"]
    assert moved[0]["Attributes"]["MessageGroupId"] == "g"


# --- message move tasks ---------------------------------------------------


def _fill_dlq(sqs, queues, n):
    src, dlq, dlq_arn = make_pair(sqs, queues, max_receive=1)
    sqs.send_message_batch(QueueUrl=src, Entries=[{"Id": str(i), "MessageBody": f"m{i}"} for i in range(n)])
    # Receive everything once, wait for visibility expiry, receive again -> all move to the DLQ.
    got = receive_all(sqs, src, n, timeout=10)
    assert len(got) == n
    time.sleep(1.2)
    assert receive_all(sqs, src, 1, timeout=2) == []
    wait_for(lambda: queue_attrs(sqs, dlq)["ApproximateNumberOfMessages"] == str(n), timeout=10, what="dlq filled")
    return src, dlq, dlq_arn


def _tasks(sqs, arn):
    return sqs.list_message_move_tasks(SourceArn=arn).get("Results", [])


@pytest.mark.qd_only
def test_message_move_task_back_to_source(sqs, queues):
    src, dlq, dlq_arn = _fill_dlq(sqs, queues, 3)
    handle = sqs.start_message_move_task(SourceArn=dlq_arn)["TaskHandle"]
    assert handle
    tasks = _tasks(sqs, dlq_arn)
    assert tasks and tasks[0]["SourceArn"] == dlq_arn
    assert tasks[0]["Status"] in ("RUNNING", "COMPLETED")
    if tasks[0]["Status"] == "RUNNING":
        assert tasks[0]["TaskHandle"] == handle
    done = wait_for(
        lambda: (lambda t: t if t and t[0]["Status"] == "COMPLETED" else None)(_tasks(sqs, dlq_arn)),
        timeout=20,
        what="move task COMPLETED",
    )
    assert done[0]["ApproximateNumberOfMessagesMoved"] == 3
    assert done[0]["ApproximateNumberOfMessagesToMove"] == 3
    back = receive_all(sqs, src, 3, timeout=5)
    assert sorted(m["Body"] for m in back) == ["m0", "m1", "m2"]
    assert receive(sqs, dlq) == []


@pytest.mark.qd_only
def test_message_move_task_to_destination(sqs, queues):
    src, dlq, dlq_arn = _fill_dlq(sqs, queues, 2)
    dest = queues.create("dest")
    dest_arn = queue_arn(sqs, dest)
    sqs.start_message_move_task(SourceArn=dlq_arn, DestinationArn=dest_arn, MaxNumberOfMessagesPerSecond=50)
    wait_for(
        lambda: (lambda t: t and t[0]["Status"] == "COMPLETED")(_tasks(sqs, dlq_arn)), timeout=20, what="move task done"
    )
    t = _tasks(sqs, dlq_arn)[0]
    assert t["DestinationArn"] == dest_arn
    assert t["MaxNumberOfMessagesPerSecond"] == 50
    assert sorted(m["Body"] for m in receive_all(sqs, dest, 2, timeout=5)) == ["m0", "m1"]
    assert receive(sqs, src) == []


@pytest.mark.qd_only
def test_message_move_task_cancel(sqs, queues):
    src, dlq, dlq_arn = _fill_dlq(sqs, queues, 10)
    handle = sqs.start_message_move_task(SourceArn=dlq_arn, MaxNumberOfMessagesPerSecond=1)["TaskHandle"]
    r = sqs.cancel_message_move_task(TaskHandle=handle)
    assert "ApproximateNumberOfMessagesMoved" in r
    st = wait_for(
        lambda: (lambda t: t[0]["Status"] if t and t[0]["Status"] in ("CANCELLED",) else None)(_tasks(sqs, dlq_arn)),
        timeout=10,
        what="task cancelled",
    )
    assert st == "CANCELLED"
    moved = _tasks(sqs, dlq_arn)[0]["ApproximateNumberOfMessagesMoved"]
    assert moved < 10
    # Cancelling a finished task fails.
    expect_error(sqs.cancel_message_move_task, "ResourceNotFoundException", "AWS.SimpleQueueService.UnsupportedOperation",
                 "InvalidParameterValue", "FailedPrecondition", TaskHandle=handle)


def test_message_move_task_requires_dlq(sqs, queue):
    # A queue that is not a DLQ of any source cannot be redriven back to its source.
    expect_error(
        sqs.start_message_move_task,
        "InvalidParameterValue",
        "AWS.SimpleQueueService.UnsupportedOperation",
        "ResourceNotFoundException",
        SourceArn=queue_arn(sqs, queue),
    )


def test_message_move_task_unknown_source(sqs, queue):
    acct = queue.rstrip("/").split("/")[-2]
    expect_error(
        sqs.start_message_move_task,
        "ResourceNotFoundException",
        "AWS.SimpleQueueService.NonExistentQueue",
        "InvalidParameterValue",
        SourceArn=f"arn:aws:sqs:us-east-1:{acct}:nope-{int(time.time())}",
    )


def test_cancel_unknown_task(sqs):
    expect_error(
        sqs.cancel_message_move_task,
        "ResourceNotFoundException",
        "InvalidParameterValue",
        TaskHandle="eyJ0YXNrSWQiOiJub3BlIn0=",
    )
