"""Queue lifecycle, URLs, listing, attributes, tags, permissions, purge."""

import json
import os
import time

import pytest
from botocore.exceptions import ClientError

from conftest import TARGET_AWS
from sqhelpers import account_of, err_code, expect_error, queue_attrs, receive, wait_for

NON_EXISTENT = "AWS.SimpleQueueService.NonExistentQueue"


def test_create_get_delete_lifecycle(sqs, queues):
    name = queues.name("life")
    url = sqs.create_queue(QueueName=name)["QueueUrl"]
    queues.track(url)
    assert url.endswith("/" + name)
    assert sqs.get_queue_url(QueueName=name)["QueueUrl"] == url
    sqs.delete_queue(QueueUrl=url)
    if TARGET_AWS:
        time.sleep(2)  # deletion is eventually consistent on AWS
    expect_error(sqs.get_queue_url, NON_EXISTENT, QueueName=name)


def test_queue_url_shape(sqs, queue):
    acct = account_of(queue)
    assert acct.isdigit() and len(acct) == 12
    if not TARGET_AWS:
        assert acct == "000000000000"
    arn = queue_attrs(sqs, queue, ["QueueArn"])["QueueArn"]
    assert arn.startswith("arn:aws:sqs:")
    assert arn.endswith(f":{acct}:{queue.rsplit('/', 1)[1]}")


def test_create_is_idempotent_with_same_attributes(sqs, queues):
    name = queues.name("idem")
    u1 = queues.track(sqs.create_queue(QueueName=name, Attributes={"VisibilityTimeout": "7"})["QueueUrl"])
    u2 = sqs.create_queue(QueueName=name, Attributes={"VisibilityTimeout": "7"})["QueueUrl"]
    assert u1 == u2


def test_create_conflicting_attributes(sqs, queues):
    name = queues.name("conflict")
    queues.track(sqs.create_queue(QueueName=name, Attributes={"VisibilityTimeout": "7"})["QueueUrl"])
    expect_error(sqs.create_queue, "QueueAlreadyExists", QueueName=name, Attributes={"VisibilityTimeout": "8"})


@pytest.mark.parametrize("bad", ["has space", "a" * 81, "dot.name", "", "slash/name"])
def test_create_invalid_name(sqs, bad):
    expect_error(sqs.create_queue, "InvalidParameterValue", "MissingParameter", QueueName=bad)


def test_get_queue_url_nonexistent_modeled_exception(sqs, run_id):
    with pytest.raises(sqs.exceptions.QueueDoesNotExist) as ei:
        sqs.get_queue_url(QueueName=f"e2e-{run_id}-does-not-exist")
    assert err_code(ei.value) == NON_EXISTENT


def test_operations_on_missing_queue(sqs, queue):
    missing = queue + "-missing"
    for fn, kw in [
        (sqs.send_message, {"MessageBody": "x"}),
        (sqs.receive_message, {}),
        (sqs.get_queue_attributes, {"AttributeNames": ["All"]}),
        (sqs.purge_queue, {}),
        (sqs.delete_queue, {}),
    ]:
        with pytest.raises(sqs.exceptions.QueueDoesNotExist):
            fn(QueueUrl=missing, **kw)


def test_get_queue_url_with_owner_account(sqs, queue):
    name = queue.rsplit("/", 1)[1]
    acct = account_of(queue)
    assert sqs.get_queue_url(QueueName=name, QueueOwnerAWSAccountId=acct)["QueueUrl"] == queue


def _list(sqs, **kw):
    return sqs.list_queues(**kw)


def test_list_queues_prefix_and_pagination(sqs, queues):
    prefix = queues.name("lst")
    urls = sorted(queues.create(name=f"{prefix}-{i}") for i in range(3))
    queues.create("unrelated")

    def all_listed():
        got = _list(sqs, QueueNamePrefix=prefix).get("QueueUrls", [])
        return sorted(got) if len(got) == 3 else None

    assert wait_for(all_listed, timeout=10 if not TARGET_AWS else 60, what="queues listed") == urls

    page1 = _list(sqs, QueueNamePrefix=prefix, MaxResults=2)
    assert len(page1["QueueUrls"]) == 2
    assert page1.get("NextToken")
    page2 = _list(sqs, QueueNamePrefix=prefix, MaxResults=2, NextToken=page1["NextToken"])
    assert len(page2["QueueUrls"]) == 1
    assert not page2.get("NextToken")
    assert sorted(page1["QueueUrls"] + page2["QueueUrls"]) == urls


def test_list_queues_no_match_omits_urls(sqs, run_id):
    r = _list(sqs, QueueNamePrefix=f"e2e-{run_id}-nothing-matches-this")
    assert r.get("QueueUrls", []) == []


def test_default_attributes(sqs, queue):
    a = queue_attrs(sqs, queue)
    assert a["VisibilityTimeout"] == "30"
    assert a["MessageRetentionPeriod"] == "345600"
    assert a["MaximumMessageSize"] in ("1048576",)
    assert a["DelaySeconds"] == "0"
    assert a["ReceiveMessageWaitTimeSeconds"] == "0"
    assert a["SqsManagedSseEnabled"] == "true"
    for k in (
        "QueueArn",
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
        "CreatedTimestamp",
        "LastModifiedTimestamp",
    ):
        assert k in a, k
    assert int(a["CreatedTimestamp"]) > 1_600_000_000
    assert "FifoQueue" not in a


def test_get_selected_attributes(sqs, queue):
    a = queue_attrs(sqs, queue, ["VisibilityTimeout", "DelaySeconds"])
    assert a == {"VisibilityTimeout": "30", "DelaySeconds": "0"}


def test_set_and_get_attributes(sqs, queue):
    sqs.set_queue_attributes(
        QueueUrl=queue,
        Attributes={
            "VisibilityTimeout": "45",
            "DelaySeconds": "3",
            "ReceiveMessageWaitTimeSeconds": "5",
            "MaximumMessageSize": "2048",
            "MessageRetentionPeriod": "120",
        },
    )
    a = queue_attrs(sqs, queue)
    assert a["VisibilityTimeout"] == "45"
    assert a["DelaySeconds"] == "3"
    assert a["ReceiveMessageWaitTimeSeconds"] == "5"
    assert a["MaximumMessageSize"] == "2048"
    assert a["MessageRetentionPeriod"] == "120"


def test_create_with_attributes(sqs, queues):
    url = queues.create("cattr", attributes={"VisibilityTimeout": "5", "DelaySeconds": "1"})
    a = queue_attrs(sqs, url, ["VisibilityTimeout", "DelaySeconds"])
    assert a == {"VisibilityTimeout": "5", "DelaySeconds": "1"}


def test_get_invalid_attribute_name(sqs, queue):
    expect_error(sqs.get_queue_attributes, "InvalidAttributeName", QueueUrl=queue, AttributeNames=["NotAnAttribute"])


def test_set_invalid_attribute_name(sqs, queue):
    expect_error(sqs.set_queue_attributes, "InvalidAttributeName", QueueUrl=queue, Attributes={"Bogus": "1"})


@pytest.mark.parametrize(
    "attr,value",
    [
        ("VisibilityTimeout", "43201"),
        ("VisibilityTimeout", "-1"),
        ("VisibilityTimeout", "abc"),
        ("DelaySeconds", "901"),
        ("ReceiveMessageWaitTimeSeconds", "21"),
        ("MessageRetentionPeriod", "59"),
        ("MessageRetentionPeriod", "1209601"),
        ("MaximumMessageSize", "1023"),
        ("MaximumMessageSize", "1048577"),
    ],
)
def test_set_invalid_attribute_value(sqs, queue, attr, value):
    expect_error(sqs.set_queue_attributes, "InvalidAttributeValue", QueueUrl=queue, Attributes={attr: value})


def test_create_invalid_attribute_value(sqs, queues):
    expect_error(
        sqs.create_queue, "InvalidAttributeValue", QueueName=queues.name("badattr"), Attributes={"DelaySeconds": "1000"}
    )


def test_cannot_change_fifo_attribute(sqs, queue):
    # FifoQueue is immutable after creation.
    expect_error(
        sqs.set_queue_attributes,
        "InvalidAttributeName",
        "InvalidAttributeValue",
        QueueUrl=queue,
        Attributes={"FifoQueue": "true"},
    )


def test_approximate_counts(sqs, queue):
    for i in range(3):
        sqs.send_message(QueueUrl=queue, MessageBody=f"m{i}")
    a = queue_attrs(sqs, queue)
    assert a["ApproximateNumberOfMessages"] == "3"
    assert a["ApproximateNumberOfMessagesNotVisible"] == "0"
    msgs = receive(sqs, queue, MaxNumberOfMessages=1)
    assert len(msgs) == 1
    a = queue_attrs(sqs, queue)
    assert a["ApproximateNumberOfMessages"] == "2"
    assert a["ApproximateNumberOfMessagesNotVisible"] == "1"


# --- tags -----------------------------------------------------------------


def test_tags_roundtrip(sqs, queue):
    assert sqs.list_queue_tags(QueueUrl=queue).get("Tags", {}) == {}
    sqs.tag_queue(QueueUrl=queue, Tags={"env": "test", "team": "core"})
    assert sqs.list_queue_tags(QueueUrl=queue)["Tags"] == {"env": "test", "team": "core"}
    sqs.tag_queue(QueueUrl=queue, Tags={"env": "prod"})
    assert sqs.list_queue_tags(QueueUrl=queue)["Tags"] == {"env": "prod", "team": "core"}
    sqs.untag_queue(QueueUrl=queue, TagKeys=["env", "not-there"])
    assert sqs.list_queue_tags(QueueUrl=queue)["Tags"] == {"team": "core"}


def test_create_queue_with_tags(sqs, queues):
    url = queues.create("tagged", tags={"a": "1", "b": "two words"})
    assert sqs.list_queue_tags(QueueUrl=url)["Tags"] == {"a": "1", "b": "two words"}


# --- permissions ----------------------------------------------------------


def _policy_sids(sqs, url):
    p = queue_attrs(sqs, url, ["Policy"]).get("Policy")
    if not p:
        return {}
    doc = json.loads(p)
    return {s["Sid"]: s for s in doc.get("Statement", [])}


def test_add_remove_permission(sqs, queue):
    acct = account_of(queue)
    sqs.add_permission(QueueUrl=queue, Label="e2eSend", AWSAccountIds=[acct], Actions=["SendMessage"])
    sqs.add_permission(QueueUrl=queue, Label="e2eRecv", AWSAccountIds=[acct], Actions=["ReceiveMessage", "DeleteMessage"])
    sids = _policy_sids(sqs, queue)
    assert set(sids) == {"e2eSend", "e2eRecv"}
    stmt = sids["e2eSend"]
    assert stmt["Effect"] == "Allow"
    actions = stmt["Action"] if isinstance(stmt["Action"], list) else [stmt["Action"]]
    assert [a.lower() for a in actions] == ["sqs:sendmessage"]
    assert stmt["Resource"] == queue_attrs(sqs, queue, ["QueueArn"])["QueueArn"]
    assert acct in json.dumps(stmt["Principal"])

    sqs.remove_permission(QueueUrl=queue, Label="e2eSend")
    assert set(_policy_sids(sqs, queue)) == {"e2eRecv"}
    sqs.remove_permission(QueueUrl=queue, Label="e2eRecv")
    assert _policy_sids(sqs, queue) == {}


def test_add_permission_duplicate_label(sqs, queue):
    acct = account_of(queue)
    sqs.add_permission(QueueUrl=queue, Label="dup", AWSAccountIds=[acct], Actions=["SendMessage"])
    expect_error(
        sqs.add_permission, "InvalidParameterValue", QueueUrl=queue, Label="dup", AWSAccountIds=[acct], Actions=["SendMessage"]
    )


def test_remove_unknown_permission(sqs, queue):
    expect_error(sqs.remove_permission, "InvalidParameterValue", QueueUrl=queue, Label="nope")


# --- purge ----------------------------------------------------------------


def test_purge(sqs, queue):
    for i in range(5):
        sqs.send_message(QueueUrl=queue, MessageBody=f"m{i}")
    sqs.purge_queue(QueueUrl=queue)
    if TARGET_AWS:
        time.sleep(5)
    assert receive(sqs, queue, WaitTimeSeconds=1) == []
    assert queue_attrs(sqs, queue)["ApproximateNumberOfMessages"] == "0"
    # Messages sent after the purge are delivered normally.
    sqs.send_message(QueueUrl=queue, MessageBody="after")
    assert [m["Body"] for m in receive(sqs, queue, WaitTimeSeconds=2)] == ["after"]


@pytest.mark.qd_only
@pytest.mark.skipif(os.environ.get("QD_E2E_PURGE_COOLDOWN_ZERO") != "1", reason="server not started with cooldown 0")
def test_purge_twice_without_cooldown(sqs, queue):
    # run.sh starts the server with QD_PURGE_COOLDOWN_SECS=0 (AWS enforces 60s; see test_auth).
    sqs.purge_queue(QueueUrl=queue)
    sqs.purge_queue(QueueUrl=queue)
