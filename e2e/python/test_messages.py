"""Send / receive / delete, checksums, message attributes, bodies."""

import os

import pytest

from conftest import PROTOCOL, TARGET_AWS
from sqhelpers import expect_error, md5_hex, md5_of_attributes, queue_attrs, receive, receive_all

MAX = 1048576

ATTRS = {
    "str": {"DataType": "String", "StringValue": "hello wörld ✓"},
    "num": {"DataType": "Number", "StringValue": "-12.50"},
    "bin": {"DataType": "Binary", "BinaryValue": bytes(range(256))},
    "custom": {"DataType": "String.custom", "StringValue": "v"},
    "numcustom": {"DataType": "Number.float", "StringValue": "3.14"},
    "bincustom": {"DataType": "Binary.png", "BinaryValue": b"\x89PNG\x00\xff"},
}


def test_send_receive_delete(sqs, queue):
    r = sqs.send_message(QueueUrl=queue, MessageBody="hello")
    assert r["MD5OfMessageBody"] == md5_hex("hello")
    assert r["MessageId"]
    assert "MD5OfMessageAttributes" not in r
    msgs = receive(sqs, queue, WaitTimeSeconds=1)
    assert len(msgs) == 1
    m = msgs[0]
    assert m["Body"] == "hello"
    assert m["MessageId"] == r["MessageId"]
    assert m["MD5OfBody"] == md5_hex("hello")
    assert m["ReceiptHandle"]
    sqs.delete_message(QueueUrl=queue, ReceiptHandle=m["ReceiptHandle"])
    # Deleting an already-deleted message with a well-formed handle succeeds (AWS is idempotent here).
    sqs.delete_message(QueueUrl=queue, ReceiptHandle=m["ReceiptHandle"])
    a = queue_attrs(sqs, queue)
    assert a["ApproximateNumberOfMessages"] == "0"
    assert a["ApproximateNumberOfMessagesNotVisible"] == "0"


def test_receive_empty_omits_messages(sqs, queue):
    r = sqs.receive_message(QueueUrl=queue)
    assert r.get("Messages", []) == []


def test_receive_max_number(sqs, queue):
    for i in range(15):
        sqs.send_message(QueueUrl=queue, MessageBody=f"m{i}")
    got = receive_all(sqs, queue, 15, timeout=10)
    assert sorted(m["Body"] for m in got) == sorted(f"m{i}" for i in range(15))
    assert all(len(receive(sqs, queue, MaxNumberOfMessages=n)) <= n for n in (1, 3))


def test_receive_one_by_default(sqs, queue):
    for i in range(3):
        sqs.send_message(QueueUrl=queue, MessageBody=f"m{i}")
    r = sqs.receive_message(QueueUrl=queue)
    assert len(r["Messages"]) == 1


@pytest.mark.parametrize(
    "kw",
    [{"MaxNumberOfMessages": 11}, {"MaxNumberOfMessages": 0}, {"WaitTimeSeconds": 21}, {"VisibilityTimeout": 43201}],
)
def test_receive_invalid_parameters(sqs, queue, kw):
    expect_error(sqs.receive_message, "InvalidParameterValue", QueueUrl=queue, **kw)


def test_delete_invalid_receipt_handle(sqs, queue):
    expect_error(sqs.delete_message, "ReceiptHandleIsInvalid", QueueUrl=queue, ReceiptHandle="garbage")


def test_system_attributes_on_receive(sqs, queue):
    sqs.send_message(QueueUrl=queue, MessageBody="x")
    m = receive(sqs, queue, WaitTimeSeconds=1, AttributeNames=["All"])[0]
    a = m["Attributes"]
    for k in ("SentTimestamp", "ApproximateReceiveCount", "ApproximateFirstReceiveTimestamp", "SenderId"):
        assert k in a, k
    assert a["ApproximateReceiveCount"] == "1"
    assert int(a["ApproximateFirstReceiveTimestamp"]) >= int(a["SentTimestamp"])


def test_select_system_attributes(sqs, queue):
    sqs.send_message(QueueUrl=queue, MessageBody="x")
    m = receive(sqs, queue, WaitTimeSeconds=1, AttributeNames=["SentTimestamp"])[0]
    assert set(m["Attributes"]) == {"SentTimestamp"}


@pytest.mark.json_only
def test_message_system_attribute_names_param(sqs, queue):
    members = sqs.meta.service_model.operation_model("ReceiveMessage").input_shape.members
    if "MessageSystemAttributeNames" not in members:
        pytest.skip("SDK too old for MessageSystemAttributeNames")
    sqs.send_message(QueueUrl=queue, MessageBody="x")
    m = receive(sqs, queue, WaitTimeSeconds=1, MessageSystemAttributeNames=["ApproximateReceiveCount", "SentTimestamp"])[0]
    assert set(m["Attributes"]) == {"ApproximateReceiveCount", "SentTimestamp"}


def test_message_attributes_md5_and_roundtrip(sqs, queue):
    r = sqs.send_message(QueueUrl=queue, MessageBody="body", MessageAttributes=ATTRS)
    expected = md5_of_attributes(ATTRS)
    assert r["MD5OfMessageAttributes"] == expected
    m = receive(sqs, queue, WaitTimeSeconds=1, MessageAttributeNames=["All"])[0]
    assert m["MD5OfMessageAttributes"] == expected
    got = m["MessageAttributes"]
    assert set(got) == set(ATTRS)
    for name, a in ATTRS.items():
        assert got[name]["DataType"] == a["DataType"]
        if "StringValue" in a:
            assert got[name]["StringValue"] == a["StringValue"]
        else:
            assert got[name]["BinaryValue"] == a["BinaryValue"]
    assert md5_of_attributes(got) == expected


@pytest.mark.parametrize("name", ["str", "num", "bin", "custom", "bincustom"])
def test_single_attribute_md5(sqs, queue, name):
    one = {name: ATTRS[name]}
    r = sqs.send_message(QueueUrl=queue, MessageBody="b", MessageAttributes=one)
    assert r["MD5OfMessageAttributes"] == md5_of_attributes(one)


def test_known_md5_vector():
    # Sanity-check our independent implementation against a fixed vector
    # (computed by hand from the documented algorithm).
    attrs = {"a": {"DataType": "String", "StringValue": "b"}}
    import hashlib

    raw = b"\x00\x00\x00\x01a" + b"\x00\x00\x00\x06String" + b"\x01" + b"\x00\x00\x00\x01b"
    assert md5_of_attributes(attrs) == hashlib.md5(raw).hexdigest()


def test_message_attribute_name_filters(sqs, queue):
    attrs = {
        "a.one": {"DataType": "String", "StringValue": "1"},
        "a.two": {"DataType": "String", "StringValue": "2"},
        "b": {"DataType": "String", "StringValue": "3"},
    }
    sqs.send_message(QueueUrl=queue, MessageBody="x", MessageAttributes=attrs)
    cases = [
        (["All"], {"a.one", "a.two", "b"}),
        ([".*"], {"a.one", "a.two", "b"}),
        (["a.*"], {"a.one", "a.two"}),
        (["b"], {"b"}),
        (["b", "a.one"], {"a.one", "b"}),
        (["nomatch"], set()),
        (None, set()),
    ]
    for names, expected in cases:
        kw = {"VisibilityTimeout": 0, "WaitTimeSeconds": 1}
        if names is not None:
            kw["MessageAttributeNames"] = names
        msgs = receive(sqs, queue, **kw)
        assert len(msgs) == 1
        got = set(msgs[0].get("MessageAttributes", {}))
        assert got == expected, (names, got)
        if expected:
            sub = {k: attrs[k] for k in expected}
            assert msgs[0]["MD5OfMessageAttributes"] == md5_of_attributes(sub), names


def test_aws_trace_header_roundtrip(sqs, queue):
    trace = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
    sys_attrs = {"AWSTraceHeader": {"DataType": "String", "StringValue": trace}}
    r = sqs.send_message(QueueUrl=queue, MessageBody="traced", MessageSystemAttributes=sys_attrs)
    assert r.get("MD5OfMessageSystemAttributes") == md5_of_attributes(sys_attrs)
    m = receive(sqs, queue, WaitTimeSeconds=1, AttributeNames=["AWSTraceHeader"])[0]
    assert m["Attributes"]["AWSTraceHeader"] == trace
    # Not returned unless requested.
    sqs.change_message_visibility(QueueUrl=queue, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
    m = receive(sqs, queue, WaitTimeSeconds=1)[0]
    assert "AWSTraceHeader" not in m.get("Attributes", {})


def test_unicode_body(sqs, queue):
    body = "héllo 世界 🚀 \t\n\r end \U0010FFFF "
    r = sqs.send_message(QueueUrl=queue, MessageBody=body)
    assert r["MD5OfMessageBody"] == md5_hex(body)
    m = receive(sqs, queue, WaitTimeSeconds=1)[0]
    assert m["Body"] == body
    assert m["MD5OfBody"] == md5_hex(body)


@pytest.mark.parametrize("bad", ["\x00", "\x01", "a\x08b", "￿", "￾"])
def test_invalid_characters(sqs, queue, bad):
    expect_error(sqs.send_message, "InvalidMessageContents", QueueUrl=queue, MessageBody=bad)


def test_empty_body_rejected(sqs, queue):
    expect_error(sqs.send_message, "MissingParameter", "InvalidParameterValue", QueueUrl=queue, MessageBody="")


def test_max_size_body(sqs, queue):
    body = "x" * MAX
    r = sqs.send_message(QueueUrl=queue, MessageBody=body)
    assert r["MD5OfMessageBody"] == md5_hex(body)
    m = receive(sqs, queue, WaitTimeSeconds=1)[0]
    assert len(m["Body"]) == MAX
    assert m["MD5OfBody"] == md5_hex(body)


def test_max_size_multibyte_body(sqs, queue):
    # Size is measured in UTF-8 bytes: 4-byte chars.
    body = "🚀" * (MAX // 4)
    sqs.send_message(QueueUrl=queue, MessageBody=body)
    expect_error(sqs.send_message, "InvalidParameterValue", QueueUrl=queue, MessageBody=body + "x")


def test_too_large_body(sqs, queue):
    expect_error(sqs.send_message, "InvalidParameterValue", QueueUrl=queue, MessageBody="x" * (MAX + 1))


def test_size_includes_attributes(sqs, queue):
    attrs = {"k": {"DataType": "String", "StringValue": "v" * 100}}
    expect_error(
        sqs.send_message,
        "InvalidParameterValue",
        QueueUrl=queue,
        MessageBody="x" * (MAX - 50),
        MessageAttributes=attrs,
    )


def test_queue_maximum_message_size_enforced(sqs, queues):
    url = queues.create("small", attributes={"MaximumMessageSize": "1024"})
    sqs.send_message(QueueUrl=url, MessageBody="x" * 1024)
    expect_error(sqs.send_message, "InvalidParameterValue", QueueUrl=url, MessageBody="x" * 1025)


def test_binary_attribute_roundtrip_all_bytes(sqs, queue):
    payload = os.urandom(4096) + bytes(range(256))
    attrs = {"blob": {"DataType": "Binary", "BinaryValue": payload}}
    r = sqs.send_message(QueueUrl=queue, MessageBody="b", MessageAttributes=attrs)
    assert r["MD5OfMessageAttributes"] == md5_of_attributes(attrs)
    m = receive(sqs, queue, WaitTimeSeconds=1, MessageAttributeNames=["All"])[0]
    assert m["MessageAttributes"]["blob"]["BinaryValue"] == payload
    assert m["MessageAttributes"]["blob"]["DataType"] == "Binary"


def test_too_many_message_attributes(sqs, queue):
    attrs = {f"a{i}": {"DataType": "String", "StringValue": "v"} for i in range(11)}
    expect_error(sqs.send_message, "InvalidParameterValue", QueueUrl=queue, MessageBody="x", MessageAttributes=attrs)


@pytest.mark.parametrize("name", ["AWS.reserved", "amazon.reserved", "bad name", ".lead", "trail.", "a..b"])
def test_invalid_message_attribute_names(sqs, queue, name):
    attrs = {name: {"DataType": "String", "StringValue": "v"}}
    expect_error(sqs.send_message, "InvalidParameterValue", QueueUrl=queue, MessageBody="x", MessageAttributes=attrs)


def test_invalid_number_attribute(sqs, queue):
    attrs = {"n": {"DataType": "Number", "StringValue": "not-a-number"}}
    expect_error(sqs.send_message, "InvalidParameterValue", QueueUrl=queue, MessageBody="x", MessageAttributes=attrs)


def test_invalid_attribute_data_type(sqs, queue):
    attrs = {"n": {"DataType": "Integer", "StringValue": "1"}}
    expect_error(sqs.send_message, "InvalidParameterValue", QueueUrl=queue, MessageBody="x", MessageAttributes=attrs)


def test_send_delay_out_of_range(sqs, queue):
    expect_error(sqs.send_message, "InvalidParameterValue", QueueUrl=queue, MessageBody="x", DelaySeconds=901)


def test_protocol_sanity(protocol):
    assert protocol == PROTOCOL in ("json", "query")
    if not TARGET_AWS:
        assert os.environ.get("QD_E2E_ENDPOINT")
