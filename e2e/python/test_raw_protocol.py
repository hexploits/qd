"""Raw HTTP wire checks for request shapes boto3 does not produce but other SDKs do.

* Older SDKs (aws-sdk-java v1, aws-sdk-go v1, PHP, Ruby v2, ...) send Query requests to the
  *queue URL path* without a QueueUrl parameter, and some use GET with the query string.
* JSON error shape / x-amzn-query-error header / content types.

Unsigned requests only work with QD_AUTH_MODE=none, hence qd_only.
"""

import json
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

import pytest

from conftest import ENDPOINT

pytestmark = pytest.mark.qd_only

NS = "{http://queue.amazonaws.com/doc/2012-11-05/}"


def http(method, url, body=None, headers=None):
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def query_post(url, params):
    body = urllib.parse.urlencode({"Version": "2012-11-05", **params}).encode()
    return http("POST", url, body, {"Content-Type": "application/x-www-form-urlencoded; charset=utf-8"})


def json_post(target, payload):
    return http(
        "POST",
        ENDPOINT,
        json.dumps(payload).encode(),
        {"Content-Type": "application/x-amz-json-1.0", "X-Amz-Target": f"AmazonSQS.{target}"},
    )


def find(xml_bytes, tag):
    root = ET.fromstring(xml_bytes)
    return [e.text for e in root.iter() if e.tag in (tag, NS + tag)]


def test_query_post_to_queue_path(sqs, queue):
    st, _, body = query_post(queue, {"Action": "SendMessage", "MessageBody": "via-path"})
    assert st == 200, body
    assert find(body, "MessageId")
    st, _, body = query_post(queue, {"Action": "ReceiveMessage", "WaitTimeSeconds": "1"})
    assert st == 200, body
    assert find(body, "Body") == ["via-path"]


def test_query_get_with_querystring(sqs, queue):
    qs = urllib.parse.urlencode({"Action": "GetQueueAttributes", "AttributeName.1": "All", "Version": "2012-11-05"})
    st, headers, body = http("GET", f"{queue}?{qs}")
    assert st == 200, body
    assert "VisibilityTimeout" in find(body, "Name")
    qs = urllib.parse.urlencode({"Action": "ListQueues", "QueueNamePrefix": queue.rsplit("/", 1)[1]})
    st, _, body = http("GET", f"{ENDPOINT}/?{qs}")
    assert st == 200, body
    assert find(body, "QueueUrl") == [queue]


def test_query_error_shape(queue):
    st, headers, body = query_post(ENDPOINT + "/", {"Action": "GetQueueUrl", "QueueName": "definitely-missing-q"})
    assert st == 400
    assert "xml" in headers.get("Content-Type", headers.get("content-type", ""))
    assert find(body, "Code") == ["AWS.SimpleQueueService.NonExistentQueue"]
    assert find(body, "Type") == ["Sender"]
    assert find(body, "RequestId")


def test_query_unknown_action():
    st, _, body = query_post(ENDPOINT + "/", {"Action": "FlyToTheMoon"})
    assert st == 400
    assert find(body, "Code")[0] in ("InvalidAction", "UnsupportedOperation", "AWS.SimpleQueueService.UnsupportedOperation")


def test_json_error_shape():
    st, headers, body = json_post("GetQueueUrl", {"QueueName": "definitely-missing-q"})
    assert st == 400
    h = {k.lower(): v for k, v in headers.items()}
    assert h["content-type"].startswith("application/x-amz-json-1.0")
    assert h["x-amzn-query-error"] == "AWS.SimpleQueueService.NonExistentQueue;Sender"
    doc = json.loads(body)
    assert doc["__type"].endswith("QueueDoesNotExist")
    assert "message" in doc or "Message" in doc


def test_json_success_content_type(queue):
    st, headers, body = json_post("GetQueueAttributes", {"QueueUrl": queue, "AttributeNames": ["VisibilityTimeout"]})
    assert st == 200
    h = {k.lower(): v for k, v in headers.items()}
    assert h["content-type"].startswith("application/x-amz-json-1.0")
    assert json.loads(body) == {"Attributes": {"VisibilityTimeout": "30"}}
    assert h.get("x-amzn-requestid")


def test_json_unknown_operation():
    st, _, body = json_post("FlyToTheMoon", {})
    assert st == 400
    assert json.loads(body)["__type"].split("#")[-1] in ("UnknownOperationException", "InvalidAction")


def test_json_receive_empty_has_no_messages_key(queue):
    st, _, body = json_post("ReceiveMessage", {"QueueUrl": queue})
    assert st == 200
    assert "Messages" not in json.loads(body)
