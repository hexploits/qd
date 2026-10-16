"""SigV4 authentication against a second qd instance (started by run.sh with
QD_AUTH_MODE=sigv4 and the default purge cooldown)."""

import os
import uuid

import pytest

from conftest import make_client
from sqhelpers import err_code, expect_error

SIGV4_ENDPOINT = os.environ.get("QD_E2E_SIGV4_ENDPOINT")
SIGV4_CREDS = os.environ.get("QD_E2E_SIGV4_CREDS", "")

pytestmark = [
    pytest.mark.qd_only,
    pytest.mark.skipif(not (SIGV4_ENDPOINT and SIGV4_CREDS.count(":") == 2), reason="no sigv4 server configured"),
]


@pytest.fixture(scope="module")
def creds():
    key, secret, account = SIGV4_CREDS.split(":")
    return key, secret, account


@pytest.fixture(scope="module")
def authed(creds):
    return make_client(endpoint=SIGV4_ENDPOINT, key=creds[0], secret=creds[1])


@pytest.fixture
def authed_queue(authed):
    url = authed.create_queue(QueueName=f"e2e-auth-{uuid.uuid4().hex[:10]}")["QueueUrl"]
    yield url
    try:
        authed.delete_queue(QueueUrl=url)
    except Exception:
        pass


def test_valid_credentials(authed, authed_queue, creds):
    assert f"/{creds[2]}/" in authed_queue
    authed.send_message(QueueUrl=authed_queue, MessageBody="signed")
    msgs = authed.receive_message(QueueUrl=authed_queue, WaitTimeSeconds=1).get("Messages", [])
    assert [m["Body"] for m in msgs] == ["signed"]
    arn = authed.get_queue_attributes(QueueUrl=authed_queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    assert f":{creds[2]}:" in arn


def test_wrong_secret(creds):
    bad = make_client(endpoint=SIGV4_ENDPOINT, key=creds[0], secret=creds[1] + "x")
    expect_error(bad.list_queues, "SignatureDoesNotMatch")


def test_unknown_access_key(creds):
    bad = make_client(endpoint=SIGV4_ENDPOINT, key="AKIDUNKNOWN000000000", secret="whatever")
    expect_error(bad.list_queues, "InvalidClientTokenId")


def test_wrong_secret_on_send(authed_queue, creds):
    bad = make_client(endpoint=SIGV4_ENDPOINT, key=creds[0], secret="nope")
    e = expect_error(bad.send_message, "SignatureDoesNotMatch", QueueUrl=authed_queue, MessageBody="x")
    assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 403


def test_unsigned_request_rejected(creds):
    from botocore import UNSIGNED
    from botocore.config import Config
    import boto3

    c = boto3.client("sqs", endpoint_url=SIGV4_ENDPOINT, region_name="us-east-1", config=Config(signature_version=UNSIGNED))
    e = expect_error(c.list_queues, "MissingAuthenticationToken", "IncompleteSignature", "InvalidClientTokenId", "AccessDenied")
    assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 403)


def test_purge_in_progress(authed, authed_queue):
    # This server runs with the default (AWS-like) 60s purge cooldown.
    authed.purge_queue(QueueUrl=authed_queue)
    with pytest.raises(authed.exceptions.PurgeQueueInProgress) as ei:
        authed.purge_queue(QueueUrl=authed_queue)
    assert err_code(ei.value) == "AWS.SimpleQueueService.PurgeQueueInProgress"
