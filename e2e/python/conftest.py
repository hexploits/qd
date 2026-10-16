"""Black-box SQS e2e fixtures.

Target selection:
  QD_E2E_ENDPOINT=http://127.0.0.1:9324   -> any SQS-compatible endpoint (dummy credentials)
  QD_E2E_TARGET=aws                       -> real AWS SQS (default credential chain / region);
                                             tests marked qd_only are skipped.
Optional (provided by run.sh):
  QD_E2E_SIGV4_ENDPOINT, QD_E2E_SIGV4_CREDS="AKID:SECRET:ACCOUNT" -> second server in sigv4 mode
"""

from __future__ import annotations

import os
import uuid

import boto3
import botocore
import pytest
from botocore.config import Config

TARGET_AWS = os.environ.get("QD_E2E_TARGET", "").lower() == "aws"
ENDPOINT = os.environ.get("QD_E2E_ENDPOINT") or None
REGION = os.environ.get("QD_E2E_REGION") or ("us-east-1" if not TARGET_AWS else None)

if not ENDPOINT and not TARGET_AWS:
    raise pytest.UsageError("set QD_E2E_ENDPOINT=<url> (or QD_E2E_TARGET=aws to run against real AWS SQS)")


def client_config(**kw) -> Config:
    base = dict(
        retries={"total_max_attempts": 1, "mode": "standard"},
        read_timeout=60,
        connect_timeout=5,
    )
    base.update(kw)
    return Config(**base)


def make_client(endpoint=ENDPOINT, key="test", secret="test", region=REGION, **kw):
    if TARGET_AWS and endpoint is None:
        return boto3.client("sqs", region_name=region, config=client_config(**kw))
    return boto3.client(
        "sqs",
        endpoint_url=endpoint,
        region_name=region or "us-east-1",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        config=client_config(**kw),
    )


_probe = boto3.client("sqs", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x")
PROTOCOL = _probe.meta.service_model.protocol  # "json" or "query"


def pytest_report_header(config):
    return [
        f"sqs-e2e: protocol={PROTOCOL} boto3={boto3.__version__} botocore={botocore.__version__}",
        f"sqs-e2e: target={'aws' if TARGET_AWS else ENDPOINT}",
    ]


def pytest_collection_modifyitems(config, items):
    for item in items:
        # Make every test id / report line show which wire protocol ran.
        item._nodeid = f"{item._nodeid}[{PROTOCOL}]"
        if TARGET_AWS and item.get_closest_marker("qd_only"):
            item.add_marker(pytest.mark.skip(reason="qd only"))
        if item.get_closest_marker("json_only") and PROTOCOL != "json":
            item.add_marker(pytest.mark.skip(reason="json protocol only"))
        if item.get_closest_marker("query_only") and PROTOCOL != "query":
            item.add_marker(pytest.mark.skip(reason="query protocol only"))


@pytest.fixture(scope="session")
def protocol():
    return PROTOCOL


@pytest.fixture(scope="session")
def sqs():
    return make_client()


@pytest.fixture(scope="session")
def run_id():
    return uuid.uuid4().hex[:8]


class QueueFactory:
    def __init__(self, sqs, run_id):
        self.sqs = sqs
        self.run_id = run_id
        self.urls: list[str] = []

    def name(self, base: str = "q", fifo: bool = False) -> str:
        n = f"e2e-{self.run_id}-{base}-{uuid.uuid4().hex[:8]}"
        return n + ".fifo" if fifo else n

    def create(self, base: str = "q", fifo: bool = False, attributes: dict | None = None, **kw) -> str:
        attrs = dict(attributes or {})
        if fifo:
            attrs.setdefault("FifoQueue", "true")
        name = kw.pop("name", None) or self.name(base, fifo)
        args = {"QueueName": name, **kw}
        if attrs:
            args["Attributes"] = attrs
        url = self.sqs.create_queue(**args)["QueueUrl"]
        self.urls.append(url)
        return url

    def track(self, url: str) -> str:
        self.urls.append(url)
        return url

    def cleanup(self):
        for url in reversed(self.urls):
            try:
                self.sqs.delete_queue(QueueUrl=url)
            except Exception:
                pass


@pytest.fixture
def queues(sqs, run_id):
    f = QueueFactory(sqs, run_id)
    yield f
    f.cleanup()


@pytest.fixture
def queue(queues):
    return queues.create("std")


@pytest.fixture
def fifo_queue(queues):
    return queues.create("fifo", fifo=True)
