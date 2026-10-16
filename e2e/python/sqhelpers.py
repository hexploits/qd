"""Shared helpers for the SQS end-to-end suite (independent re-implementations, no server code)."""

from __future__ import annotations

import hashlib
import json
import struct
import time
from typing import Callable, Iterable

from botocore.exceptions import ClientError


def md5_hex(data: bytes | str) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.md5(data).hexdigest()


def _enc(b: bytes) -> bytes:
    return struct.pack(">I", len(b)) + b


def md5_of_attributes(attrs: dict) -> str:
    """MD5 of message (or message system) attributes, per the documented SQS algorithm.

    For each attribute, sorted by name: len+name, len+DataType, 1-byte transport type
    (1 = string/number, 2 = binary), len+value bytes.
    """
    buf = b""
    for name in sorted(attrs):
        a = attrs[name]
        buf += _enc(name.encode("utf-8"))
        buf += _enc(a["DataType"].encode("utf-8"))
        if "StringValue" in a and a["StringValue"] is not None:
            buf += b"\x01" + _enc(a["StringValue"].encode("utf-8"))
        else:
            v = a["BinaryValue"]
            if isinstance(v, str):
                v = v.encode("utf-8")
            buf += b"\x02" + _enc(v)
    return hashlib.md5(buf).hexdigest()


def err_code(e: ClientError) -> str:
    return e.response.get("Error", {}).get("Code", "")


def expect_error(fn: Callable, *codes: str, **kwargs) -> ClientError:
    """Call fn(**kwargs) and assert it raises a ClientError whose code is one of `codes`."""
    try:
        fn(**kwargs)
    except ClientError as e:
        assert err_code(e) in codes, f"expected one of {codes}, got {err_code(e)!r}: {e.response.get('Error')}"
        return e
    raise AssertionError(f"expected ClientError {codes}, call succeeded")


def receive(sqs, url: str, **kw) -> list[dict]:
    kw.setdefault("MaxNumberOfMessages", 10)
    kw.setdefault("WaitTimeSeconds", 0)
    return sqs.receive_message(QueueUrl=url, **kw).get("Messages", [])


def receive_all(sqs, url: str, expected: int, timeout: float = 10, **kw) -> list[dict]:
    """Receive until `expected` messages were collected or timeout."""
    out: list[dict] = []
    deadline = time.time() + timeout
    while len(out) < expected and time.time() < deadline:
        kw.setdefault("WaitTimeSeconds", 1)
        out.extend(receive(sqs, url, **kw))
    return out


def wait_for(pred: Callable[[], object], timeout: float = 10, interval: float = 0.25, what: str = "condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = pred()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {what}; last={last!r}")


def queue_attrs(sqs, url: str, names: Iterable[str] = ("All",)) -> dict:
    return sqs.get_queue_attributes(QueueUrl=url, AttributeNames=list(names)).get("Attributes", {})


def queue_arn(sqs, url: str) -> str:
    return queue_attrs(sqs, url, ["QueueArn"])["QueueArn"]


def account_of(url: str) -> str:
    return url.rstrip("/").split("/")[-2]


def jloads(s: str) -> dict:
    return json.loads(s)
