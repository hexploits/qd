"""Concurrent consumers: no duplicate delivery within the visibility timeout, FIFO group exclusivity."""

import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from conftest import make_client
from sqhelpers import receive


def _send_many(sqs, url, n, **kw):
    for start in range(0, n, 10):
        entries = [{"Id": str(i), "MessageBody": f"m{i}", **kw} for i in range(start, min(start + 10, n))]
        r = sqs.send_message_batch(QueueUrl=url, Entries=entries)
        assert not r.get("Failed")


def test_concurrent_receivers_no_duplicates(sqs, queue):
    n = 100
    _send_many(sqs, queue, n)
    seen = []
    lock = threading.Lock()

    def worker():
        c = make_client()
        empty = 0
        while empty < 2:
            msgs = receive(c, queue, WaitTimeSeconds=1)
            if not msgs:
                empty += 1
                continue
            with lock:
                seen.extend(m["MessageId"] for m in msgs)

    with ThreadPoolExecutor(8) as ex:
        for f in [ex.submit(worker) for _ in range(8)]:
            f.result()
    dupes = [k for k, v in Counter(seen).items() if v > 1]
    assert not dupes, f"{len(dupes)} messages delivered twice within visibility timeout"
    assert len(seen) == n


def test_concurrent_long_pollers_single_message(sqs, queue):
    results = []
    lock = threading.Lock()

    def poller():
        c = make_client()
        msgs = receive(c, queue, WaitTimeSeconds=4)
        with lock:
            results.extend(msgs)

    threads = [threading.Thread(target=poller) for _ in range(6)]
    for t in threads:
        t.start()
    time.sleep(1)
    sqs.send_message(QueueUrl=queue, MessageBody="only-one")
    for t in threads:
        t.join()
    assert [m["Body"] for m in results] == ["only-one"]


def test_fifo_concurrent_receivers_group_exclusive(sqs, queues):
    url = queues.create("fifoconc", fifo=True, attributes={"VisibilityTimeout": "30"})
    groups = [f"g{i}" for i in range(6)]
    for g in groups:
        entries = [
            {"Id": str(i), "MessageBody": f"{g}-{i}", "MessageGroupId": g, "MessageDeduplicationId": uuid.uuid4().hex}
            for i in range(5)
        ]
        assert not sqs.send_message_batch(QueueUrl=url, Entries=entries).get("Failed")
    responses = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def worker():
        c = make_client()
        barrier.wait()
        for _ in range(3):
            msgs = receive(c, url, MaxNumberOfMessages=3, AttributeNames=["MessageGroupId"])
            with lock:
                responses.append(msgs)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # Nothing is deleted, so every group may be handed to at most one receive call, and the
    # messages handed out for a group must be a prefix of that group's order.
    owner = {}
    per_group = {}
    for idx, msgs in enumerate(responses):
        for m in msgs:
            g = m["Attributes"]["MessageGroupId"]
            assert owner.setdefault(g, idx) == idx, f"group {g} delivered to two concurrent receives"
            per_group.setdefault(g, []).append(int(m["Body"].split("-")[1]))
    for g, seq in per_group.items():
        assert seq == list(range(len(seq))), (g, seq)
    assert per_group, "nothing received"
