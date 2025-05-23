from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
import asyncio
from datetime import datetime, timezone
import pytest
import re

from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice.models import JobState

from conftest import kafka  # @UnusedImport
from utils import find_free_port


@pytest.mark.asyncio
async def test_create_fail_bad_args():
    await _fail_create(None, "topic", ValueError("bootstrap_servers is required"))
    await _fail_create("   \t   ", "topic", ValueError("bootstrap_servers is required"))
    await _fail_create("localhost:10000", None, ValueError("topic is required"))
    await _fail_create("localhost:10000", "    \t    ", ValueError("topic is required"))
    # TODO TEST add test for topic > 249 chars
    port = find_free_port()
    await _fail_create(f"localhost:{port}", "topic", KafkaConnectionError(
        re.escape(f"KafkaConnectionError: Unable to bootstrap from [('localhost', {port}, "
        + "<AddressFamily.AF_UNSPEC: 0>)]")))
    
    for c in ['Ѽ', '_', '.', '*']:
        await _fail_create("localhost:10000", f'topic{c}topic', ValueError(
            re.escape(f'Illegal character in Kafka topic topic{c}topic: {c}')))


async def _fail_create(bootstrap: str, topic: str, expected: Exception):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await KafkaNotifier.create(bootstrap, topic)


@pytest.mark.asyncio
async def test_send(kafka):
    kn = await KafkaNotifier.create(f"localhost:{kafka.port}", "topichere")
    passed = set()
    async def cb():
        passed.add("pass")
    await kn.update_job_state(
        "id1",
        JobState.CREATED,
        datetime(2024, 3, 24, 12, 0, 0, tzinfo=timezone.utc),
        "trans_id1",
        callback=cb()
    )
    # test with no callback
    await kn.update_job_state(
        "id2",
        JobState.DOWNLOAD_SUBMITTED,
        datetime(2025, 7, 24, 12, 0, 0, tzinfo=timezone.utc),
        "trans_id2"
    )
    await _check_send_results(kafka.port, "topichere")
    assert passed == {"pass"}
    # reaching into the implementation is bad, but no need for adding an access method
    assert len(kn._futures) == 0
    assert len(kn._tasks) == 0
    await kn.close()


@pytest.mark.asyncio
async def test_send_with_recovery(kafka):
    kn = await KafkaNotifier.create(f"localhost:{kafka.port}", "topichere")
    passed = set()
    async def cb(add: str):
        passed.add(add)
    await kn.update_job_state(
        "id1",
        JobState.CREATED,
        datetime(2024, 3, 24, 12, 0, 0, tzinfo=timezone.utc),
        "trans_id1",
        callback=cb("1"),
    )
    await asyncio.sleep(1)  # wait for message to send and future to be removed
    try:
        kafka.pause()
        await kn.update_job_state(
            "id2",
            JobState.DOWNLOAD_SUBMITTED,
            datetime(2025, 7, 24, 12, 0, 0, tzinfo=timezone.utc),
            "trans_id2",
            callback=cb("2"),
        )
        await asyncio.sleep(2)
        # reaching into the implementation is bad, but no need for adding an access method
        assert len(kn._futures) == 1
        assert len(kn._tasks) == 0
    # Now test that we recover and the message still gets sent
    finally:
        kafka.unpause()
    await _check_send_results(kafka.port, "topichere")
    assert passed == {"1", "2"}
    # reaching into the implementation is bad, but no need for adding an access method
    assert len(kn._futures) == 0
    assert len(kn._tasks) == 0
    await kn.close()


async def _check_send_results(port, topic):
    kc = AIOKafkaConsumer(
        topic,
        bootstrap_servers=f"localhost:{port}",
        auto_offset_reset="earliest"
    )
    await kc.start()
    res1 = await kc.getone()
    res2 = await kc.getone()
    assert res1.topic == topic
    assert res2.topic == topic
    if b"id1" not in res1.value:
        # messages aren't guaranteed to be in any particular order
        temp = res1
        res1 = res2
        res2 = temp
    assert res1.value == (
        b'{"job_id": "id1", "state": "created", "time": "2024-03-24T12:00:00+00:00", '
        + b'"trans_id": "trans_id1"}'
    )
    assert res2.value == (
        b'{"job_id": "id2", "state": "download_submitted", "time": "2025-07-24T12:00:00+00:00", '
        + b'"trans_id": "trans_id2"}'
    )
    await kc.stop()


@pytest.mark.asyncio
async def test_fail_send_on_close(kafka):
    kn = await KafkaNotifier.create(f"localhost:{kafka.port}", "topichere")
    await kn.close()
    with pytest.raises(ValueError, match="client is closed"):
        await kn.update_job_state("id", JobState.DOWNLOAD_SUBMITTED, datetime.now(), "foo")
    await kn.close()
