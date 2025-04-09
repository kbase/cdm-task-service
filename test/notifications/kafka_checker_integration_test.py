from aiokafka import AIOKafkaConsumer
import asyncio
import datetime
import json
import pytest

from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.notifications.kafka_checker import KafkaChecker
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier

from conftest import (
    mondb,  # @UnusedImport
    kafka,  # @UnusedImport
)
from mongo_test import set_up_jobs

# The integration tests cover the behavior of the checker, so unit tests aren't helpful in
# this case.


@pytest.mark.asyncio
async def test_checker_noop(mondb, kafka):
    mc = await MongoDAO.create(mondb)
    kn = await KafkaNotifier.create(f"localhost:{kafka.port}", "mytopic")
    kc = KafkaChecker(mc, kn)
    dt = datetime.datetime(2030, 1, 1, 1, 1, 2, tzinfo=datetime.timezone.utc)
    jobs, updates = await kc.check(dt)
    assert jobs == 0
    assert updates == 0

    await kn.close()


@pytest.mark.asyncio
async def test_checker(mondb, kafka):
    mc = await MongoDAO.create(mondb)
    kn = await KafkaNotifier.create(f"localhost:{kafka.port}", "mytopic")
    kc = KafkaChecker(mc, kn)
    job3, job4, job5 = await set_up_jobs(mc)
    
    # check no updates
    dt = datetime.datetime(2020, 1, 1, 1, 1, 2, tzinfo=datetime.timezone.utc)
    jobs, updates = await kc.check(dt)
    assert jobs == 0
    assert updates == 0
    
    dt = datetime.datetime(2020, 1, 1, 1, 1, 3, tzinfo=datetime.timezone.utc)
    jobs, updates = await kc.check(dt)
    assert jobs == 3
    assert updates == 4
    await asyncio.sleep(0.2)  # wait for tasks to stop running
    
    got3 = await mc.get_job(job3.id, as_admin=True)
    got4 = await mc.get_job(job4.id, as_admin=True)
    got5 = await mc.get_job(job5.id, as_admin=True)

    # update jobs to expected state
    job3.transition_times[1].notif_sent = True
    job4.transition_times[0].notif_sent = True
    job5.transition_times[0].notif_sent = True
    job5.transition_times[1].notif_sent = True
    
    assert got3 == job3
    assert got4 == job4
    assert got5 == job5
    
    kc = AIOKafkaConsumer(
        "mytopic",
        bootstrap_servers=f"localhost:{kafka.port}",
        auto_offset_reset="earliest"
    )
    await kc.start()
    res = {}
    for _ in range(4):
        r = await kc.getone()
        assert r.topic == "mytopic"
        j = json.loads(r.value.decode("utf-8"))
        res[f"{j['job_id']}_{j['trans_id']}"] = j
        
    assert res == {
        "job3_trans2": {
            "job_id": "job3",
            "trans_id": "trans2",
            "state": "download_submitted",
            "time": "2020-01-01T01:01:02+00:00"
        },
        "job4_trans1": {
            "job_id": "job4",
            "trans_id": "trans1",
            "state": "created",
            "time": "2020-01-01T01:01:02+00:00"
        },
        "job5_trans1": {
            "job_id": "job5",
            "trans_id": "trans1",
            "state": "created",
            "time": "2020-01-01T01:01:02+00:00"
        },
        "job5_trans2": {
            "job_id": "job5",
            "trans_id": "trans2",
            "state": "download_submitted",
            "time": "2020-01-01T01:01:02+00:00"
        },
    }
    await kc.stop()
    await kn.close()


@pytest.mark.asyncio
async def test_construct_fail(mondb, kafka):
    mc = await MongoDAO.create(mondb)
    kn = await KafkaNotifier.create(f"localhost:{kafka.port}", "mytopic")
    
    _fail_construct(None, kn, ValueError("mongo is required"))
    _fail_construct(mc, None, ValueError("kafka is required"))
    
    await kn.close()


def _fail_construct(mc: MongoDAO, kn: KafkaNotifier, expected: Exception):
    with pytest.raises(type(expected), match=expected.args[0]):
        KafkaChecker(mc, kn)


@pytest.mark.asyncio
async def test_checker_fail_bad_args(mondb, kafka):
    mc = await MongoDAO.create(mondb)
    kn = await KafkaNotifier.create(f"localhost:{kafka.port}", "mytopic")
    kc = KafkaChecker(mc, kn)
    
    await _fail_checker(kc, None, ValueError("older_than is required"))
    await _fail_checker(kc, datetime.datetime.now(), ValueError(
        "older_than must be a timezone aware datetime")
    )
    await kn.close()


async def _fail_checker(kc: KafkaChecker, dt: datetime.datetime, expected: Exception):
    with pytest.raises(type(expected), match=expected.args[0]):
        await kc.check(dt)
