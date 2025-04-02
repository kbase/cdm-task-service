
from bson.son import SON
import datetime
import pytest
from typing import Coroutine, Callable

from cdmtaskservice import models
from cdmtaskservice.mongo import MongoDAO, NoSuchJobError

from conftest import (
    mongo,  # @UnusedImport
    mondb,  # @UnusedImport
    MONGO_TEST_DB,
)

# TODO TEST add more tests

# Mongo only has millisecond precision
_SAFE_TIME = datetime.datetime(2025, 3, 31, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
_BASEJOB = models.AdminJobDetails(
    id="foo",
    job_input=models.JobInput(
        cluster=models.Cluster.PERLMUTTER_JAWS,
        image="some_image",
        params=models.Parameters(),
        input_files=[models.S3FileWithDataID(file="bucket/file")],
        output_dir="bucket/output",
    ),
    user="user",
    image=models.Image(
        name="some_image",
        digest="digest",
        entrypoint=["arg1"],
        registered_by="someuser",
        registered_on=_SAFE_TIME,
    ),
    input_file_count=1,
    state=models.JobState.DOWNLOAD_SUBMITTED,
    transition_times=[
        models.AdminJobStateTransition(
            state=models.JobState.CREATED,
            time=_SAFE_TIME,
            trans_id="trans1",
            notif_sent=False,
        ),
        models.AdminJobStateTransition(
            state=models.JobState.DOWNLOAD_SUBMITTED,
            time=_SAFE_TIME,
            trans_id="trans2",
            notif_sent=False,
        ),
    ]
)


@pytest.mark.asyncio
async def test_indexes(mongo, mondb):
    await MongoDAO.create(mondb)
    cols = mongo.client[MONGO_TEST_DB].list_collection_names()
    assert set(cols) == {"jobs", "refdata", "images"}
    jobindex = mongo.client[MONGO_TEST_DB]["jobs"].index_information()
    assert jobindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        "id_1": {"v": 2, "key": [("id", 1)], "unique": True},
        "transition_times.time_-1": {
            "v": 2,
            "key": [("transition_times.time", -1)],
            "partialFilterExpression": SON([("transition_times.notif_sent", False)])
        }
    }
    refindex = mongo.client[MONGO_TEST_DB]["refdata"].index_information()
    assert refindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        "id_1": {"v": 2, "key": [("id", 1)], "unique": True},
        "file_1": {"v": 2, "key": [("file", 1)]}
    }
    imgindex = mongo.client[MONGO_TEST_DB]["images"].index_information()
    assert imgindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        "UNIQUE_IMAGE_DIGEST_INDEX": {
            "v": 2,
            "key": [("name", 1), ("digest", 1)],
            "unique": True
        },
        "UNIQUE_IMAGE_TAG_INDEX": {
            "v": 2,
            "key": [("name", 1), ("tag", 1)],
            "unique": True,
            "sparse": True
        }
    }


@pytest.mark.asyncio
async def test_job_update_sent(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)
    
    job2 = _BASEJOB.model_copy(deep=True)
    job2.id = "bar"
    await mc.save_job(job2)
    
    await mc.job_update_sent("foo", "trans2")
    await mc.job_update_sent("bar", "trans1")
    
    got1 = await mc.get_job("foo", as_admin=True)
    got2 = await mc.get_job("bar", as_admin=True)
    
    expected1 = _BASEJOB.model_copy(deep=True)
    expected1.transition_times[1].notif_sent = True
    
    job2.transition_times[0].notif_sent = True
    
    assert got1 == expected1
    assert got2 == job2


@pytest.mark.asyncio
async def test_job_update_sent_fail_bad_args(mondb):
    mc = await MongoDAO.create(mondb)
    await _fail_job_update_sent(mc, None, "foo", ValueError("job_id is required"))
    await _fail_job_update_sent(mc, "   \t    ", "foo", ValueError("job_id is required"))
    await _fail_job_update_sent(mc, "foo", None, ValueError("trans_id is required"))
    await _fail_job_update_sent(mc, "foo", "    \t    ", ValueError("trans_id is required"))


@pytest.mark.asyncio
async def test_job_update_sent_fail_no_such_job(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)
    await _fail_job_update_sent(mc, "fooo", "trans1", NoSuchJobError(
        "No job with ID 'fooo' and state transition ID 'trans1' exists")
    )
    await _fail_job_update_sent(mc, "foo", "trans3", NoSuchJobError(
        "No job with ID 'foo' and state transition ID 'trans3' exists")
    )
    # check the job wasn't updated
    gotjob = await mc.get_job("foo", as_admin=True)
    assert gotjob == _BASEJOB


async def _fail_job_update_sent(mc: MongoDAO, job_id: str, trans_id: str, expected: Exception):
    with pytest.raises(type(expected), match=expected.args[0]):
        await mc.job_update_sent(job_id, trans_id)


@pytest.mark.asyncio
async def test_process_jobs_with_unsent_updates_noop(mondb):
    mc = await MongoDAO.create(mondb)
    job = _BASEJOB.model_copy(deep=True)
    for trans in job.transition_times:
        trans.notif_sent = True
    await mc.save_job(job)
    
    jobs = []
    dt = datetime.datetime(1985, 3, 31, 12, 0, 0, tzinfo=datetime.timezone.utc)
    async def collector(j: models.AdminJobDetails):
        jobs.append(j)
    count = await mc.process_jobs_with_unsent_updates(collector, dt)
    assert jobs == []
    assert count == 0


async def _set_up_jobs(mc: MongoDAO) -> tuple[models.AdminJobDetails]:
    old_dt = datetime.datetime(2000, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc)
    newer_dt = datetime.datetime(2020, 1, 1, 1, 1, 2, tzinfo=datetime.timezone.utc)
    
    # job should never show up, dates are very new
    await mc.save_job(_BASEJOB)
    
    # job should never show up even with old dates since messages are sent
    job1 = _BASEJOB.model_copy(deep=True)
    job1.id = "job1"
    for trans in job1.transition_times:
        trans.notif_sent = True
        trans.time = old_dt
    await mc.save_job(job1)
    
    # jobs with a very new unsent transition and old sent transition sholdn't show up
    job2 = _BASEJOB.model_copy(deep=True)
    job2.id = "job2"
    job2.transition_times[0].notif_sent = True
    job2.transition_times[0].time = old_dt
    await mc.save_job(job2)
    
    # should hit 2nd position
    job3 = _BASEJOB.model_copy(deep=True)
    job3.id = "job3"
    job3.transition_times[0].notif_sent = True
    job3.transition_times[0].time = old_dt
    job3.transition_times[1].time = newer_dt
    await mc.save_job(job3)
    
    # should hit 1st position
    job4 = _BASEJOB.model_copy(deep=True)
    job4.id = "job4"
    job4.transition_times[0].time = newer_dt
    job4.transition_times[1].notif_sent = True
    job4.transition_times[1].time = old_dt
    await mc.save_job(job4)
    
    # should hit both positions
    job5 = _BASEJOB.model_copy(deep=True)
    job5.id = "job5"
    job5.transition_times[0].time = newer_dt
    job5.transition_times[1].time = newer_dt
    await mc.save_job(job5)
    
    return job3, job4, job5


@pytest.mark.asyncio
async def test_process_jobs_with_unsent_updates(mondb):
    mc = await MongoDAO.create(mondb)
    jobs = {}
    async def collector(j: models.AdminJobDetails):
        jobs[j.id] = j
        
    job3, job4, job5 = await _set_up_jobs(mc)
    
    # check no jobs are found
    dt = datetime.datetime(2020, 1, 1, 1, 1, 2, tzinfo=datetime.timezone.utc)
    count = await mc.process_jobs_with_unsent_updates(collector, dt)
    assert jobs == {}
    assert count == 0
    
    # check expected jobs are found
    dt = datetime.datetime(2020, 1, 1, 1, 1, 3, tzinfo=datetime.timezone.utc)
    count = await mc.process_jobs_with_unsent_updates(collector, dt)
    assert jobs.keys() == {"job3", "job4", "job5"}
    assert count == 3
    assert jobs["job3"] == job3
    assert jobs["job4"] == job4
    assert jobs["job5"] == job5


@pytest.mark.asyncio
async def test_process_jobs_with_unset_updates_using_index(mondb):
    # This tests that the mongo query for the respective function uses the correct index.
    mc = await MongoDAO.create(mondb)
    await _set_up_jobs(mc)
    
    dt = datetime.datetime(2020, 1, 1, 1, 1, 3, tzinfo=datetime.timezone.utc)
    # This query is copied from the function and needs to be kept in sync
    sf = f"{models.FLD_COMMON_TRANS_TIMES}.{models.FLD_JOB_STATE_TRANSITION_NOTIFICATION_SENT}"
    query = {
        sf: False,
        models.FLD_COMMON_TRANS_TIMES:{
            "$elemMatch": {
                models.FLD_JOB_STATE_TRANSITION_NOTIFICATION_SENT: False,
                models.FLD_COMMON_STATE_TRANSITION_TIME: {"$lt": dt}
            }
        }
    }
    plan = await mondb.jobs.find(query).explain()
    index = plan["queryPlanner"]["winningPlan"]["inputStage"]
    assert index == {
        "stage": "IXSCAN",
        "keyPattern": {
            "transition_times.time": -1
        },
        "indexName": "transition_times.time_-1",
        "isMultiKey": True,
        "multiKeyPaths": {
            "transition_times.time": [
                "transition_times"
            ]
        },
        "isUnique": False,
        "isSparse": False,
        "isPartial": True,
        "indexVersion": 2,
        "direction": "forward",
        "indexBounds": {
            "transition_times.time": [
                "(new Date(1577840463000), new Date(-9223372036854775808)]"
            ]
        }
    }


@pytest.mark.asyncio
async def test_process_jobs_with_unsent_updates_fail_bad_args(mondb):
    mc = await MongoDAO.create(mondb)
    async def foo(j: models.AdminJobDetails):
        pass

    await _fail_process_jobs_with_unsent_updates(mc, None, _SAFE_TIME, ValueError(
        "processor is required")
    )
    await _fail_process_jobs_with_unsent_updates(mc, foo, None, ValueError(
        "older_than is required")
    )
    await _fail_process_jobs_with_unsent_updates(
        mc,
        foo,
        datetime.datetime.now(),
        ValueError("older_than must be a timezone aware datetime")
    )


async def _fail_process_jobs_with_unsent_updates(
    mc: MongoDAO,
    processor: Callable[[models.AdminJobDetails], Coroutine[None, None, None]],
    older_than: datetime.datetime,
    expected: Exception,
):
    with pytest.raises(type(expected), match=expected.args[0]):
        await mc.process_jobs_with_unsent_updates(processor, older_than)
