
import pytest
from motor.motor_asyncio import AsyncIOMotorClient

from cdmtaskservice.mongo import MongoDAO, NoSuchJobError

from conftest import mongo_serv  # @UnusedImport
from cdmtaskservice import models
from cdmtaskservice import timestamp
import datetime

# TODO TEST add more tests

_TEST_DB = "testing"

_NOW = timestamp.utcdatetime()
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
        registered_on=_NOW,
    ),
    input_file_count=1,
    state=models.JobState.DOWNLOAD_SUBMITTED,
    transition_times=[
        models.AdminJobStateTransition(
            state=models.JobState.CREATED,
            time=_NOW,
            trans_id="trans1",
            notif_sent=False,
        ),
        models.AdminJobStateTransition(
            state=models.JobState.DOWNLOAD_SUBMITTED,
            time=_NOW,
            trans_id="trans2",
            notif_sent=False,
        ),
    ]
)


@pytest.fixture
def mongo(mongo_serv):
    mongo_serv.clear_database(_TEST_DB, drop_indexes=True)
    
    yield mongo_serv


@pytest.fixture()
def mondb(mongo):
    mcli = AsyncIOMotorClient(f"mongodb://localhost:{mongo.port}", tz_aware=True)

    yield mcli[_TEST_DB]

    mcli.close()


@pytest.mark.asyncio
async def test_indexes(mongo, mondb):
    await MongoDAO.create(mondb)
    cols = mongo.client[_TEST_DB].list_collection_names()
    assert set(cols) == {"jobs", "refdata", "images"}
    jobindex = mongo.client[_TEST_DB]["jobs"].index_information()
    assert jobindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        "id_1": {"v": 2, "key": [("id", 1)], "unique": True}
    }
    refindex = mongo.client[_TEST_DB]["refdata"].index_information()
    assert refindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        "id_1": {"v": 2, "key": [("id", 1)], "unique": True},
        "file_1": {"v": 2, "key": [("file", 1)]}
    }
    imgindex = mongo.client[_TEST_DB]["images"].index_information()
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
    _set_job_times(got1, _NOW)
    _set_job_times(got2, _NOW)
    
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
    _set_job_times(gotjob, _NOW)
    assert gotjob == _BASEJOB


def _set_job_times(job: models.AdminJobDetails, time_: datetime.datetime):
    # mongo uses its own tzinfo class. We don't care a bout tzinfo here so...
    job.image.registered_on = time_
    for t in job.transition_times:
        t.time = time_


async def _fail_job_update_sent(mc: MongoDAO, job_id: str, trans_id: str, expected: Exception):
    with pytest.raises(type(expected), match=expected.args[0]):
        await mc.job_update_sent(job_id, trans_id)
