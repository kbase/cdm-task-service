"""
Manages getting and updating job state.
"""

from cdmtaskservice import kb_auth
from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.exceptions import UnauthorizedError
from cdmtaskservice.mongo import MongoDAO

class JobState:
    """
    A manager for CDM job state.
    """
    
    def __init__(self, mongo: MongoDAO):
        """
        mongo - a MongoDB DAO object.
        """
        self._mongo = _not_falsy(mongo, "mongo")

    async def get_job(self, job_id: str, user: kb_auth.KBaseUser) -> models.Job:
        """
        Get a job based on its ID. If the provided user doesn't match the job's owner,
        an error is thrown.
        """
        # TODO ADMIN add way for admins to get any job, as_admin query param maybe
        _not_falsy(user, "user")
        job = await self._mongo.get_job(_require_string(job_id, "job_id"))
        if job.user != user.user:
            # reveals the job ID exists in the system but I don't see a problem with that
            raise UnauthorizedError(f"User {user.user} may not access job {job_id}")
        return job
