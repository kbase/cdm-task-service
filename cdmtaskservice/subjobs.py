"""
Methods and classes for dealing the handling subjobs.
"""

from dataclasses import dataclass
import datetime

from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.mongo import MongoDAO


@dataclass
class JobUpdate:
    """
    An update to a job state based on all its subjobs reaching equivalent states.
    """
    
    state: models.JobState
    """ The new state for the job. """

    time: datetime.datetime
    """ The time at which the last subjob transitioned into the equivalent state. """
    


def _check_count_is_valid(st: models.JobState, state_count: int, all_count: int, ttl_count: int):
    if all_count > ttl_count:
        raise ValueError(f"More subjobs found ({all_count}) than containers ({ttl_count})")
    if state_count < 1:
        raise ValueError(
            f"You reported that a subjob transitioned to state {st.value} but no "
            + "subjobs are in that state"
        )


async def _check_equiv_states_complete(
    md: MongoDAO,
    job: models.Job,
    target_state: models.JobState,
    stdstate: models.JobState,
    errstate: models.JobState,
) -> JobUpdate | None:
    counts = await md.have_subjobs_reached_state(job.id, stdstate, errstate)
    ttl = sum(c[0] for c in counts.values())
    _check_count_is_valid(target_state, counts[target_state][0], ttl, job.job_input.num_containers)
    if ttl != job.job_input.num_containers:
        return None
    t = max(c[1] for c in counts.values() if c[1])
    return JobUpdate(errstate, t) if counts[errstate][0] > 0 else JobUpdate(stdstate, t)


async def get_job_update(
    md: MongoDAO,
    job: models.Job,
    subjob_transition: models.JobState,
) -> JobUpdate | None:
    """
    Determine whether a subjob transition should trigger a transition for the main job.
    
    md - the Mongo instance to query.
    job - the main job.
    subjob_transition - the transition the subjob underwent.
    
    Returns the job update information or None if the job should not yet be transitioned.
    Transitions are determined based on whether all the subjobs have reached the same state
    or an equivalent state for error states.
    """
    # Not liking this implementation, it has to know too much about equivalent job states
    # Making it work for now, maybe can be refactored later
    
    _not_falsy(md, "md")
    _not_falsy(job, "job")
    st = _not_falsy(subjob_transition, "subjob_transition")
    js = models.JobState
    if st in {js.CREATED, js.DOWNLOAD_SUBMITTED, js.JOB_SUBMITTING, js.JOB_SUBMITTED}:
        stcount = (await md.have_subjobs_reached_state(job.id, st))[st]
        _check_count_is_valid(st, stcount[0], stcount[0], job.job_input.num_containers)
        return JobUpdate(st, stcount[1]) if stcount[0] == job.job_input.num_containers else None
    if st in {js.UPLOAD_SUBMITTING, js.ERROR_PROCESSING_SUBMITTING}:
        return await _check_equiv_states_complete(
            md, job, st, js.UPLOAD_SUBMITTING, js.ERROR_PROCESSING_SUBMITTING
        )
    if st in {js.UPLOAD_SUBMITTED, js.ERROR_PROCESSING_SUBMITTED}:
        return await _check_equiv_states_complete(
            md, job, st, js.UPLOAD_SUBMITTED, js.ERROR_PROCESSING_SUBMITTED
        )
    if st.is_terminal():
        # TODO CANCEL_JOBS will need to do something different here
        return await _check_equiv_states_complete(md, job, st, js.COMPLETE, js.ERROR)
    raise ValueError("Seems like someone added a state without updating this method, oops")
