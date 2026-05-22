# Job recovery for the `kbase` site

## Goal

Add an admin job recovery endpoint for the `kbase` site that restarts any held containers and
updates the job state appropriately.

* Why not a user recovery endpoint?
  * Virtually all of the job errors encountered thus far require changes before the jobs can be
    run again; configuration issues, container problems, command line errors, etc. Just blindly
    restarting the job is unlikely to help in most cases. For things like transient errors
    retries should be built into the job executor / server.
* Why the `kbase` site?
  * The `kbase` site is simpler than the JAWS based sites (although still complicated) as each
    container either completed successfully or needs to be restarted from scratch.

## Nomenclature

* CTS - CDM Task Service
* HTC - HTCondor

## Issues to account for

* 2+ simultaneous recovery requests - use the RECOVERING job state described below

## Issues not to account for

* If a cluster process is held in HTC we assume it's not running anywhere else and
  requests to the CTS will not be made for that process
  * It's possible some jerk may have gotten access to the container token and is making bogus
    requests but if that's the case all bets are off. The point of the container token to to
    prevent bogus requests
* We assume a held state in HTC means the container did not complete successfully. It's
  theoretically possible that a container reported a successful completion to the server but
  then failed and was held, but virtually nothing occurs in the external executor code between
  updating the final state and exiting so we don't account for this possibility
* We assume that a HTC process will stay in the held state until the recover method releases it.
  If someone is altering CTS owned HTC clusters without going through the API all bets are off

## On a recovery request

* If the job has completed successfully, is in recovery, or is in a cancel state,
  return an informative error
  * May want a new error type, TBD
* If there is no HTC cluster ID in the job record throw an informative error
  * Either the job is so new it hasn't yet been submitted -OR-
  * The submission failed, in which case it makes more sense to just make a new job. The point
    of job recovery is to finish partially complete jobs where 1 or more of the containers
    succeeded and results were uploaded
  * If this occurs frequently could revisit
* If there are no held or running processes in HTC for the job cluster
  * This implies the job completed successfully but the main job state was not updated
  * If the job is not on the standard job state path (CREATED -> COMPLETED) throw a 500 error,
    something is broken
    * Unless it's moved to a cancel state, then throw a 4XX error
  * Determine the set of state updates that need to be applied to the main job,
    e.g. from current job state through to the COMPLETED state.
  * For each update in order, call `subjobs.get_job_update()` to get the update information
    and apply the update.
    * For the completed state transition, standard updates need to be done (set output files,
      cpu usage, etc.)
  * If an update fails, that presumably means that another recovery call is ongoing and has taken
    over responsibility for the updates, the job has started canceling,
    or something is very wrong. In any case throw an error and do not update the job state.
* If there are running but no held processes in HTC, there is nothing to do, throw an
  informative error.
* Otherwise, set the job to the new RECOVERING state
  * The job cannot transition to the RECOVERING state from the COMPLETED state, either of the
    CANCEL states, or the RECOVERING state - this should throw an error
  * No state transitions are allowed from the RECOVERING state
    * Even canceling the job - this keeps the logic simpler
  * Once a server instance + coroutine has transitioned the job to the RECOVERING state, it has
    effectively secured a lock on the job state updates
  * Send a notification for the state change
  * For the held containers / subjobs:
    * Append the current transition times to a new array, `trans_times_historical`
      (exact name TBD) including a final RECOVERING state, set the transition times
      array to a single new DOWNLOAD_SUBMITTED event, set the state to DOWNLOAD_SUBMITTED,
      and clear any errors and exit codes
      * The new array is visible to users in the API
      * This probably requires a new mongo DAO method
        * See appendix 1 for implementation notes
      * This allows the subjob.get_job_update code to remain unchanged, as the historical
        events are moved to the new array rather than staying in the active array
        * Also more readable for users, since the current job state transitions are emphasized
  * Restart the held jobs in HTCondor
    * Should just be one action / Release call with the cluster ID as that will only affect
      held jobs
    * Throw an informative 4XX error if this fails and leave the job in the RECOVERING state.
      * Should generally never happen; will need HTC rebugging and a force recovery attempt
        (see below)
  * For the main job, append the current transition times to a new array with the same name as
    the subjobs array, set the transition times array to a single new DOWNLOAD_SUBMITTED event,
    set the state to DOWNLOAD_SUBMITTED, set the cleaned flag to false, and clear any errors
    * See the notes for the subjob transition
    * Theoretically the containers could all transition to JOB_SUBMITTING before this
      happens and cause an update failure. That seems very unlikely, so don't account for it
      for now (but do add explanatory comments)
  * The job should now run normally

## On a recovery failure

If the recovery operation fails prior to updating the main job, the job would be stuck in the
RECOVERING state.

* Add a force parameter (default false) to the API
* Whenever setting the job to the RECOVERING state, add a `_last_recover` timestamp to the
  document
* On a force
  * Throw an error if any containers are running. All containers must be held or complete.
    * Parent job state advancement works by checking whether all N subjobs have a given
      state in their transition history. Resetting the main job to DOWNLOAD_SUBMITTED while
      containers are still running creates a race: containers may finish and fire their
      remaining callbacks before or during the reset, leaving the parent stuck at
      DOWNLOAD_SUBMITTED with no future callbacks arriving to advance it through the
      intermediate states. Requiring all containers to be held or complete first gives a
      stable snapshot with no in-flight callbacks to race against.
    * Regular recovery avoids this race by only changing the job state when there are held
      containers — if there are only running containers it throws an error and leaves the job
      alone. Force recovery cannot do the same because the job is already stuck in RECOVERING
      and must be reset regardless of whether there are held containers.
  * Allow transitioning the job from RECOVERING -> RECOVERING
    * Any other transition should fail, throw an error, and not update the job state
  * Only allow recovery every 10m
    * Assert that `_last_recover` is > 10m in the past
    * Set `_last_recover` to the current time
    * This only allows a force every 10 minutes which should be enough to ensure that recoveries
      don't overlap. Forces should be an extremely rare event so this annoyance seems reasonable
      so that job recovery logic is easier to reason about.
  * The HTC job state should be otherwise ignored prior to attempting to update the
    job state (other than the lack of a cluster ID), since the job MUST be in the recovering
    state for the service instance + coroutine to get a state lock. The enforced 10m wait means we
    assume the prior recovery failed and the job is stuck in RECOVERING. As such, the job
    may have succeeded and there are no held jobs.
  * Once the lock is acquired, run through the standard steps of checking for completed / held
    jobs as per the standard recovery process
    * Note that if the job has succeeded, the code should build the new transition times
      array starting from download submitted and perform the array append / set as described
      previously, but it can be done in one operation vs. one operation per state
    * If it has not succeeded there must be held jobs

## Notes

* If containers uploaded error logs, the current design leaves the logs in place and the `logpath`
  set in the job record. If the container uploads new logs the old logs will be overwritten.
  This seems ok...? Otherwise during the recovery phase we can delete / rename all the logs.
  Wait and see what users prefer
  * Note rename = copy in S3, so this might cause problems with the 10m force hack
  * Currently users don't have access to the container api. May want to consider this, although
    it adds complication to the UX
* Trying to recover a job for a NERSC site should throw an Unimplemented or Unsupported error for
  now
* We check for held containers to restart in HTC rather than Mongo as a container may be held
  and unable to update its state to `error` 

## Appendix 1: Mongo operations

### Atomically append array A to array B and set array A, optionally adding a new entry to array B

Subjobs need the new entry, the main job does not.

```
db.collection.updateMany(
  { someField: someValue },
  [
    {
      $set: {
        arrayB: { $concatArrays: ["$arrayB", "$arrayA", ["newEntry"]] },
        arrayA: ["new", "values", "here"]
      }
    }
  ]
)
```

## Appendix 2: Implementation notes

* Container / subjob state updates need to improved so that failing to set the main job state
  doesn't fail the container. For example, if a job is set to RECOVERING, all the containers but 1
  have passed the error processing submitted or upload submitted states, and the final container
  enters the upload submitted state, that container will fail because `update_container_state` will
  call `update_job_state`, which will fail, and then `handle_exception` which calls `save_error`,
  which will fail. 
  * Instead of spawning a new coroutine only for terminal state updates, spawn it directly after
    updating the container state.
  * This allows containers to continue to run to completion during a RECOVERING event.
  * However, it does mean that if a main job transition fails during that time it will spam the
    logs with an error that's harmless and expected. That's probably better than fetching the job
    state for every update and checking it's not in RECOVERING which should be very rare
    * If this becomes an issue if an update fails pull the job and check if it's in
      recovery perhaps?
 