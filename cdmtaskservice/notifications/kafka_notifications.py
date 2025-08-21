"""
Sends job status notifications to Kafka.
"""

# Could make an interface here to allow for additional / swapping notification systems, but YAGNI

from aiokafka import AIOKafkaProducer
import asyncio
from asyncio.futures import Future
import datetime
import json
import logging
import re
from typing import Coroutine, Any

from cdmtaskservice.arg_checkers import require_string as _require_string, not_falsy as _not_falsy
from cdmtaskservice import logfields
from cdmtaskservice.models import JobState


class KafkaNotifier:
    """
    Sends messages to Kafka when job status changes.
    
    Configured so that all replicates must get writes before the call to Kafka returns.
    """
    
    _KAFKA_TOPIC_ILLEGAL_CHARS_RE = re.compile('[^a-zA-Z0-9-]+')
    
    @classmethod
    async def create(cls, bootstrap_servers: str, topic: str):
        """
        Create the client.
        
        bootstrap_servers - the list of bootstrap servers in the standard kafka format.
        topic - the topic where messages will be sent. The notifier requires the topic
            name to consist of ASCII alphanumeric values and the hyphen to avoid Kafka issues
            around ambiguity between period and underscore values.
        """
        _require_string(bootstrap_servers, "bootstrap_servers")
        kn = KafkaNotifier(topic)
        # TODO KAFKA auth
        # TODO LATER KAFKA support delivery.timeout.ms when the client supports it
        # https://github.com/aio-libs/aiokafka/issues/632
        # https://github.com/dpkp/kafka-python/issues/1723
        kp = AIOKafkaProducer(
            # can't test multiple servers without a massive PITA
            bootstrap_servers=bootstrap_servers.split(','),
            enable_idempotence=True,
            acks='all',
        )
        # this will fail if it can't connect
        try:
            await kp.start()
        except Exception:
            await kp.stop()
            raise
        kn._prod = kp
        return kn
        
    def __init__(self, topic: str):
        # TODO ARGCHECKING fail on string length > 249
        match = self._KAFKA_TOPIC_ILLEGAL_CHARS_RE.search(_require_string(topic, "topic"))
        if match:
            raise ValueError(f'Illegal character in Kafka topic {topic}: {match.group()}')
        self._topic = topic
        self._closed = False
        self._futures = set()
        self._tasks = set()

    async def update_job_state(
        self,
        job_id: str,
        state: JobState,
        time: datetime.datetime,
        trans_id: str,
        callback: Coroutine[None, Any, None] | None = None,
    ):
        """
        Update Kafka with the job state change.
        
        job_id - the job's ID.
        state - the new state of the job.
        time - the time at which the state change occurred.
        trans_id - a unique ID for the state transition. 
        """
        if self._closed:
            raise ValueError('client is closed')
        msg = {
            "job_id": _require_string(job_id, "job_id"),
            "state": _not_falsy(state, "state").value,
            "time": _not_falsy(time, "time").isoformat(),
            "trans_id": _require_string(trans_id, "trans_id")
        }
        future = await self._prod.send(self._topic, json.dumps(msg).encode("utf-8"))
        self._futures.add(future)  # ensure future isn't garbage collected
        # kafka client appears to enter an infinite loop if kafka is down
        # will eventually send the messages if kafka comes back up
        # https://github.com/aio-libs/aiokafka/issues/1101
        def cb(future: Future):
            self._futures.discard(future)
            try:
                future.result()  # trigger exception and don't call callback if one occurs
            except Exception:
                # no idea how to test this, or what would trigger it
                logging.getLogger(__name__).exception(
                    f"Failed to send state update to Kafka",
                    extra={
                        logfields.JOB_ID: job_id,
                        logfields.TRANS_ID: trans_id,
                    }
                )
                return
            if callback:
                task = asyncio.create_task(callback)
                self._tasks.add(task)  # prevent garbage collection
                task.add_done_callback(self._tasks.discard)
        future.add_done_callback(cb)

    async def close(self):
        """
        Close the notifier.
        """
        await self._prod.stop()
        self._closed = True
        self._futures = None  # allow garbage collection
        self._tasks = None  # allow garbage collection
