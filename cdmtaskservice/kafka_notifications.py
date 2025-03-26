"""
Sends job status notifications to Kafka.
"""

# Could make an interface here to allow for additional / swapping notification systems, but YAGNI

from aiokafka import AIOKafkaProducer
import asyncio
import datetime
import json
import re

from cdmtaskservice.arg_checkers import require_string as _require_string, not_falsy as _not_falsy
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
        await kp.start()
        kn._prod = kp
        return kn
        
    def __init__(self, topic: str):
        # TODO ARGCHECKING fail on string length > 249
        match = self._KAFKA_TOPIC_ILLEGAL_CHARS_RE.search(_require_string(topic, "topic"))
        if match:
            raise ValueError(f'Illegal character in Kafka topic {topic}: {match.group()}')
        self._topic = topic
        self._closed = False

    async def update_job_state(self, job_id: str, state: JobState, time: datetime.datetime):
        """
        Update Kafka with the job state change.
        
        job_id - the job's ID.
        state - the new state of the job.
        time - the time at which the state change occurred.
        """
        if self._closed:
            raise ValueError('client is closed')
        future = await self._prod.send(self._topic, json.dumps({
            "job_id": _require_string(job_id, "job_id"),
            "state": _not_falsy(state, "state").value,
            "time": _not_falsy(time, "time").isoformat()
        }).encode("utf-8"))
        # kafka client appears to enter an infinite loop if kafka is down
        # may need to make this configurable, but 10s is a pretty long time to wait for a 
        # tiny message to send. Don't worry about it for now
        # https://github.com/aio-libs/aiokafka/issues/1101
        await asyncio.wait_for(future, 10)  # throw exception, if any. This is a pain to test

    async def close(self):
        """
        Close the notifier.
        """
        await self._prod.stop()
        self._closed = True
