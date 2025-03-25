"""
Test what happens when kafka is down and the notifier sends a message.
"""

# Not sure if I want to or if it's worth it to integrate this into the std tests

# TODO KAFKA aiokafka seems to have a bug where if kafka is down it gets stuck forever
#            https://github.com/aio-libs/aiokafka/issues/1101
#            If a fix comes out or the behavior is explained update

import asyncio
from aiokafka import AIOKafkaProducer
from datetime import datetime, timezone
import sys
import time

from cdmtaskservice.kafka_notifications import KafkaNotifier
from cdmtaskservice.models import JobState
from controllers.kafka_controller import KafkaController
from test_common import config
import traceback


async def main():
    kc = KafkaController(config.KAFKA_DOCKER_IMAGE)
    kn = await KafkaNotifier.create(f"localhost:{kc.port}", "sometopic")
    print("sending 1st message")
    await kn.update_job_state(
        "jobbyjob", JobState.CREATED, datetime(2025, 1, 1, 1, 1, 1, tzinfo=timezone.utc)
    )
    print("done")
    start = time.time()
    kc.destroy(False)
    try:
        print("sending 2nd message")
        await kn.update_job_state(
            "jobbyjob", JobState.CREATED, datetime(2025, 1, 1, 1, 1, 1, tzinfo=timezone.utc)
        )
        print("done")
        # spams the logs with error messages and blocks w/o a timout on the future in the client
    except TimeoutError as e:
        traceback.print_exception(e)
    print(start, time.time())
    await kn.close()  # this currently hangs forever


async def main2():
    # start kafka
    kc = KafkaController(config.KAFKA_DOCKER_IMAGE)
    kp = AIOKafkaProducer(
        # can't test multiple servers without a massive PITA
        bootstrap_servers=f"localhost:{kc.port}",
        enable_idempotence=True,
        acks='all',
        request_timeout_ms=8000,
        retry_backoff_ms=2000
    )
    await kp.start()
    print("sending 1st message")
    future = await kp.send("mytopic", b"foo1")
    print("done")
    await future
    # stop kafka
    kc.destroy(False)
    print("sending 2nd message")
    future = await kp.send("mytopic", b"foo2")
    print("done")
    try:
        await asyncio.wait_for(future, 10)
    except TimeoutError as e:
        traceback.print_exception(e)
    print("stopping client")
    await kp.stop()  # this currently hangs forever
    print('done"')


if __name__ == "__main__":
    if len(sys.argv) > 1:
        asyncio.run(main2())
    else:
        asyncio.run(main())
