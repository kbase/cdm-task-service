"""
Writes the current contents of the Kafka CDM Task Service topic in their entirety
to stdout.

Assuming a docker compose context, invoke like:

docker compose exec cdm-task-service python test_manual/dump_kafka_topic.py

Note that the docker compose must be altered so that the CTS build section contains:

```
      args:
        UV_DEV_ARGUMENT: "--dev"
```

... to ensure the sync Kafka client is installed.
"""

# Probably some useful options we could add here, like a group ID to only see new messages, etc.


from kafka import KafkaConsumer
import os
from pprint import pprint


def main():
    cons = KafkaConsumer(
        os.environ["KBCTS_KAFKA_TOPIC_JOBS"],
        bootstrap_servers=os.environ["KBCTS_KAFKA_BOOTSTRAP_SERVERS"].split(","),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    pollres = True
    try:
        while pollres:
            pollres = cons.poll(timeout_ms=2000)
            for _, messages in pollres.items():
                for msg in messages:
                    # https://docs.python.org/3/library/collections.html#collections.somenamedtuple._asdict
                    pprint(msg._asdict())
    finally:
        cons.close()


if __name__ == "__main__":
    main()
