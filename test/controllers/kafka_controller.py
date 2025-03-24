"""
Q&D kafka controller for use for testing. Starts and stops a kafka Docker container and has
a few helper methods for creating and deleting topics.

Production use is not recommended.
"""

# I'd prefer to not use docker but asking a dev to install java 17 just to run tests for
# a python module is a bit much

# TODO KAFKA will need to handle auth at some point

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable
import subprocess
import uuid
import time

from test_common import config
from utils import find_free_port

class KafkaController:
    """
    The main Kafka controller class.

    Attributes:
    port - the port for the Kafka service.
    """
    
    def __init__(self, docker_image: str):
        self._container_name = f"kafka_controller_test_{uuid.uuid4()}"
        self.port = find_free_port()
        env = self._get_kafka_env()
        command = [
            "docker", "run",
            "--rm",
            "-d",
            "-p", f"{self.port}:{self.port}",
            "--name", self._container_name
        ]
        for k, v in env.items():
            command.extend(["-e", f"{k}={v}"])
        command.append(docker_image)
        
        self._proc = subprocess.Popen(command)

        for count in range(40):
            err = None
            time.sleep(1)  # wait for server to start
            try:
                KafkaProducer(bootstrap_servers=[f"localhost:{self.port}"])
                break
            except NoBrokersAvailable as e:
                err = KafkaStartException('No Kafka brokers available')
                err.__cause__ = e
        if err:
            self._print_kafka_logs()
            raise err
        self.startup_count = count + 1
        self._admin = KafkaAdminClient(bootstrap_servers=f"localhost:{self.port}")


    def _get_kafka_env(self):
        return {
            # default config
            "KAFKA_NODE_ID": "1",
            "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_LISTENERS": f"PLAINTEXT://:{self.port},CONTROLLER://localhost:9093",
            "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://localhost:{self.port}",
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": f"1@localhost:9093",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
            "KAFKA_NUM_PARTITIONS": "3",
          
            # custom config
            "KAFKA_DELETE_TOPIC_ENABLE": "true"
        }

    def delete_topics(self, *topics: str):
        self._admin.delete_topics(topics)

    def delete_all_topics(self):
        topics = self._admin.list_topics()
        topics = [t for t in topics if not t.startswith("_")]
        self.delete_topics(*topics)

    def _print_kafka_logs(self):
        command = ["docker", "logs", self._container_name]
        subprocess.run(command, check=True)

    def destroy(self, print_logs=False):
        if print_logs:
            self._print_kafka_logs()
        command = ["docker", "stop", self._container_name]
        subprocess.run(command, check=True)
        self._proc.wait(timeout=10)


class KafkaStartException(Exception):
    pass


def main():
    kc = KafkaController(config.KAFKA_DOCKER_IMAGE)
    try:
        print(f"kafka port: {kc.port}")
        prod = KafkaProducer(bootstrap_servers=[f'localhost:{kc.port}'])
        prod.send('mytopic', 'some message'.encode('utf-8'))
        time.sleep(1)  # wait for message to be ready
    
        # kc.delete_topics("mytopic")  # comment out to test consumer getting message
        # kc.delete_all_topics()  # comment out to test consumer getting message
    
        cons = KafkaConsumer(
            'mytopic',
            bootstrap_servers=[f'localhost:{kc.port}'],
            auto_offset_reset='earliest',
            group_id='foo'
        )
        print(cons.poll(timeout_ms=1000))
        input('press enter to shut down')
    finally:
        kc.destroy(True)


if __name__ == '__main__':
    main()
