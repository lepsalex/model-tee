import os
import json
import multiprocessing
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv("KAKFA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "workflow")


class Consumer(multiprocessing.Process):
    def __init__(self, onMessageFunc):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
        self.onMessageFunc = onMessageFunc

    def stop(self):
        self.stop_event.set()

    def run(self):
        print("Starting Kafka consumer ...")
        consumer = KafkaConsumer(client_id="model-tee",
                                 bootstrap_servers=BOOTSTRAP_SERVERS,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))

        consumer.subscribe([TOPIC])

        print(consumer.assignment())

        while not self.stop_event.is_set():
            for message in consumer:
                self.onMessageFunc(message)
                if self.stop_event.is_set():
                    break

        consumer.close()
