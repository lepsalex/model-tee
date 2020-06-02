import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()


class Kafka:
    BOOTSTRAP_SERVERS = os.getenv("KAKFA_BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC = os.getenv("KAFKA_TOPIC", "workflow")

    @classmethod
    def consumeTopicWith(cls, topic, onMessageFunc):
        print("Starting Kafka consumer ...")
        consumer = KafkaConsumer(client_id="model-tee",
                                 group_id="model-tee-workflow-consumer",
                                 bootstrap_servers=cls.BOOTSTRAP_SERVERS,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))

        consumer.subscribe(topic)

        for message in consumer:
            onMessageFunc(message)

        if consumer is not None:
            consumer.close()
