import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

class Kafka:
    BOOTSTRAP_SERVERS = os.getenv("KAKFA_BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC = os.getenv("KAFKA_TOPIC", "workflow")
    CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "model-tee")
    GROUP_ID = os.getenv("KAFKA_GROUP_ID", "model-tee-workflow-consumer")

    @classmethod
    def consumeTopicWith(cls, topic, onMessageFunc):
        print("Starting Kafka consumer ...")
        consumer = KafkaConsumer(client_id=cls.CLIENT_ID,
                                 group_id=cls.GROUP_ID,
                                 bootstrap_servers=cls.BOOTSTRAP_SERVERS,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))

        consumer.subscribe(topic)

        for message in consumer:
            onMessageFunc(message)

        if consumer is not None:
            consumer.close()
