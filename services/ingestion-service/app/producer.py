from kafka import KafkaProducer
import json
from app.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3
)

def send_transaction(txn: dict):
    key = txn["source_account"].encode()

    producer.send(
        topic=KAFKA_TOPIC,
        key=key,
        value=txn
    )