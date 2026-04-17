from kafka import KafkaConsumer
import json
from app.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from app.feature_engine import FeatureEngine

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="feature-group"
)

def start():
    engine = FeatureEngine()

    print("Feature Service running...")

    for msg in consumer:
        tx = msg.value

        print(f"[FEATURE] Processing tx: {tx['transaction_id']} | account: {tx['source_account']}")

        print("CONSUMER RECEIVED:", tx)

        engine.update_features(tx)