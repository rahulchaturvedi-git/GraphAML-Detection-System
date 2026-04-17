from kafka import KafkaConsumer
import json
from app.feature_engine import FeatureEngine
from app.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

feature_engine = FeatureEngine()

print("Graph Builder running...")

for message in consumer:
    tx = message.value

    print("KAFKA → CONSUMER:", tx)

    # 🔴 FULL OBJECT PASS
    feature_engine.update_features(tx)

    print("CONSUMER RECEIVED:", tx)