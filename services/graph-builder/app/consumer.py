from kafka import KafkaConsumer
import json
import time
from app.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from app.graph_writer import GraphWriter

BATCH_SIZE = 50
FLUSH_INTERVAL = 2

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=False,   # IMPORTANT
    group_id="graph-builder-group"
)

def start():
    writer = GraphWriter()
    buffer = []
    last_flush_time = time.time()

    print("Graph Builder (fault-tolerant mode) running...")

    for msg in consumer:
        tx = msg.value
        buffer.append(tx)

        current_time = time.time()

        should_flush = (
            len(buffer) >= BATCH_SIZE or
            (current_time - last_flush_time >= FLUSH_INTERVAL and buffer)
        )

        if should_flush:
            print(f"Attempting batch write of {len(buffer)}")

            success = writer.write_batch(buffer)

            if success:
                consumer.commit()  # commit ONLY after success
                print("[SUCCESS] Batch committed")
                buffer.clear()
                last_flush_time = current_time
            else:
                print("[RETRY] Keeping buffer for retry")