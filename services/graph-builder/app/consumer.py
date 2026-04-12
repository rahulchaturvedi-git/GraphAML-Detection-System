from kafka import KafkaConsumer
import json
import time
from app.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from app.graph_writer import GraphWriter

# CONFIG
BATCH_SIZE = 50
FLUSH_INTERVAL = 2  # seconds

MAX_BUFFER_SIZE = 500
RESUME_THRESHOLD = 200


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=False,   # critical
    group_id="graph-builder-group"
)


def start():
    writer = GraphWriter()
    buffer = []
    last_flush_time = time.time()
    paused = False

    print("Graph Builder (backpressure enabled) running...")

    while True:

        # ---------------------------
        # BACKPRESSURE CONTROL
        # ---------------------------
        if len(buffer) >= MAX_BUFFER_SIZE:
            if not paused:
                print("[PAUSE] Buffer full, pausing consumption")
                paused = True

            # stop consuming temporarily
            time.sleep(0.5)

        else:
            if paused and len(buffer) <= RESUME_THRESHOLD:
                print("[RESUME] Buffer drained, resuming consumption")
                paused = False

            # controlled polling
            records = consumer.poll(timeout_ms=100)

            for tp, messages in records.items():
                for msg in messages:
                    buffer.append(msg.value)

        # ---------------------------
        # BATCH FLUSH CONDITIONS
        # ---------------------------
        current_time = time.time()

        should_flush = (
            len(buffer) >= BATCH_SIZE or
            (current_time - last_flush_time >= FLUSH_INTERVAL and buffer)
        )

        if should_flush:
            print(f"Attempting batch write of {len(buffer)}")

            success = writer.write_batch(buffer)

            if success:
                consumer.commit()
                print("[SUCCESS] Batch committed")

                buffer.clear()
                last_flush_time = current_time

            else:
                print("[RETRY] Keeping buffer for retry")