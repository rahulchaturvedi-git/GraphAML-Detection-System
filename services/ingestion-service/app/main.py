from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer
import json

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

KAFKA_TOPIC = "transactions"


class Transaction(BaseModel):
    transaction_id: str
    source_account: str
    destination_account: str
    amount: float
    timestamp: int
    currency: str
    channel: str
    country: str
    label: int   # 🔴 REQUIRED


@app.post("/ingest")
def ingest(data: Transaction):

    tx = {
        "transaction_id": data.transaction_id,
        "source_account": data.source_account,
        "destination_account": data.destination_account,
        "amount": data.amount,
        "timestamp": data.timestamp,
        "currency": data.currency,
        "channel": data.channel,
        "country": data.country,
        "label": data.label   # 🔴 CRITICAL
    }

    print("INGEST → Kafka:", tx)

    producer.send(KAFKA_TOPIC, value=tx)

    return {"status": "sent"}