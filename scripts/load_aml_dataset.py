import pandas as pd
import requests
import time
import random

FILE_PATH = "data/aml/SAML-D.csv"
API_URL = "http://127.0.0.1:8000/ingest"

CHUNK_SIZE = 500
MAX_ROWS = 3000

count = 0

print("Starting ingestion...")

accounts = set()

# -----------------------------
# SEND FUNCTION
# -----------------------------
def send(tx):
    try:
        res = requests.post(API_URL, json=tx)
        if res.status_code != 200:
            print("Error:", res.text)
    except Exception as e:
        print("Request failed:", e)

# -----------------------------
# 1. NORMAL DATA INGESTION
# -----------------------------
for chunk in pd.read_csv(FILE_PATH, chunksize=CHUNK_SIZE):
    for _, row in chunk.iterrows():

        src = str(row["Sender_account"])
        dst = str(row["Receiver_account"])

        accounts.add(src)
        accounts.add(dst)

        tx = {
            "transaction_id": f"tx_{count}",
            "source_account": src,
            "destination_account": dst,
            "amount": float(row["Amount"]) + random.uniform(-5, 5),
            "timestamp": int(time.time()),
            "currency": str(row["Payment_currency"]),
            "channel": "normal",
            "country": str(row["Sender_bank_location"]),
            "label": 1 if int(row["Is_laundering"]) > 0 else 0
        }

        send(tx)
        count += 1

        time.sleep(0.005)

        if count % 100 == 0:
            print(f"{count} normal transactions sent")

        if count >= MAX_ROWS:
            print("Reached normal data limit")
            break
    if count >= MAX_ROWS:
        break

accounts = list(accounts)

print("Normal data loaded. Injecting fraud patterns...")

# -----------------------------
# 2. CHAIN FRAUD
# A → B → C → D
# -----------------------------
for i in range(50):
    a, b, c, d = random.sample(accounts, 4)

    send({
        "transaction_id": f"chain_{i}_1",
        "source_account": a,
        "destination_account": b,
        "amount": 1000,
        "timestamp": int(time.time()),
        "currency": "USD",
        "channel": "chain",
        "country": "US",
        "label": 1
    })

    send({
        "transaction_id": f"chain_{i}_2",
        "source_account": b,
        "destination_account": c,
        "amount": 1000,
        "timestamp": int(time.time()),
        "currency": "USD",
        "channel": "chain",
        "country": "US",
        "label": 1
    })

    send({
        "transaction_id": f"chain_{i}_3",
        "source_account": c,
        "destination_account": d,
        "amount": 1000,
        "timestamp": int(time.time()),
        "currency": "USD",
        "channel": "chain",
        "country": "US",
        "label": 1
    })

# -----------------------------
# 3. FAN-OUT FRAUD
# A → many accounts
# -----------------------------
for i in range(50):
    a = random.choice(accounts)
    receivers = random.sample(accounts, 5)

    for j, r in enumerate(receivers):
        send({
            "transaction_id": f"fanout_{i}_{j}",
            "source_account": a,
            "destination_account": r,
            "amount": 500,
            "timestamp": int(time.time()),
            "currency": "USD",
            "channel": "fanout",
            "country": "US",
            "label": 1
        })

# -----------------------------
# 4. CIRCULAR FRAUD
# A → B → C → A
# -----------------------------
for i in range(50):
    a, b, c = random.sample(accounts, 3)

    send({
        "transaction_id": f"loop_{i}_1",
        "source_account": a,
        "destination_account": b,
        "amount": 700,
        "timestamp": int(time.time()),
        "currency": "USD",
        "channel": "loop",
        "country": "US",
        "label": 1
    })

    send({
        "transaction_id": f"loop_{i}_2",
        "source_account": b,
        "destination_account": c,
        "amount": 700,
        "timestamp": int(time.time()),
        "currency": "USD",
        "channel": "loop",
        "country": "US",
        "label": 1
    })

    send({
        "transaction_id": f"loop_{i}_3",
        "source_account": c,
        "destination_account": a,
        "amount": 700,
        "timestamp": int(time.time()),
        "currency": "USD",
        "channel": "loop",
        "country": "US",
        "label": 1
    })

print("Fraud patterns injected successfully.")