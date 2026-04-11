import requests
import time
import random

URL = "http://127.0.0.1:8000/ingest"

def generate_tx(i):
    return {
        "transaction_id": f"tx{i}",
        "timestamp": 1712839200 + i,
        "source_account": f"A{random.randint(1,10)}",
        "destination_account": f"B{random.randint(1,10)}",
        "amount": random.randint(100, 10000),
        "currency": "INR",
        "channel": random.choice(["upi", "wire", "card"]),
        "country": "IND"
    }

for i in range(1, 201):  # send 200 transactions
    tx = generate_tx(i)

    try:
        res = requests.post(URL, json=tx)
        print(i, res.status_code)
    except Exception as e:
        print("Error:", e)

    time.sleep(0.01)  # small delay