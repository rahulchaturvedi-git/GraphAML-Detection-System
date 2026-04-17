import pandas as pd
import requests
import time

FILE_PATH = "data/aml/SAML-D.csv"
API_URL = "http://127.0.0.1:8000/ingest"

CHUNK_SIZE = 500
MAX_ROWS = 3000

count = 0

print("Starting ingestion...")

for chunk in pd.read_csv(FILE_PATH, chunksize=CHUNK_SIZE):
    for _, row in chunk.iterrows():

        tx = {
            "transaction_id": f"tx_{count}",
            "source_account": str(row["Sender_account"]),
            "destination_account": str(row["Receiver_account"]),
            "amount": float(row["Amount"]),
            "timestamp": int(time.time()),
            "currency": str(row["Payment_currency"]),
            "channel": str(row["Payment_type"]),
            "country": str(row["Sender_bank_location"]),
            "label": int(row["Is_laundering"])   # 🔴 REAL LABEL
        }

        try:
            res = requests.post(API_URL, json=tx)

            if res.status_code != 200:
                print("Error:", res.text)
            else:
                count += 1

        except Exception as e:
            print("Request failed:", e)

        time.sleep(0.01)

        if count % 100 == 0:
            print(f"{count} sent")

        if count >= MAX_ROWS:
            print("Stopped at limit")
            exit()