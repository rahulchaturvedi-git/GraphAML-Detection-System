import requests
import time
import random

from synthetic.fraud_patterns import (
    generate_structuring_pattern,
    generate_layering_pattern,
    generate_smurfing_pattern,
    generate_circular_laundering,
    generate_fan_out_pattern,
    generate_fan_in_pattern
)


URL = "http://127.0.0.1:8000/ingest"


def generate_normal_tx(i):

    return {
        "transaction_id": f"tx{i}",
        "timestamp": 1712839200 + i,
        "source_account": f"A{random.randint(1,50)}",
        "destination_account": f"B{random.randint(1,50)}",
        "amount": random.randint(100, 10000),
        "currency": "INR",
        "channel": random.choice(
            ["upi", "wire", "card"]
        ),
        "country": "IND",
        "label": 0
    }


fraud_batches = [
    generate_structuring_pattern(),
    generate_layering_pattern(),
    generate_smurfing_pattern(),
    generate_circular_laundering(),
    generate_fan_out_pattern(),
    generate_fan_in_pattern()
]


print("\nStarting AML Load Simulation...\n")


for i in range(1, 2001):

    # NORMAL TRAFFIC
    tx = generate_normal_tx(i)

    try:

        res = requests.post(
            URL,
            json=tx
        )

        print(
            f"NORMAL {i} -> {res.status_code}"
        )

    except Exception as e:

        print("Error:", e)

    # FRAUD BURST INJECTION
    if random.random() < 0.05:

        print("\nInjecting Fraud Batch...\n")

        fraud_batch = random.choice(
            fraud_batches
        )

        for fraud_tx in fraud_batch:

            try:

                res = requests.post(
                    URL,
                    json=fraud_tx
                )

                print(
                    f"FRAUD -> {res.status_code}"
                )

            except Exception as e:

                print("Fraud Error:", e)

            time.sleep(0.02)

    # RANDOM TRAFFIC SPIKES
    if random.random() < 0.02:

        print("\nTraffic Spike...\n")

        for spike in range(50):

            spike_tx = generate_normal_tx(
                i * 1000 + spike
            )

            try:

                requests.post(
                    URL,
                    json=spike_tx
                )

            except:
                pass

    time.sleep(0.01)


print("\nLoad Test Complete")