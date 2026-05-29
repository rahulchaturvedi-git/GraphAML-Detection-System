import random
import time


def generate_structuring_pattern(num_tx=20):

    transactions = []

    mule_account = f"MULE_{random.randint(1000, 9999)}"

    for i in range(num_tx):

        transactions.append({
            "transaction_id": f"STR_{i}",
            "source_account": f"ACC_{random.randint(10000,99999)}",
            "destination_account": mule_account,
            "amount": round(random.uniform(9800, 9950), 2),
            "timestamp": int(time.time()) + i,
            "currency": "INR",
            "channel": "wire",
            "country": "IND",
            "label": 1,
            "pattern": "structuring"
        })

    return transactions


def generate_layering_pattern():

    accounts = [f"LAYER_{i}" for i in range(5)]

    transactions = []

    for i in range(4):

        transactions.append({
            "transaction_id": f"LAY_{i}",
            "source_account": accounts[i],
            "destination_account": accounts[i + 1],
            "amount": 50000,
            "timestamp": int(time.time()) + i,
            "currency": "INR",
            "channel": "wire",
            "country": "IND",
            "label": 1,
            "pattern": "layering"
        })

    return transactions


def generate_smurfing_pattern():

    transactions = []

    central = "CENTRAL_ACC"

    for i in range(50):

        transactions.append({
            "transaction_id": f"SMU_{i}",
            "source_account": f"SMURF_{i}",
            "destination_account": central,
            "amount": round(random.uniform(500, 2000), 2),
            "timestamp": int(time.time()) + i,
            "currency": "INR",
            "channel": "upi",
            "country": "IND",
            "label": 1,
            "pattern": "smurfing"
        })

    return transactions


def generate_circular_laundering():

    accounts = [
        "CIRC_A",
        "CIRC_B",
        "CIRC_C"
    ]

    transactions = []

    for i in range(3):

        transactions.append({
            "transaction_id": f"CIRC_{i}",
            "source_account": accounts[i],
            "destination_account": accounts[(i + 1) % 3],
            "amount": 25000,
            "timestamp": int(time.time()) + i,
            "currency": "INR",
            "channel": "wire",
            "country": "IND",
            "label": 1,
            "pattern": "circular"
        })

    return transactions


def generate_fan_out_pattern():

    transactions = []

    source = "MASTER_FANOUT"

    for i in range(20):

        transactions.append({
            "transaction_id": f"FANOUT_{i}",
            "source_account": source,
            "destination_account": f"TARGET_{i}",
            "amount": random.randint(1000, 10000),
            "timestamp": int(time.time()) + i,
            "currency": "INR",
            "channel": "wire",
            "country": "IND",
            "label": 1,
            "pattern": "fan_out"
        })

    return transactions


def generate_fan_in_pattern():

    transactions = []

    target = "MASTER_FANIN"

    for i in range(20):

        transactions.append({
            "transaction_id": f"FANIN_{i}",
            "source_account": f"SRC_{i}",
            "destination_account": target,
            "amount": random.randint(1000, 10000),
            "timestamp": int(time.time()) + i,
            "currency": "INR",
            "channel": "wire",
            "country": "IND",
            "label": 1,
            "pattern": "fan_in"
        })

    return transactions
