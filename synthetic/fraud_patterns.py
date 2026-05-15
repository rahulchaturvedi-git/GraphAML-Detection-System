import random
from datetime import datetime, timedelta


def generate_structuring_pattern(num_tx=20):

    transactions = []

    mule_account = f"MULE_{random.randint(1000, 9999)}"

    for i in range(num_tx):

        transactions.append({
            "transaction_id": f"STR_{i}",
            "from_account": f"ACC_{random.randint(10000,99999)}",
            "to_account": mule_account,
            "amount": round(random.uniform(9800, 9950), 2),
            "timestamp": (
                datetime.now() + timedelta(minutes=i)
            ).isoformat(),
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
            "from_account": accounts[i],
            "to_account": accounts[i + 1],
            "amount": 50000,
            "timestamp": (
                datetime.now() + timedelta(seconds=i * 30)
            ).isoformat(),
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
            "from_account": f"SMURF_{i}",
            "to_account": central,
            "amount": round(random.uniform(500, 2000), 2),
            "timestamp": (
                datetime.now() + timedelta(seconds=i * 10)
            ).isoformat(),
            "label": 1,
            "pattern": "smurfing"
        })

    return transactions