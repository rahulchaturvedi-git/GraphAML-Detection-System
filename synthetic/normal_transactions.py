import random
from datetime import datetime, timedelta


def generate_normal_transactions(num_tx=5000):

    transactions = []

    for i in range(num_tx):

        transactions.append({
            "transaction_id": f"NORM_{i}",
            "from_account": f"USER_{random.randint(1000,9999)}",
            "to_account": f"USER_{random.randint(1000,9999)}",
            "amount": round(random.uniform(10, 5000), 2),
            "timestamp": (
                datetime.now() + timedelta(seconds=i)
            ).isoformat(),
            "label": 0,
            "pattern": "normal"
        })

    return transactions