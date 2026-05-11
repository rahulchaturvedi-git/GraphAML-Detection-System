import pandas as pd

from fraud_patterns import (
    generate_structuring_pattern,
    generate_layering_pattern,
    generate_smurfing_pattern
)

all_transactions = []

all_transactions.extend(generate_structuring_pattern())
all_transactions.extend(generate_layering_pattern())
all_transactions.extend(generate_smurfing_pattern())

df = pd.DataFrame(all_transactions)

df.to_csv(
    "data/processed/synthetic_fraud.csv",
    index=False
)

print(df.head())
print(f"\nGenerated {len(df)} fraud transactions")