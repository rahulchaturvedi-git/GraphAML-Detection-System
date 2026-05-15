import pandas as pd

from synthetic.fraud_patterns import (
    generate_structuring_pattern,
    generate_layering_pattern,
    generate_smurfing_pattern,
    generate_circular_laundering,
    generate_fan_out_pattern,
    generate_fan_in_pattern
)

from synthetic.normal_transactions import (
    generate_normal_transactions
)


all_transactions = []


# NORMAL TRANSACTIONS
all_transactions.extend(
    generate_normal_transactions(5000)
)


# FRAUD TRANSACTIONS
all_transactions.extend(
    generate_structuring_pattern()
)

all_transactions.extend(
    generate_layering_pattern()
)

all_transactions.extend(
    generate_smurfing_pattern()
)

all_transactions.extend(
    generate_circular_laundering()
)

all_transactions.extend(
    generate_fan_out_pattern()
)

all_transactions.extend(
    generate_fan_in_pattern()
)

# CREATE DATAFRAME
df = pd.DataFrame(all_transactions)


# SHUFFLE
df = df.sample(frac=1).reset_index(drop=True)


# EXPORT CSV
df.to_csv(
    "data/processed/full_dataset.csv",
    index=False
)


print(df.head())

print("\nLabel Distribution:")
print(df["label"].value_counts())

print(f"\nGenerated {len(df)} transactions")