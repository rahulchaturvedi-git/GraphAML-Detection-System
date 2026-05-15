import pandas as pd


print("Loading Elliptic dataset...")


BASE_PATH = "data/raw/elliptic_bitcoin_dataset/"


# LOAD DATA
edges = pd.read_csv(
    BASE_PATH + "elliptic_txs_edgelist.csv"
)

features = pd.read_csv(
    BASE_PATH + "elliptic_txs_features.csv",
    header=None
)

labels = pd.read_csv(
    BASE_PATH + "elliptic_txs_classes.csv"
)


print("Merging datasets...")


# MERGE FEATURES
transactions = edges.merge(
    features,
    left_on="txId1",
    right_on=0
)

# MERGE LABELS
transactions = transactions.merge(
    labels,
    left_on="txId1",
    right_on="txId"
)


# AML SCHEMA
transactions["transaction_id"] = (
    transactions["txId1"].astype(str)
    + "_"
    + transactions["txId2"].astype(str)
)

transactions["from_account"] = (
    transactions["txId1"].astype(str)
)

transactions["to_account"] = (
    transactions["txId2"].astype(str)
)


# APPROX AMOUNT
transactions["amount"] = (
    transactions[2].abs() * 10000
)


# STANDARDIZED TIMESTAMP
base_time = pd.Timestamp("2024-01-01")

transactions["timestamp"] = (
    base_time
    + pd.to_timedelta(transactions[1], unit="h")
)


# BINARY LABEL
transactions["label"] = transactions["class"].map({
    "1": 1,
    "2": 0,
    "unknown": 0
})


# FINAL DATAFRAME
final_df = transactions[
    [
        "transaction_id",
        "from_account",
        "to_account",
        "amount",
        "timestamp",
        "label"
    ]
]


print("\nDataset Preview:")
print(final_df.head())

print("\nLabel Distribution:")
print(final_df["label"].value_counts())


# EXPORT PARQUET
final_df.to_parquet(
    "data/processed/elliptic_transactions.parquet",
    index=False
)

print("\nExport Complete")