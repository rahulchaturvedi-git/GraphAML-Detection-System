import pandas as pd


df = pd.read_csv(
    "data/processed/full_dataset.csv"
)

df["timestamp"] = pd.to_datetime(
    df["timestamp"]
)

df = df.sort_values(
    "timestamp"
)


train_size = int(len(df) * 0.7)
val_size = int(len(df) * 0.15)


train_df = df[:train_size]

val_df = df[
    train_size:train_size + val_size
]

test_df = df[
    train_size + val_size:
]


# CSV EXPORTS
train_df.to_csv(
    "data/train/train.csv",
    index=False
)

val_df.to_csv(
    "data/validation/validation.csv",
    index=False
)

test_df.to_csv(
    "data/test/test.csv",
    index=False
)


# PARQUET EXPORTS
train_df.to_parquet(
    "data/train/train.parquet"
)

val_df.to_parquet(
    "data/validation/validation.parquet"
)

test_df.to_parquet(
    "data/test/test.parquet"
)


print("Dataset splitting complete")
print(f"Train: {len(train_df)}")
print(f"Validation: {len(val_df)}")
print(f"Test: {len(test_df)}")