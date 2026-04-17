import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.utils import resample

# Load
df = pd.read_csv("ml/aml_features.csv")
df = df.fillna(0)

# Separate classes
df_majority = df[df.label == 0]
df_minority = df[df.label == 1]

print("Before balancing:")
print(df["label"].value_counts())

# 🔴 UPSAMPLE FRAUD
df_minority_upsampled = resample(
    df_minority,
    replace=True,
    n_samples=len(df_majority),
    random_state=42
)

df_balanced = pd.concat([df_majority, df_minority_upsampled])

print("\nAfter balancing:")
print(df_balanced["label"].value_counts())

# Features
X = df_balanced.drop(columns=["account_id", "label"])
y = df_balanced["label"]

# Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Model
model = RandomForestClassifier(
    n_estimators=200,
    max_depth=10,
    class_weight="balanced"
)

model.fit(X_train, y_train)

# Predict
y_pred = model.predict(X_test)

print("\nClassification Report:")
print(classification_report(y_test, y_pred))