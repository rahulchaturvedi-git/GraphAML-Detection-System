import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.utils import resample

# Load
df = pd.read_csv("ml/aml_features.csv")
df = df.fillna(0)

# Features
X = df.drop(columns=["account_id", "label"])
y = df["label"]

# 🔴 SPLIT FIRST
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Combine training
train_df = X_train.copy()
train_df["label"] = y_train

# Separate classes
df_majority = train_df[train_df.label == 0]
df_minority = train_df[train_df.label == 1]

print("Train BEFORE balancing:")
print(train_df["label"].value_counts())

# 🔴 Upsample ONLY train
df_minority_upsampled = resample(
    df_minority,
    replace=True,
    n_samples=len(df_majority),
    random_state=42
)

train_balanced = pd.concat([df_majority, df_minority_upsampled])

print("\nTrain AFTER balancing:")
print(train_balanced["label"].value_counts())

# Final train
X_train = train_balanced.drop(columns=["label"])
y_train = train_balanced["label"]

# Model
model = RandomForestClassifier(
    n_estimators=200,
    max_depth=10,
    class_weight="balanced",
    random_state=42
)

model.fit(X_train, y_train)

# 🔴 PROBABILITY-BASED PREDICTION (IMPORTANT)
y_prob = model.predict_proba(X_test)[:, 1]

# 🔴 LOWER THRESHOLD FOR BETTER RECALL
threshold = 0.2
y_pred = (y_prob > threshold).astype(int)

print("\nTEST RESULTS (threshold=0.2):")
print(classification_report(y_test, y_pred))

print(y_prob[:20])

print("\nFeature importance:")
for name, val in zip(X.columns, model.feature_importances_):
    print(name, round(val, 3))


import joblib

# Save model
joblib.dump(model, "ml/aml_model.pkl")
print("Model saved at ml/aml_model.pkl")