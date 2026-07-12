from pathlib import Path
import joblib
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_auc_score
)

from xgboost import XGBClassifier


# -------------------------
# Load Dataset
# -------------------------

df = pd.read_parquet(
    Path("../data/features/graph_features_latest.parquet")
)

df.fillna(0, inplace=True)

# -------------------------
# Encode categoricals
# -------------------------

for col in ["channel", "country", "currency"]:

    encoder = LabelEncoder()

    df[col] = encoder.fit_transform(df[col])

# -------------------------
# Features
# -------------------------

X = df.drop(
    columns=[
        "transaction_id",
        "sender",
        "receiver",
        "label"
    ]
)

y = df["label"]

# -------------------------
# Train/Test Split
# -------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y
)

# -------------------------
# XGBoost
# -------------------------

model = XGBClassifier(

    n_estimators=300,

    max_depth=6,

    learning_rate=0.05,

    subsample=0.8,

    colsample_bytree=0.8,

    scale_pos_weight=40,

    random_state=42,

    eval_metric="logloss"
)

model.fit(X_train, y_train)

# -------------------------
# Prediction
# -------------------------

y_prob = model.predict_proba(X_test)[:, 1]

threshold = 0.30

y_pred = (y_prob >= threshold).astype(int)

print()

print(classification_report(y_test, y_pred))

print()

print("ROC-AUC:", roc_auc_score(y_test, y_prob))

print()

print(confusion_matrix(y_test, y_pred))

# -------------------------
# Save Model
# -------------------------

Path("models").mkdir(exist_ok=True)

joblib.dump(
    model,
    "models/aml_xgboost.pkl"
)

print()

print("Model Saved")