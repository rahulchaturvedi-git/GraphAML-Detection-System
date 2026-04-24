import os
import joblib
import pandas as pd
from fastapi import FastAPI

# 🔴 CREATE APP FIRST
app = FastAPI()

# 🔴 LOAD MODEL
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
MODEL_PATH = os.path.join(BASE_DIR, "ml", "aml_model.pkl")

model = joblib.load(MODEL_PATH)

FEATURES = [
    "out_degree", "in_degree", "total_sent", "total_received",
    "tx_count_60s", "unique_receivers", "unique_senders",
    "neighbor_risk_ratio", "two_hop_risk",
    "flow_ratio", "burst_ratio", "degree_ratio"
]

# 🔴 ROUTE
@app.post("/score")
def score(tx: dict):
    df = pd.DataFrame([tx])
    df = df[FEATURES]

    prob = model.predict_proba(df)[0][1]

    importances = model.feature_importances_
    contributions = {}

    for i, f in enumerate(FEATURES):
        contributions[f] = float(importances[i] * df.iloc[0][f])

    top_reasons = sorted(contributions.items(), key=lambda x: x[1], reverse=True)[:3]

    return {
        "fraud_probability": float(prob),
        "is_fraud": int(prob > 0.2),
        "top_reasons": top_reasons
    }