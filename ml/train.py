import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib

df = pd.read_csv("ml/dataset.csv")

X = df[[
    "out_degree",
    "in_degree",
    "total_sent",
    "total_received",
    "velocity"
]].fillna(0)

model = IsolationForest(n_estimators=200, contamination=0.1, random_state=42)
df["anomaly"] = model.fit_predict(X)          # -1 anomaly, 1 normal
df["anomaly_score"] = model.decision_function(X)

df.to_csv("ml/scored_dataset.csv", index=False)
joblib.dump(model, "ml/model.joblib")

print(df.head())