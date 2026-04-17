import pandas as pd
from neo4j import GraphDatabase

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", "test1234")
)

df = pd.read_csv("ml/scored_dataset.csv")

query = """
MATCH (a:Account {account_id: $account})
SET a.ml_anomaly = $anomaly,
    a.ml_score = $score
"""

with driver.session() as session:
    for _, r in df.iterrows():
        session.run(
            query,
            account=r["account"],
            anomaly=int(r["anomaly"]),
            score=float(r["anomaly_score"])
        )

print("Wrote ML scores to Neo4j")