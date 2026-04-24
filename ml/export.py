from neo4j import GraphDatabase
import pandas as pd

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test1234"))

query = """
MATCH (a:Account)

// receivers
OPTIONAL MATCH (a)-[:SENT]->(b)
WITH a, count(DISTINCT b) AS unique_receivers

// senders
OPTIONAL MATCH (c)-[:SENT]->(a)
WITH a, unique_receivers, count(DISTINCT c) AS unique_senders

// neighbor risk (keep vars through WITH)
OPTIONAL MATCH (a)--(n)
WITH a, unique_receivers, unique_senders,
     count(n) AS total_neighbors,
     sum(coalesce(n.is_laundering,0)) AS risky_neighbors

WITH a, unique_receivers, unique_senders,
     CASE WHEN total_neighbors = 0 THEN 0
     ELSE (risky_neighbors * 1.0 / total_neighbors) * 0.5 END AS neighbor_risk_ratio

// two-hop risk (normalized)
OPTIONAL MATCH (a)-[:SENT]->(b)-[:SENT]->(x)
WITH a, unique_receivers, unique_senders, neighbor_risk_ratio,
     count(x) AS two_hop_total,
     sum(coalesce(x.is_laundering,0)) AS two_hop_fraud

WITH a, unique_receivers, unique_senders, neighbor_risk_ratio,
     CASE WHEN two_hop_total = 0 THEN 0
     ELSE two_hop_fraud * 1.0 / two_hop_total END AS two_hop_risk

RETURN
a.account_id AS account_id,

coalesce(a.out_degree, 0) AS out_degree,
coalesce(a.in_degree, 0) AS in_degree,
coalesce(a.total_sent, 0) AS total_sent,
coalesce(a.total_received, 0) AS total_received,
coalesce(a.tx_count_60s, 0) AS tx_count_60s,

unique_receivers,
unique_senders,
neighbor_risk_ratio,
two_hop_risk,

// flow imbalance
coalesce(a.total_sent,0) / (coalesce(a.total_received,1)) AS flow_ratio,

// burst
coalesce(a.tx_count_60s,0) * 1.0 / (coalesce(a.out_degree,1)) AS burst_ratio,

// degree ratio
(coalesce(a.out_degree,0)+1) / (coalesce(a.in_degree,1)) AS degree_ratio,

coalesce(a.is_laundering, 0) AS label
"""

with driver.session() as session:
    result = session.run(query)
    df = pd.DataFrame([r.data() for r in result])

df.to_csv("ml/aml_features.csv", index=False)

print("Exported:", df.shape)