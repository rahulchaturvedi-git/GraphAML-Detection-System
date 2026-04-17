from neo4j import GraphDatabase
import pandas as pd

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "test1234"  # update if needed

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

query = """
MATCH (a:Account)
RETURN 
a.account_id AS account_id,
coalesce(a.out_degree, 0) AS out_degree,
coalesce(a.in_degree, 0) AS in_degree,
coalesce(a.total_sent, 0) AS total_sent,
coalesce(a.total_received, 0) AS total_received,
coalesce(a.tx_count_60s, 0) AS tx_count_60s,
coalesce(a.is_laundering, 0) AS label
"""

with driver.session() as session:
    result = session.run(query)
    data = [record.data() for record in result]

df = pd.DataFrame(data)
df.to_csv("ml/aml_features.csv", index=False)

print("Exported:", df.shape)