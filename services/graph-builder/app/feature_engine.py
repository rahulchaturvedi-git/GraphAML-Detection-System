from neo4j import GraphDatabase
from app.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD


class FeatureEngine:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )

    def update_features(self, tx):
        query = """
        MERGE (s:Account {account_id: $source})
        MERGE (d:Account {account_id: $destination})

        SET s.out_degree = coalesce(s.out_degree, 0) + 1,
            s.total_sent = coalesce(s.total_sent, 0) + $amount

        SET d.in_degree = coalesce(d.in_degree, 0) + 1,
            d.total_received = coalesce(d.total_received, 0) + $amount

        // 🔴 FIX: label on BOTH nodes
        SET s.is_laundering = coalesce(s.is_laundering, 0) + $label,
            d.is_laundering = coalesce(d.is_laundering, 0) + $label

        WITH s, $timestamp AS ts
        SET s.tx_times = coalesce(s.tx_times, []) + ts

        WITH s, [t IN s.tx_times WHERE t >= ts - 60] AS recent_times
        SET s.tx_times = recent_times,
            s.tx_count_60s = size(recent_times)
        """

        with self.driver.session() as session:
            session.run(
                query,
                source=tx["source_account"],
                destination=tx["destination_account"],
                amount=tx["amount"],
                timestamp=tx["timestamp"],
                label=int(tx.get("label", 0))   # 🔴 CRITICAL
            )