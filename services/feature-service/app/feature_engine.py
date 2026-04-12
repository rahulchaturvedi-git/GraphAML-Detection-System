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
        MERGE (a:Account {account_id: $source})
        MERGE (b:Account {account_id: $destination})

        // --------------------------
        // OUTGOING FEATURES
        // --------------------------
        SET a.out_degree = coalesce(a.out_degree, 0) + 1,
            a.total_sent = coalesce(a.total_sent, 0) + $amount

        // --------------------------
        // INCOMING FEATURES
        // --------------------------
        SET b.in_degree = coalesce(b.in_degree, 0) + 1,
            b.total_received = coalesce(b.total_received, 0) + $amount

        // --------------------------
        // VELOCITY (sender only)
        // --------------------------
        WITH a, b, $timestamp AS ts

        SET a.tx_times = coalesce(a.tx_times, []) + ts

        WITH a, b, [t IN a.tx_times WHERE t >= ts - 60] AS recent_times

        SET a.tx_times = recent_times,
            a.tx_count_60s = size(recent_times)

        // --------------------------
        // PATTERN FLAGS
        // --------------------------
        SET a.is_high_fanout = CASE WHEN a.out_degree > 5 THEN true ELSE false END,
            b.is_high_fanin = CASE WHEN b.in_degree > 5 THEN true ELSE false END

        // --------------------------
        // SIMPLE CYCLE DETECTION
        // --------------------------
        WITH a, b

        OPTIONAL MATCH (a)-[:TRANSFER]->(x)-[:TRANSFER]->(y)-[:TRANSFER]->(a)

        WITH a, b, COUNT(y) AS cycle_count

        SET a.has_cycle = CASE WHEN cycle_count > 0 THEN true ELSE false END

        // --------------------------
        // RISK SCORING (IMPORTANT FIX)
        // --------------------------
        WITH a, b

        SET a.risk_score =
            (CASE WHEN a.out_degree > 5 THEN 40 ELSE 0 END) +
            (CASE WHEN a.tx_count_60s > 5 THEN 30 ELSE 0 END) +
            (CASE WHEN a.has_cycle = true THEN 50 ELSE 0 END),

            b.risk_score =
            (CASE WHEN b.in_degree > 5 THEN 40 ELSE 0 END)

        // --------------------------
        // ALERT FLAG
        // --------------------------
        SET a.is_suspicious = CASE WHEN a.risk_score >= 50 THEN true ELSE false END,
            b.is_suspicious = CASE WHEN b.risk_score >= 50 THEN true ELSE false END
        """

        with self.driver.session() as session:
            session.run(
                query,
                source=tx["source_account"],
                destination=tx["destination_account"],
                amount=tx["amount"],
                timestamp=tx["timestamp"]
            )