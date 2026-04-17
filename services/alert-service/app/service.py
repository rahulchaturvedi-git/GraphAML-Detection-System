from neo4j import GraphDatabase
from app.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD


class AlertService:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )

    def get_alerts(self):
        query = """
        MATCH (a:Account)
        WHERE a.is_suspicious = true OR a.ml_anomaly = -1

        RETURN 
            a.account_id AS account,
            a.risk_score AS risk,
            a.out_degree AS out_degree,
            a.in_degree AS in_degree,
            a.tx_count_60s AS velocity,
            a.has_cycle AS has_cycle,
            a.ml_anomaly AS ml_flag,
            a.ml_score AS ml_score
        ORDER BY risk DESC
        """

        with self.driver.session() as session:
            result = session.run(query)

            alerts = []
            for r in result:
                reasons = []

                # Rule-based reasons
                if r["out_degree"] and r["out_degree"] > 5:
                    reasons.append("High fan-out")

                if r["in_degree"] and r["in_degree"] > 5:
                    reasons.append("High fan-in")

                if r["velocity"] and r["velocity"] > 5:
                    reasons.append("High transaction velocity")

                if r["has_cycle"]:
                    reasons.append("Cycle detected (layering)")

                # ML-based reason
                if r["ml_flag"] == -1:
                    reasons.append("ML anomaly detected")

                alerts.append({
                    "account": r["account"],
                    "risk": r["risk"],
                    "ml_score": r["ml_score"],
                    "ml_flag": r["ml_flag"],
                    "reasons": reasons
                })

            return alerts