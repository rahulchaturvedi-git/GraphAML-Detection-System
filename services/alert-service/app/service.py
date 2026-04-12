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
      WHERE a.is_suspicious = true

      RETURN 
          a.account_id AS account,
          a.risk_score AS risk,
          a.out_degree AS out_degree,
          a.in_degree AS in_degree,
          a.tx_count_60s AS velocity,
          a.has_cycle AS has_cycle
      ORDER BY risk DESC
      """

      with self.driver.session() as session:
          result = session.run(query)

          alerts = []
          for r in result:
              reasons = []

              if r["out_degree"] and r["out_degree"] > 5:
                  reasons.append("High fan-out")

              if r["in_degree"] and r["in_degree"] > 5:
                  reasons.append("High fan-in")

              if r["velocity"] and r["velocity"] > 5:
                  reasons.append("High transaction velocity")

              if r["has_cycle"]:
                  reasons.append("Cycle detected (layering)")

              alerts.append({
                  "account": r["account"],
                  "risk": r["risk"],
                  "reasons": reasons
              })

          return alerts