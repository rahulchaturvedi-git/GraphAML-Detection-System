import time
from neo4j import GraphDatabase
from app.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

class GraphWriter:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )

    def write_batch(self, tx_list):
        query = """
        UNWIND $transactions AS tx

        MERGE (a:Account {account_id: tx.source_account})
        MERGE (b:Account {account_id: tx.destination_account})

        MERGE (a)-[t:TRANSFER {transaction_id: tx.transaction_id}]->(b)

        SET t.amount = tx.amount,
            t.timestamp = tx.timestamp,
            t.channel = tx.channel,
            t.country = tx.country
        """

        for attempt in range(MAX_RETRIES):
            try:
                with self.driver.session() as session:
                    session.run(query, transactions=tx_list)
                return True

            except Exception as e:
                print(f"[ERROR] Write failed (attempt {attempt+1}): {e}")
                time.sleep(RETRY_DELAY * (attempt + 1))

        print("[FATAL] Batch failed after retries")
        return False