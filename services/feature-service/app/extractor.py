from app.config import driver


class FeatureExtractor:

    def __init__(self):
        self.driver = driver


    def extract_transactions(self):

        query = """
        MATCH (s:Account)-[:SENT]->(t:Transaction)-[:TO]->(d:Account)

        RETURN
            t.transaction_id AS transaction_id,
            t.amount AS amount,
            t.timestamp AS timestamp,
            t.channel AS channel,
            t.country AS country,
            t.currency AS currency,
            t.label AS label,

            s.account_id AS sender,
            d.account_id AS receiver,

            coalesce(s.out_degree,0) AS sender_out_degree,
            coalesce(s.in_degree,0) AS sender_in_degree,

            coalesce(d.out_degree,0) AS receiver_out_degree,
            coalesce(d.in_degree,0) AS receiver_in_degree,

            coalesce(s.total_sent,0) AS sender_total_sent,
            coalesce(d.total_received,0) AS receiver_total_received,

            coalesce(s.tx_count_60s,0) AS sender_velocity
        """

        with self.driver.session() as session:

            result = session.run(query)

            return [record.data() for record in result]