from app.config import driver


class GraphAnalytics:

    def run_query(self, query, **params):
        with driver.session() as session:
            result = session.run(query, **params)
            return [record.data() for record in result]

    # -------------------------------------------------
    # TOP SENDERS
    # -------------------------------------------------

    def top_senders(self):

        query = """
        MATCH (a:Account)-[:SENT]->(t:Transaction)

        RETURN
            a.account_id AS account,
            count(t) AS transaction_count,
            sum(t.amount) AS total_sent

        ORDER BY total_sent DESC
        LIMIT 20
        """

        return self.run_query(query)

    # -------------------------------------------------
    # MOST ACTIVE ACCOUNTS
    # -------------------------------------------------

    def most_active(self):

        query = """
        MATCH (a:Account)

        OPTIONAL MATCH (a)-[:SENT]->(t:Transaction)
        WITH a, count(t) AS sent

        OPTIONAL MATCH (:Transaction)-[:TO]->(a)
        WITH a, sent, count(*) AS received

        RETURN
            a.account_id AS account,
            sent,
            received,
            sent + received AS activity

        ORDER BY activity DESC
        LIMIT 20
        """

        return self.run_query(query)

    # -------------------------------------------------
    # HIGH RISK ACCOUNTS
    # -------------------------------------------------

    def high_risk_accounts(self):

        query = """
        MATCH (a:Account)-[:SENT]->(t:Transaction)

        WHERE t.label = 1

        RETURN
            a.account_id AS account,
            count(t) AS fraud_transactions,
            sum(t.amount) AS fraud_amount

        ORDER BY fraud_transactions DESC
        """

        return self.run_query(query)

    # -------------------------------------------------
    # LARGEST TRANSACTIONS
    # -------------------------------------------------

    def largest_transactions(self):

        query = """
        MATCH (s:Account)-[:SENT]->(t:Transaction)-[:TO]->(d:Account)

        RETURN
            t.transaction_id AS transaction,
            s.account_id AS sender,
            d.account_id AS receiver,
            t.amount AS amount,
            t.timestamp AS timestamp,
            t.channel AS channel,
            t.label AS label

        ORDER BY amount DESC
        LIMIT 20
        """

        return self.run_query(query)

    # -------------------------------------------------
    # FAN OUT
    # -------------------------------------------------

    def fan_out_accounts(self, min_receivers=10):

        query = """
        MATCH (a:Account)-[:SENT]->(:Transaction)-[:TO]->(b:Account)

        WITH a, count(DISTINCT b) AS receivers

        WHERE receivers >= $min_receivers

        RETURN
            a.account_id AS account,
            receivers

        ORDER BY receivers DESC
        """

        return self.run_query(
            query,
            min_receivers=min_receivers
        )

    # -------------------------------------------------
    # FAN IN
    # -------------------------------------------------

    def fan_in_accounts(self, min_senders=10):

        query = """
        MATCH (a:Account)-[:SENT]->(:Transaction)-[:TO]->(b:Account)

        WITH b, count(DISTINCT a) AS senders

        WHERE senders >= $min_senders

        RETURN
            b.account_id AS account,
            senders

        ORDER BY senders DESC
        """

        return self.run_query(
            query,
            min_senders=min_senders
        )

    # -------------------------------------------------
    # CIRCULAR MONEY FLOW
    # -------------------------------------------------

    def circular_paths(self):

        query = """
        MATCH p =
        (a:Account)-[:SENT]->(:Transaction)-[:TO]->
        (b:Account)-[:SENT]->(:Transaction)-[:TO]->
        (a)

        RETURN p
        LIMIT 10
        """

        return self.run_query(query)

    # -------------------------------------------------
    # ACCOUNT SUMMARY
    # -------------------------------------------------

    def account_summary(self, account_id):

        query = """
        MATCH (a:Account {account_id:$account_id})

        OPTIONAL MATCH (a)-[:SENT]->(t1:Transaction)
        WITH a,
             count(t1) AS sent_count,
             sum(t1.amount) AS sent_amount

        OPTIONAL MATCH (:Transaction)-[:TO]->(a)

        RETURN
            a.account_id AS account,
            sent_count,
            coalesce(sent_amount,0) AS total_sent,
            count(*) AS received_count
        """

        return self.run_query(
            query,
            account_id=account_id
        )