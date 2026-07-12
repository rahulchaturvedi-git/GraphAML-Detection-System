from fastapi import FastAPI
from app.graph_analytics import GraphAnalytics

app = FastAPI(
    title="AML Graph Analytics Service"
)

analytics = GraphAnalytics()


@app.get("/")
def home():

    return {
        "service": "Graph Analytics",
        "status": "running"
    }


@app.get("/top-senders")
def top_senders():

    return analytics.top_senders()


@app.get("/most-active")
def most_active():

    return analytics.most_active()


@app.get("/high-risk")
def high_risk():

    return analytics.high_risk_accounts()


@app.get("/largest-transactions")
def largest_transactions():

    return analytics.largest_transactions()


@app.get("/fan-out")
def fan_out():

    return analytics.fan_out_accounts()


@app.get("/fan-in")
def fan_in():

    return analytics.fan_in_accounts()


@app.get("/circular-paths")
def circular_paths():

    return analytics.circular_paths()