from fastapi import FastAPI
from app.service import AlertService

app = FastAPI()
service = AlertService()


@app.get("/alerts")
def get_alerts():
    return service.get_alerts()