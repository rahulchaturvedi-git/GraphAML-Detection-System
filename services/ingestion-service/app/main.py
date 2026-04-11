from fastapi import FastAPI, HTTPException
from app.validator import Transaction
from app.producer import send_transaction

app = FastAPI()

@app.post("/ingest")
def ingest(txn: Transaction):
    try:
        send_transaction(txn.dict())
        return {"status": "accepted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))