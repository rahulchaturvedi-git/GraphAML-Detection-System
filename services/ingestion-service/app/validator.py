from pydantic import BaseModel, Field

class Transaction(BaseModel):
    transaction_id: str
    timestamp: int
    source_account: str
    destination_account: str
    amount: float = Field(gt=0)
    currency: str
    channel: str
    country: str