from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class LogEntry(BaseModel):
    created_on: datetime = Field(default_factory=datetime.utcnow)
    company: int
    api_key: str
    user: int