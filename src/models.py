from pydantic import BaseModel
from typing import Dict, Any

class RawEvent(BaseModel):
    event_id: str
    entity_id: str
    action_type: str
    event_timestamp: str

class FeatureResponse(BaseModel):
    entity_id: str
    features: Dict[str, str]
