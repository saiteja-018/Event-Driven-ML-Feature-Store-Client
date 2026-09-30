import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

import os
import sys

os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
os.environ["POSTGRES_USER"] = "test"
os.environ["POSTGRES_PASSWORD"] = "test"
os.environ["POSTGRES_DB"] = "test"
os.environ["POSTGRES_HOST"] = "localhost"
os.environ["TESTING"] = "true"

sys.modules['src.db_manager'] = MagicMock()
sys.modules['src.consumer'] = MagicMock()

from src.main import app
from src.models import RawEvent
from pydantic import ValidationError

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

def test_raw_event_model_valid():
    data = {
        "event_id": "123",
        "entity_id": "user_1",
        "action_type": "click",
        "event_timestamp": "1234567890"
    }
    event = RawEvent(**data)
    assert event.event_id == "123"
    assert event.entity_id == "user_1"

def test_raw_event_model_invalid():
    data = {
        "event_id": "123",
        "action_type": "click",
        "event_timestamp": "1234567890"
    }
    with pytest.raises(ValidationError):
        RawEvent(**data)

@patch('src.main.db_manager')
def test_get_features_success(mock_db_manager, client):
    mock_db_manager.get_features.return_value = {"last_action": "click"}
    
    response = client.get("/features/user_1")
    assert response.status_code == 200
    assert response.json() == {"entity_id": "user_1", "features": {"last_action": "click"}}

@patch('src.main.db_manager')
def test_get_features_not_found(mock_db_manager, client):
    mock_db_manager.get_features.return_value = {}
    
    response = client.get("/features/unknown_user")
    assert response.status_code == 404
