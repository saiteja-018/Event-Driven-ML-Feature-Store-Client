import os
import logging
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from src.db_manager import PostgreSQLManager
from src.consumer import FeatureConsumer
from src.models import FeatureResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_manager = None
feature_consumer = None
consumer_thread = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.getenv("TESTING") == "true":
        yield
        return

    global db_manager, feature_consumer, consumer_thread
    
    postgres_user = os.getenv("POSTGRES_USER", "postgres")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    postgres_db = os.getenv("POSTGRES_DB", "feature_store")
    postgres_host = os.getenv("POSTGRES_HOST", "localhost")
    dsn = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:5432/{postgres_db}"
    
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("RAW_EVENTS_TOPIC", "raw-events")
    
    logger.info("Initializing Database Manager")
    db_manager = PostgreSQLManager(dsn)
    
    logger.info("Initializing Feature Consumer")
    feature_consumer = FeatureConsumer(kafka_servers, topic, db_manager)
    
    consumer_thread = threading.Thread(target=feature_consumer.start_consuming, daemon=True)
    consumer_thread.start()
    
    yield
    
    logger.info("Shutting down...")
    if feature_consumer:
        feature_consumer.stop()
    if consumer_thread:
        consumer_thread.join()
    if db_manager:
        db_manager.close()

app = FastAPI(lifespan=lifespan)

@app.get("/features/{entity_id}", response_model=FeatureResponse)
def get_features(entity_id: str):
    if not db_manager:
        raise HTTPException(status_code=500, detail="Database manager not initialized")
    
    features = db_manager.get_features(entity_id)
    if not features:
        raise HTTPException(status_code=404, detail="Entity not found or no features available")
    
    return FeatureResponse(entity_id=entity_id, features=features)
