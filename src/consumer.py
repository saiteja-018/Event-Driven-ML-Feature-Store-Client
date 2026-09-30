import json
import logging
import threading
import time
from confluent_kafka import Consumer, KafkaError, KafkaException
from pydantic import ValidationError
from src.models import RawEvent

logger = logging.getLogger(__name__)

class FeatureConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, db_manager):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.db_manager = db_manager
        self._stop_event = threading.Event()
        self.consumer = None

    def start_consuming(self):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'feature-store-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        
        retries = 5
        while retries > 0 and not self._stop_event.is_set():
            try:
                self.consumer = Consumer(conf)
                self.consumer.subscribe([self.topic])
                logger.info(f"Subscribed to topic {self.topic}")
                break
            except Exception as e:
                logger.error(f"Failed to create consumer: {e}. Retries left: {retries - 1}")
                retries -= 1
                time.sleep(2)
        
        if not self.consumer:
            logger.error("Could not start consumer")
            return

        while not self._stop_event.is_set():
            msg = self.consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka consumer error: {msg.error()}")
                    continue
            
            try:
                val = msg.value().decode('utf-8')
                data = json.loads(val)
                event = RawEvent(**data)
                
                self.db_manager.save_feature(event.entity_id, "last_action", event.action_type)
                
                self.consumer.commit(asynchronous=False)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON: {e}")
            except ValidationError as e:
                logger.error(f"Validation error: {e}")
            except Exception as e:
                logger.error(f"Unexpected error processing message: {e}")

    def stop(self):
        self._stop_event.set()
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
