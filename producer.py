import os
import json
import uuid
import time
import random
import logging
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("RAW_EVENTS_TOPIC", "raw-events")
    
    conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(conf)
    
    user_ids = [f"user_{i}" for i in range(1, 6)]
    actions = ["click", "view", "purchase", "login", "logout"]
    
    logger.info("Starting to produce messages...")
    
    try:
        # Generate 1000 events
        for i in range(1000):
            event = {
                "event_id": str(uuid.uuid4()),
                "entity_id": random.choice(user_ids),
                "action_type": random.choice(actions),
                "event_timestamp": str(time.time())
            }
            
            producer.produce(
                topic,
                key=event["entity_id"],
                value=json.dumps(event),
                callback=delivery_report
            )
            # Sleep slightly to test throughput
            time.sleep(0.001)
            
            # Serve delivery reports
            producer.poll(0)
            
        logger.info("Flushing producer...")
        producer.flush()
        logger.info("Done producing messages.")
        
    except KeyboardInterrupt:
        logger.info("Stopped producing.")

if __name__ == "__main__":
    main()
