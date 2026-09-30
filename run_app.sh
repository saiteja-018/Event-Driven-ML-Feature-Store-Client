#!/bin/bash

echo "Waiting for PostgreSQL to start..."
while ! nc -z $POSTGRES_HOST 5432; do
  sleep 1
done
echo "PostgreSQL started"

echo "Waiting for Kafka to start..."
# Note: In docker-compose, kafka is reachable at kafka:29092
# using the KAFKA_BOOTSTRAP_SERVERS environment variable.
KAFKA_HOST=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d: -f1)
KAFKA_PORT=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d: -f2)

while ! nc -z $KAFKA_HOST $KAFKA_PORT; do
  sleep 1
done
echo "Kafka started"

# Give kafka a few more seconds to fully initialize
sleep 5

# Start the FastAPI application
exec uvicorn src.main:app --host 0.0.0.0 --port 8000
