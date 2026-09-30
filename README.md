# Event-Driven ML Feature Store Client

An event-driven machine learning feature store client built with Python, Apache Kafka, PostgreSQL, and FastAPI. This project demonstrates a production-grade MLOps architecture for real-time feature computation and low-latency serving.

## 🏗 Architecture Overview

In modern machine learning systems, providing fresh, relevant features to models at inference time is a critical operational challenge. This system addresses this bottleneck by ingesting raw data events in real-time, instantly processing them into aggregated features, and making these features available with millisecond latency.

### The Pipeline
1. **Event Generation**: Upstream services (simulated by `producer.py`) publish raw JSON user events (e.g., clicks, views, purchases) into an Apache Kafka topic (`raw-events`).
2. **Ingestion & Processing**: A background Python consumer thread continuously polls Kafka, validates incoming JSON payloads against strict schemas using Pydantic, and computes the latest features.
3. **Idempotent Storage**: The processed features are stored in a PostgreSQL database acting as the "online" feature store. The system uses highly optimized UPSERT (`INSERT ... ON CONFLICT DO UPDATE`) constraints to handle duplicate Kafka messages gracefully.
4. **Feature Serving**: A high-performance FastAPI server exposes the computed features via a REST endpoint (`/features/{entity_id}`) for downstream ML model inference services.

## 🚀 Tech Stack

- **Apache Kafka & Zookeeper**: Distributed event streaming.
- **Python 3.10**: Core programming language.
- **FastAPI & Uvicorn**: High-performance asynchronous REST API.
- **Pydantic**: Strict data validation and typing.
- **PostgreSQL**: Relational database for online feature serving.
- **Docker & Docker Compose**: Containerization and infrastructure orchestration.
- **confluent-kafka**: High-performance Kafka client wrapper.
- **Pytest**: Unit testing framework.

## ⚙️ Local Development Setup

The entire architecture is containerized and orchestrated via Docker Compose, making it trivial to spin up on any machine.

### Prerequisites
- Docker and Docker Compose installed.
- Python 3.10+ (if running the producer locally).

### 1. Start the Infrastructure
Clone the repository and spin up the Docker containers:

```bash
git clone https://github.com/saiteja-018/Event-Driven-ML-Feature-Store-Client.git
cd Event-Driven-ML-Feature-Store-Client

# Start Kafka, Zookeeper, PostgreSQL, and the FastAPI application
docker-compose up --build -d
```

### 2. Verify the Services
The `docker-compose` setup uses a specialized `run_app.sh` script to ensure that the FastAPI application only boots *after* PostgreSQL and Kafka are healthy. 

Check the logs to verify a successful startup:
```bash
docker-compose logs -f feature-client
```

## 🧪 Testing the Data Pipeline

### 1. Generate Raw Events
We provide a standalone producer script that simulates a high-throughput upstream microservice pushing 1,000 raw events to Kafka.

You can run this directly within the container:
```bash
docker-compose exec feature-client python producer.py
```

### 2. Query the Feature Store
Once events are being produced, the background consumer thread immediately processes and upserts them to PostgreSQL. You can query the REST API to see the fresh features in real-time:

```bash
curl http://localhost:8000/features/user_1
```
*Expected Output:*
```json
{"entity_id": "user_1", "features": {"last_action": "click"}}
```

## ✅ Running Unit Tests

The repository includes a comprehensive `pytest` suite that mocks out PostgreSQL and Kafka dependencies to validate Pydantic schemas and FastAPI route behaviors in total isolation.

Run the tests inside the container:
```bash
docker-compose exec feature-client pytest tests/
```

## 🛡 System Resilience & Idempotency

Distributed systems guarantee at-least-once delivery, meaning duplicate messages are inevitable. This system handles this safely:
1. **Schema Validation**: Pydantic strictly validates all incoming JSON events. Malformed messages are securely logged and skipped, preventing consumer crashes.
2. **Idempotent Upserts**: The PostgreSQL schema uses a composite primary key (`entity_id`, `feature_name`). If a duplicate message arrives, the DB executes an `ON CONFLICT DO UPDATE`, ensuring duplicate processing does not skew the final state.
3. **Graceful Degradation**: Both the Kafka consumer and DB manager are wrapped in `try...except` blocks with automatic retry logic for transient network failures.
