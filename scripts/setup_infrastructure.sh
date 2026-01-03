#!/bin/bash

# Agentic Framework Infrastructure Setup Script

set -e

echo "========================================="
echo "Agentic Framework Infrastructure Setup"
echo "========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker first."
    echo "Visit: https://docs.docker.com/get-docker/"
    exit 1
fi

print_info "Docker is installed: $(docker --version)"

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    print_error "Docker Compose is not installed."
    echo "For Docker Desktop users, Docker Compose is included."
    echo "For Linux users: sudo apt install docker-compose"
    exit 1
fi

print_info "Docker Compose is available"

# Create necessary directories
print_info "Creating directories..."
mkdir -p ./dags/generated_dags
mkdir -p ./logs
mkdir -p ./data
mkdir -p ./config
mkdir -p ./workflows/examples
mkdir -p ./workflows/templates

# Copy configuration files if they don't exist
if [ ! -f ./config/kafka_config.yaml ]; then
    print_info "Creating Kafka configuration..."
    cat > ./config/kafka_config.yaml << 'EOF'
bootstrap.servers: localhost:9092
group.id: agentic_framework
auto.offset.reset: earliest
enable.auto.commit: true
EOF
fi

if [ ! -f ./config/airflow_config.yaml ]; then
    print_info "Creating Airflow configuration..."
    cat > ./config/airflow_config.yaml << 'EOF'
core:
  executor: CeleryExecutor
  parallelism: 32
  dag_concurrency: 16
  max_active_runs_per_dag: 16
  load_examples: false

database:
  sql_alchemy_conn: postgresql+psycopg2://airflow:airflow@postgres/airflow

celery:
  result_backend: db+postgresql://airflow:airflow@postgres/airflow
  broker_url: redis://redis:6379/0
EOF
fi

# Create environment file
if [ ! -f .env ]; then
    print_info "Creating environment file..."
    cat > .env << 'EOF'
# Agentic Framework Environment Variables

# Framework
FRAMEWORK_API_URL=http://localhost:8000
NODE_ENV=development
API_TOKENS=dev_token,admin_token

# Kafka
KAFKA_BROKERS=localhost:9092
KAFKA_GROUP_ID=agentic_framework

# Redis
REDIS_URL=redis://localhost:6379

# MongoDB
MONGODB_URL=mongodb://localhost:27017/agentic_framework

# OpenAI (optional)
OPENAI_API_KEY=your_openai_api_key_here

# Database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
EOF
    print_warning "Please update .env file with your actual API keys"
fi

# Create docker-compose.yml if it doesn't exist
if [ ! -f docker-compose.yml ]; then
    print_info "Creating docker-compose.yml..."
    cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes

  postgres:
    image: postgres:13-alpine
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  mongodb:
    image: mongo:6
    ports:
      - "27017:27017"
    volumes:
      - mongodb_data:/data/db

  airflow-init:
    image: apache/airflow:2.7.0
    depends_on:
      - postgres
      - redis
    environment:
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/0
    command: >
      bash -c "
      airflow db init &&
      airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs

  airflow-webserver:
    image: apache/airflow:2.7.0
    depends_on:
      - airflow-init
    ports:
      - "8080:8080"
    environment:
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/0
    command: webserver
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs

  airflow-scheduler:
    image: apache/airflow:2.7.0
    depends_on:
      - airflow-init
    environment:
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/0
    command: scheduler
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs

  airflow-worker:
    image: apache/airflow:2.7.0
    depends_on:
      - airflow-init
    environment:
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/0
    command: celery worker
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs

volumes:
  postgres_data:
  mongodb_data:
EOF
fi

# Create requirements.txt if it doesn't exist
if [ ! -f requirements.txt ]; then
    print_info "Creating requirements.txt..."
    cat > requirements.txt << 'EOF'
apache-airflow==2.7.0
confluent-kafka==2.2.0
fastapi==0.104.1
uvicorn==0.24.0
pydantic==2.5.0
jsonschema==4.19.2
redis==5.0.1
celery==5.3.4
sqlalchemy==2.0.23
pymongo==4.6.0
requests==2.31.0
openai==1.3.0
python-dotenv==1.0.0
python-multipart==0.0.6
httpx==0.25.1
websockets==12.0
python-jose[cryptography]==3.3.0
passlib[bcrypt]==1.7.4
EOF
fi

# Create example workflow
if [ ! -f ./workflows/examples/content_creation_workflow.json ]; then
    print_info "Creating example workflow..."
    cat > ./workflows/examples/content_creation_workflow.json << 'EOF'
{
  "workflow_id": "content_creation_v1",
  "name": "Content Creation Workflow",
  "description": "Creates blog content with human review and SEO optimization",
  "version": "1.0.0",
  "tasks": [
    {
      "task_id": "topic_research",
      "name": "Topic Research",
      "type": "ai_agent",
      "agent_type": "research_agent",
      "config": {
        "model": "gpt-4",
        "temperature": 0.7,
        "instructions": "Research trending topics in AI and machine learning"
      },
      "dependencies": [],
      "timeout_seconds": 300
    },
    {
      "task_id": "topic_approval",
      "name": "Topic Approval",
      "type": "human_task",
      "config": {
        "priority": "high",
        "instructions": "Review and select the best topic for the blog post",
        "assignee_group": "content_managers"
      },
      "dependencies": ["topic_research"]
    }
  ]
}
EOF
fi

# Start infrastructure using Docker Compose
print_info "Starting infrastructure containers..."

# Check if Docker Compose V2 is available
if docker compose version &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker compose"
else
    DOCKER_COMPOSE_CMD="docker-compose"
fi

$DOCKER_COMPOSE_CMD down -v 2>/dev/null || true
$DOCKER_COMPOSE_CMD up -d zookeeper kafka redis postgres mongodb

print_info "Waiting for services to start (30 seconds)..."
sleep 30

# Check if services are running
print_info "Checking service status..."
if ! docker ps | grep -q "zookeeper\|kafka\|redis\|postgres\|mongodb"; then
    print_error "Some services failed to start. Check logs with: docker-compose logs"
    exit 1
fi

# Initialize Kafka topics
print_info "Creating Kafka topics..."
docker exec -it agentic_framework_kafka_1 \
  kafka-topics --create \
  --topic workflow_events \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092 \
  --if-not-exists 2>/dev/null || \
  print_warning "Kafka topic creation may have failed. Continuing..."

# Start Airflow services
print_info "Starting Airflow services..."
$DOCKER_COMPOSE_CMD up -d airflow-init airflow-webserver airflow-scheduler airflow-worker

print_info "Waiting for Airflow to initialize (60 seconds)..."
sleep 60

# Install Python dependencies
print_info "Installing Python dependencies..."
if command -v pip3 &> /dev/null; then
    pip3 install -r requirements.txt || print_warning "Some dependencies may have failed to install"
elif command -v pip &> /dev/null; then
    pip install -r requirements.txt || print_warning "Some dependencies may have failed to install"
else
    print_warning "pip not found. Please install Python dependencies manually"
fi

# Create basic project structure
print_info "Creating project structure..."
mkdir -p framework/core
mkdir -p framework/integration
mkdir -p framework/agents
mkdir -p framework/schemas
mkdir -p api/endpoints

# Create a simple README
if [ ! -f README.md ]; then
    cat > README.md << 'EOF'
