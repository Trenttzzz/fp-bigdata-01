#!/bin/bash

echo "=== Big Data Lakehouse Status Check ==="

# Check Docker Compose services
echo "Docker Compose Services:"
docker-compose ps

echo ""
echo "=== Health Check Details ==="

services=("minio" "postgres" "kafka" "zookeeper" "spark-master" "mlflow" "jupyter" "batch-trainer")

for service in "${services[@]}"; do
    echo "Checking $service..."
    status=$(docker-compose ps $service --format "table {{.State}}")
    echo "  Status: $status"
    
    if [[ "$status" == *"unhealthy"* ]] || [[ "$status" == *"exited"* ]]; then
        echo "  ❌ $service has issues. Check logs:"
        echo "     docker-compose logs $service"
    elif [[ "$status" == *"running"* ]] || [[ "$status" == *"healthy"* ]]; then
        echo "  ✅ $service is running"
    fi
    echo ""
done

echo "=== Quick Access URLs ==="
echo "JupyterLab: http://localhost:8888"
echo "MLflow UI: http://localhost:5000"
echo "Model API: http://localhost:8000/docs"
echo "Spark UI: http://localhost:8081"
echo "MinIO Console: http://localhost:9001 (admin/password123)"

echo ""
echo "=== Key Commands ==="
echo "Start all: docker-compose up -d"
echo "Stop all: docker-compose down"
echo "View logs: docker-compose logs <service-name>"
echo "Monitor batch training: docker-compose logs -f batch-trainer"
