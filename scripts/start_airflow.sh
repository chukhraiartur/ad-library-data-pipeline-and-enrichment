#!/bin/bash

# Airflow startup script with automatic cleanup
# This script ensures Airflow starts properly and handles stuck tasks

set -e

echo "🚀 Starting Airflow with automatic cleanup..."

# Function to cleanup stuck tasks
cleanup_stuck_tasks() {
    echo "🧹 Running cleanup for stuck tasks..."
    docker-compose exec -T airflow python /opt/airflow/scripts/cleanup_stuck_tasks.py || true
}

# Function to check if Airflow is healthy
check_airflow_health() {
    local max_attempts=30
    local attempt=1
    
    echo "🏥 Checking Airflow health..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s --fail http://localhost:8080/health > /dev/null 2>&1; then
            echo "✅ Airflow is healthy!"
            return 0
        fi
        
        echo "⏳ Waiting for Airflow to be ready... (attempt $attempt/$max_attempts)"
        sleep 10
        attempt=$((attempt + 1))
    done
    
    echo "❌ Airflow failed to start properly"
    return 1
}

# Function to trigger pipeline
trigger_pipeline() {
    echo "🎯 Triggering ad_data_pipeline..."
    docker-compose exec -T airflow airflow dags trigger ad_data_pipeline || {
        echo "⚠️  Failed to trigger pipeline, but continuing..."
    }
}

# Main execution
main() {
    # Start containers
    echo "📦 Starting Docker containers..."
    docker-compose up -d
    
    # Wait for Airflow to be ready
    if check_airflow_health; then
        # Run initial cleanup
        cleanup_stuck_tasks
        
        # Trigger pipeline
        trigger_pipeline
        
        echo "🎉 Airflow is ready! Access it at: http://localhost:8080"
        echo "📊 Username: admin, Password: admin"
        echo ""
        echo "📋 Useful commands:"
        echo "  make airflow-cleanup    - Clean up stuck tasks"
        echo "  make pipeline-run       - Trigger pipeline"
        echo "  make pipeline-status    - Check pipeline status"
        echo "  make airflow-logs       - View logs"
        echo "  make airflow-restart    - Restart Airflow"
    else
        echo "❌ Failed to start Airflow properly"
        echo "📋 Check logs with: make airflow-logs"
        exit 1
    fi
}

# Run main function
main "$@" 