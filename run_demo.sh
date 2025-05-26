#!/bin/bash

# filepath: run_demo.sh

# Set the project directory
PROJECT_DIR=$(pwd)

# Install dependencies using uv
echo "Installing dependencies using uv..."
uv install

# Run the Generator Pipeline
echo "Running the Generator Pipeline..."
uv run generator_pipeline.py

# Run the Consumer Pipeline
echo "Running the Consumer Pipeline..."
uv run consumer_pipeline.py

# Start the REST API
echo "Starting the REST API..."
uv run uvicorn src.app.rest_api:app --reload

echo "Data Pipeline Demo started successfully!"
echo "Access the REST API documentation at http://localhost:8000/docs"
