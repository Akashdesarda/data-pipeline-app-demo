# Data Pipeline App Demo

This project demonstrates a data pipeline application that generates, processes, and analyzes data using modern data engineering tools and techniques. The pipeline is divided into two major components: the **Generator Pipeline** and the **Consumer Pipeline**. Additionally, a FastAPI-based REST API is provided for querying KPIs (Key Performance Indicators) derived from the processed data.

---

## Table of Contents

- [Data Pipeline App Demo](#data-pipeline-app-demo)
  - [Table of Contents](#table-of-contents)
  - [Project Setup](#project-setup)
  - [Tech Stack](#tech-stack)
  - [Pipeline Overview](#pipeline-overview)
    - [Generator Pipeline](#generator-pipeline)
    - [Consumer Pipeline](#consumer-pipeline)
  - [Rest API](#rest-api)
  - [Directory Structure](#directory-structure)

---

## Project Setup

Follow these steps to set up and run the project:

1. **Clone the Repository**:

   ```bash
   git clone <repository-url>
   cd data-pipeline-app-demo
    ```

2. Install Dependencies: This project uses uv for dependency management. Install [uv](https://docs.astral.sh/uv/) if not already installed:

3. Run the pipeline
   - To run the Generator Pipeline, execute:

    ```bash
    uv run generator_pipeline.py
    ```

   - To run the Consumer Pipeline, execute:

    ```bash
    uv run consumer_pipeline.py
    ```

4. **Start the REST API**:

   ```bash
   uv run uvicorn src.app.rest_api:app --reload
   ```

5. **Access the REST API**: Open your browser and navigate to `http://localhost:8000/docs` to view the API documentation and test the endpoints.

## Tech Stack

he project leverages the following technologies:

- `Python 3.13+`: Core programming language.
- `Polars`: High-performance DataFrame library for data manipulation.
- `FastAPI`: Framework for building RESTful APIs.
- `Azure` Storage Blob: For storing data in Azure Blob Storage.
- `PostgreSQL`: Relational database for structured data storage.
- `Kafka`: Message broker for streaming data.
- `Delta` Lake: Storage layer for managing data lakes.
- `UV`: Build system for Python projects.

## Pipeline Overview

### Generator Pipeline

The Generator Pipeline is responsible for generating synthetic data and publishing it to various storage locations.

**Steps:**

1. **Data Generation**:

- Generates 50 dummy records for:
  - Partner agency booking data.
  - Clickstream events.
- Uses polars.LazyFrame to optimize memory usage.
  
2. **Data Publishing:**

- Partner Agency Booking Data:
  - Stored as CSV and Parquet files in Azure Blob Storage.
  - Inserted into a PostgreSQL table (pipeline.partner_agency_booking).
- Clickstream Data:
  - Inserted into a PostgreSQL table (pipeline.clickstream_data).
  - Published to a Kafka topic (topic_0).

### Consumer Pipeline

The Consumer Pipeline processes the generated data, merges it into Delta tables, and transforms it into a star schema for analytical purposes.

**Steps:**

1. **Data Ingestion:**

- Reads partner agency booking data from:
  - Azure Blob Storage (CSV files).
  - PostgreSQL database.
- Reads clickstream data from:
  - PostgreSQL database.
  - Kafka topic.

2. **Data Processing:**

- Merges data into Delta tables stored on disk.
- Ensures upserts using Delta Lake's merge functionality.

3. **Data Transformation:**

- Transforms partner agency booking data into a star schema:
  - Dimension Tables: dim_time, dim_product, dim_customer.
  - Fact Table: fact_orders.
- Saves transformed data to the "gold" layer.

## Rest API

The REST API provides endpoints to query KPIs derived from the processed data. It uses FastAPI for building the API and provides interactive documentation.

## Directory Structure

The project follows a structured directory which is packaged as a Python module. Everywhere the code is referred as python package.

````markdown
data-pipeline-app-demo/
├── data/                     # Local storage for Delta tables
├── src/                      # Source code
│   ├── app/                  # FastAPI application
│   ├── connector/            # Data connectors for Azure, PostgreSQL, Kafka
│   ├── models/               # Data models and schemas
│   ├── ops/                  # Operations for data generation, publishing, and transformation
├── generator_pipeline.py     # Generator pipeline script
├── consumer_pipeline.py      # Consumer pipeline script
├── pyproject.toml            # Project configuration and dependencies
├── README.md                 # Project documentation
````
