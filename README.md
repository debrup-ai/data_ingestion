# RAG Airflow Pipeline

This project implements a Retrieval Augmented Generation (RAG) system using Apache Airflow for workflow orchestration.

## Project Structure

- `dags/` - Airflow DAG definitions
- `etl/` - ETL components for extraction, transformation, and loading
  - `extract/` - Data extraction from various sources
  - `transform/` - Data transformation components
  - `embed/` - Vector embedding implementations
  - `load/` - Data loading to vector database
- `common/` - Common utilities and configurations
- `data/` - Sample data files
- `tests/` - Unit tests

## Components

### Data Extraction
Extract data from PDFs, URLs, and other sources.

### Data Transformation
Chunk, clean, and transform text data.

### Vector Embedding
Create vector embeddings using various models.

### Vector Storage
Store vectors in Qdrant vector database.

## Getting Started

### Prerequisites

- Docker
- Python 3.11+
- Qdrant vector database

### Setup

1. Start the Qdrant vector database:
   ```bash
   docker run -d -p 6333:6333 -p 6334:6334 -v ~/qdrant_storage:/qdrant/storage --name qdrant qdrant/qdrant
   ```

2. Set up the Airflow environment:
   ```bash
   ./start_airflow.sh
   ```

3. Access the Airflow UI at http://localhost:8080

### Running the RAG Pipeline

1. Trigger the data ingestion DAG from the Airflow UI
2. Monitor the progress through the Airflow interface
3. Query Qdrant to explore the embedded documents

## Development

To contribute to this project:

1. Create a new branch
2. Make your changes
3. Run tests: `pytest tests/`
4. Submit a pull request
