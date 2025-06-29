# RAG Pipeline Test Data

## PDF Extraction Testing

This folder contains test data for the RAG ingestion pipeline.

### Test Configuration

To test the PDF extraction DAG, use this configuration:

```json
{
    "pdf_file": "/data/sample.pdf"
}
```

### Expected Output

The `extract_pdf_data` task will:
1. Load the PDF file using DataExtractor
2. Extract document objects with llama-index
3. Push documents to XCom with key `documents`
4. Return processing status

### Sample Document Structure

```python
# Expected document object structure
{
    "text": "Extracted PDF content...",
    "metadata": {
        "file_path": "/data/sample.pdf",
        "file_name": "sample.pdf"
    }
}
```

### Testing Steps

1. Place your test PDF file in this data folder
2. Update the DAG configuration with the correct path
3. Trigger the DAG manually in Airflow UI
4. Check task logs for extraction status
5. Verify XCom data contains the extracted documents

### Notes

- Single PDF file processing only
- Returns raw document objects from llama-index
- No chunking or embedding in this first component 