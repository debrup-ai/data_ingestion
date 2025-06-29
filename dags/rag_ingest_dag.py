from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os
import json

# Setup sys.path to import from data_ingestion
current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else '/home/zudu/data_ingestion/rag_airflow/dags'
data_ingestion_path = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, data_ingestion_path)

default_args = {
    'owner': 'rag_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_ingest_pipeline',
    default_args=default_args,
    description='RAG PDF extraction pipeline',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

# -----------------------------
# Task 1: Extract PDF to /tmp
# -----------------------------
def extract_pdf_data(**context):
    print("[INFO] Starting PDF extraction...")
    from rag_airflow.etl.extract.from_pdf import DataExtractor

    conf = context.get('dag_run', {}).conf or {}
    pdf_file = conf.get('pdf_file', "/home/zudu/data_ingestion/rag_airflow/data/README.md")
    print(f"[INFO] Processing: {pdf_file}")

    extractor = DataExtractor()
    documents = extractor.extract(pdf_file)

    if not documents:
        print("[WARNING] No documents extracted")
        return "No documents extracted"

    print(f"[SUCCESS] Extracted {len(documents)} documents")
    print("[INFO] First document preview:")
    print(documents[0].text[:200])  # Safe preview

    # Write documents to disk (JSON)
    output_path = "/tmp/extracted_docs.json"
    doc_dicts = [doc.to_dict() for doc in documents]
    with open(output_path, "w") as f:
        json.dump(doc_dicts, f)

    # Pass file path to next task via XCom
    context['task_instance'].xcom_push(key='doc_path', value=output_path)
    return f"Saved {len(doc_dicts)} docs to {output_path}"

# -----------------------------
# Task 2: Chunk Documents from /tmp
# -----------------------------
def chunk_documents(**context):
    print("[INFO] Starting document chunking...")
    from rag_airflow.etl.transform.chunk import ChunkTransformer
    from llama_index.core.schema import Document

    ti = context['task_instance']
    doc_path = ti.xcom_pull(task_ids='extract_pdf_data', key='doc_path')

    if not doc_path or not os.path.exists(doc_path):
        print("[ERROR] Document path not found")
        return "Missing input file"

    with open(doc_path, "r") as f:
        doc_dicts = json.load(f)

    documents = [Document.from_dict(d) for d in doc_dicts]
    print(f"[INFO] Chunking {len(documents)} documents")

    transformer = ChunkTransformer(chunk_size=200, chunk_overlap=50)
    nodes = transformer.transform(documents)

    print(f"[SUCCESS] Created {len(nodes)} chunks")

    # ✅ Print first node's content
    if nodes:
        print("[INFO] First node content:")
        print(nodes[0].get_content()[:500])  # Safe truncate

    # Optionally write chunks to disk
    chunk_output_path = "/tmp/chunks.json"
    node_dicts = [node.to_dict() for node in nodes]
    with open(chunk_output_path, "w") as f:
        json.dump(node_dicts, f)

    ti.xcom_push(key='chunk_path', value=chunk_output_path)
    return f"Saved {len(nodes)} chunks to {chunk_output_path}"

# -----------------------------
# Task Definitions
# -----------------------------
pdf_task = PythonOperator(
    task_id='extract_pdf_data',
    python_callable=extract_pdf_data,
    dag=dag,
)

chunk_task = PythonOperator(
    task_id='chunk_documents',
    python_callable=chunk_documents,
    dag=dag,
)

pdf_task >> chunk_task

if __name__ == "__main__":
    dag.test()
