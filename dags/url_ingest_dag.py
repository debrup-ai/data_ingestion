from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os
import json
from typing import cast, List
from llama_index.core.schema import BaseNode, TextNode, Document


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
    'url_ingest_pipeline',
    default_args=default_args,
    description='RAG URL scraping and ingestion pipeline',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

# -----------------------------
# Task 1: Extract URL Data
# -----------------------------
def extract_url_data(**context):
    from rag_airflow.etl.extract.from_url import URLBaseExtractor

    conf = context.get('dag_run', {}).conf or {}
    url = conf.get('url', "https://en.wikipedia.org/wiki/Hotel_California#Recordings")
    print(f"[INFO] Scraping URL: {url}")

    extractor = URLBaseExtractor(url)
    try:
        markdown_content = extractor.extract()
        
        if not markdown_content:
            print("[WARNING] No content extracted from URL")
            return "No content extracted"

        # Create a Document object from the scraped content
        document = Document(
            text=markdown_content,
            metadata={
                'source': url,
                'type': 'web_scrape'
            }
        )
        
        documents = [document]
        
        output_path = "/tmp/extracted_url_docs.json"
        with open(output_path, "w") as f:
            json.dump([doc.to_dict() for doc in documents], f)

        context['task_instance'].xcom_push(key='doc_path', value=output_path)
        return f"Saved {len(documents)} docs from URL to {output_path}"
        
    except Exception as e:
        print(f"[ERROR] Failed to extract from URL {url}: {str(e)}")
        raise

# -----------------------------
# Task 2: Chunk Documents
# -----------------------------
def chunk_documents(**context):
    from rag_airflow.etl.transform.chunk import ChunkTransformer
    from llama_index.core.schema import Document

    ti = context['task_instance']
    doc_path = ti.xcom_pull(task_ids='extract_url_data', key='doc_path')

    with open(doc_path, "r") as f:
        documents = [Document.from_dict(d) for d in json.load(f)]

    transformer = ChunkTransformer(chunk_size=200, chunk_overlap=50)
    nodes = transformer.transform(documents)

    if nodes:
        print("[INFO] First node content:")
        print(nodes[0].get_content()[:500])

    chunk_output_path = "/tmp/url_chunks.json"
    with open(chunk_output_path, "w") as f:
        json.dump([node.to_dict() for node in nodes], f)

    ti.xcom_push(key='chunk_path', value=chunk_output_path)
    return f"Saved {len(nodes)} chunks to {chunk_output_path}"

# -----------------------------
# Task 3: Load to Qdrant
# -----------------------------
def load_to_qdrant(**context):
    from rag_airflow.etl.load.to_qdrant import ToQdrantLoader

    ti = context['task_instance']
    chunk_path = ti.xcom_pull(task_ids='chunk_documents', key='chunk_path')

    with open(chunk_path, "r") as f:
        node_dicts = json.load(f)

    # Convert all nodes to TextNodes
    text_nodes = []
    for node_dict in node_dicts:
        text_node = TextNode.from_dict(node_dict)
        text_nodes.append(text_node)
    
    print(f"[INFO] Loading {len(text_nodes)} nodes to Qdrant")

    loader = ToQdrantLoader()
    result = loader.load(text_nodes)
    return result

# -----------------------------
# DAG Task Definitions
# -----------------------------
url_task = PythonOperator(
    task_id='extract_url_data',
    python_callable=extract_url_data,
    dag=dag,
)

chunk_task = PythonOperator(
    task_id='chunk_documents',
    python_callable=chunk_documents,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_qdrant',
    python_callable=load_to_qdrant,
    dag=dag,
)

url_task >> chunk_task >> load_task

if __name__ == "__main__":
    dag.test() 