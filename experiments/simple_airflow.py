# rag_ingest_pipeline_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
import requests
import time
import json

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Mock functions for RAG data ingestion pipeline

def ingest_from_url(**context):
    """Mock function to ingest text data from URLs"""
    print(f"[INFO] 🌐 Starting URL data ingestion...")
    
    # Get URLs from DAG configuration or use defaults
    conf = context.get('dag_run', {}).conf or {}
    urls = conf.get('urls', [
        'https://example.com/article1.html',
        'https://example.com/blog/post1.html',
        'https://docs.example.com/guide.html'
    ])
    
    extracted_texts = []
    
    for i, url in enumerate(urls):
        print(f"[INFO] 📄 Processing URL {i+1}/{len(urls)}: {url}")
        
        # Mock HTTP request simulation
        time.sleep(0.5)  # Simulate network delay
        
        # Mock extracted text content
        mock_content = f"""
        Mock article content from {url}
        This is a sample article about artificial intelligence and machine learning.
        It contains various sections about neural networks, deep learning, and RAG systems.
        The content would normally be extracted using web scraping tools like BeautifulSoup.
        """
        
        extracted_texts.append({
            'source': url,
            'content': mock_content.strip(),
            'metadata': {
                'source_type': 'url',
                'timestamp': context['ds'],
                'word_count': len(mock_content.split())
            }
        })
        
        print(f"[SUCCESS] ✅ Extracted {len(mock_content.split())} words from {url}")
    
    print(f"[INFO] 🎯 URL ingestion completed. Processed {len(urls)} URLs")
    
    # Push data to XCom for next tasks
    context['task_instance'].xcom_push(key='url_texts', value=extracted_texts)
    return f"Processed {len(urls)} URLs"

def ingest_from_pdfs(**context):
    """Mock function to ingest text data from PDF files"""
    print(f"[INFO] 📚 Starting PDF data ingestion...")
    
    # Get PDF paths from DAG configuration or use defaults
    conf = context.get('dag_run', {}).conf or {}
    pdf_paths = conf.get('pdf_paths', [
        '/data/documents/research_paper1.pdf',
        '/data/documents/manual.pdf',
        '/data/documents/report_2024.pdf'
    ])
    
    extracted_texts = []
    
    for i, pdf_path in enumerate(pdf_paths):
        print(f"[INFO] 📖 Processing PDF {i+1}/{len(pdf_paths)}: {pdf_path}")
        
        # Mock PDF processing simulation
        time.sleep(1)  # Simulate PDF parsing time
        
        # Mock extracted PDF content
        mock_content = f"""
        Mock PDF content from {pdf_path}
        This document contains technical information about data engineering and MLOps.
        It includes sections on data pipelines, model deployment, and monitoring strategies.
        The content would normally be extracted using libraries like PyPDF2 or pdfplumber.
        Additional content with charts, tables, and technical specifications.
        """
        
        extracted_texts.append({
            'source': pdf_path,
            'content': mock_content.strip(),
            'metadata': {
                'source_type': 'pdf',
                'timestamp': context['ds'],
                'word_count': len(mock_content.split()),
                'pages': 15  # Mock page count
            }
        })
        
        print(f"[SUCCESS] ✅ Extracted {len(mock_content.split())} words from {pdf_path}")
    
    print(f"[INFO] 🎯 PDF ingestion completed. Processed {len(pdf_paths)} PDFs")
    
    # Push data to XCom for next tasks
    context['task_instance'].xcom_push(key='pdf_texts', value=extracted_texts)
    return f"Processed {len(pdf_paths)} PDFs"

def ingest_from_text_files(**context):
    """Mock function to ingest data from text files"""
    print(f"[INFO] 📝 Starting text file data ingestion...")
    
    # Get text file paths from DAG configuration or use defaults
    conf = context.get('dag_run', {}).conf or {}
    text_paths = conf.get('text_paths', [
        '/data/texts/knowledge_base.txt',
        '/data/texts/faq.txt',
        '/data/texts/procedures.txt'
    ])
    
    extracted_texts = []
    
    for i, text_path in enumerate(text_paths):
        print(f"[INFO] 📄 Processing text file {i+1}/{len(text_paths)}: {text_path}")
        
        # Mock file reading simulation
        time.sleep(0.3)  # Simulate file I/O
        
        # Mock text file content
        mock_content = f"""
        Mock text content from {text_path}
        This file contains structured knowledge about our organization's processes.
        It includes frequently asked questions, standard operating procedures,
        and detailed explanations of various business workflows.
        The content would normally be read directly from text files.
        """
        
        extracted_texts.append({
            'source': text_path,
            'content': mock_content.strip(),
            'metadata': {
                'source_type': 'text_file',
                'timestamp': context['ds'],
                'word_count': len(mock_content.split()),
                'file_size': '2.5KB'  # Mock file size
            }
        })
        
        print(f"[SUCCESS] ✅ Extracted {len(mock_content.split())} words from {text_path}")
    
    print(f"[INFO] 🎯 Text file ingestion completed. Processed {len(text_paths)} files")
    
    # Push data to XCom for next tasks
    context['task_instance'].xcom_push(key='text_file_texts', value=extracted_texts)
    return f"Processed {len(text_paths)} text files"

def chunk_text_data(**context):
    """Mock function to chunk text data for better RAG performance"""
    print(f"[INFO] ✂️ Starting text chunking process...")
    
    # Pull data from previous tasks
    url_texts = context['task_instance'].xcom_pull(task_ids='ingest_url_data', key='url_texts') or []
    pdf_texts = context['task_instance'].xcom_pull(task_ids='ingest_pdf_data', key='pdf_texts') or []
    text_file_texts = context['task_instance'].xcom_pull(task_ids='ingest_text_file_data', key='text_file_texts') or []
    
    all_texts = url_texts + pdf_texts + text_file_texts
    print(f"[INFO] 📊 Total documents to chunk: {len(all_texts)}")
    
    chunked_data = []
    chunk_size = 500  # Mock chunk size in words
    overlap = 50     # Mock overlap between chunks
    
    for doc_idx, document in enumerate(all_texts):
        print(f"[INFO] 🔪 Chunking document {doc_idx + 1}: {document['source']}")
        
        words = document['content'].split()
        total_words = len(words)
        
        # Create overlapping chunks
        start_idx = 0
        chunk_idx = 0
        
        while start_idx < total_words:
            end_idx = min(start_idx + chunk_size, total_words)
            chunk_text = ' '.join(words[start_idx:end_idx])
            
            chunk_data = {
                'chunk_id': f"{doc_idx}_{chunk_idx}",
                'source': document['source'],
                'source_type': document['metadata']['source_type'],
                'content': chunk_text,
                'metadata': {
                    'chunk_index': chunk_idx,
                    'start_word': start_idx,
                    'end_word': end_idx,
                    'chunk_word_count': len(chunk_text.split()),
                    'parent_document': document['source']
                }
            }
            
            chunked_data.append(chunk_data)
            print(f"[DEBUG] 📝 Created chunk {chunk_idx}: {len(chunk_text.split())} words")
            
            # Move to next chunk with overlap
            start_idx = max(start_idx + chunk_size - overlap, end_idx)
            chunk_idx += 1
    
    print(f"[SUCCESS] ✅ Text chunking completed. Created {len(chunked_data)} chunks")
    
    # Push chunked data to XCom
    context['task_instance'].xcom_push(key='chunked_texts', value=chunked_data)
    return f"Created {len(chunked_data)} text chunks"

def generate_embeddings(**context):
    """Mock function to generate embeddings for text chunks"""
    print(f"[INFO] 🧠 Starting embedding generation...")
    
    # Pull chunked data from previous task
    chunked_texts = context['task_instance'].xcom_pull(task_ids='chunk_text_data', key='chunked_texts') or []
    print(f"[INFO] 📊 Total chunks to embed: {len(chunked_texts)}")
    
    embedded_chunks = []
    
    for i, chunk in enumerate(chunked_texts):
        print(f"[INFO] 🔄 Generating embedding for chunk {i + 1}/{len(chunked_texts)}")
        
        # Mock embedding generation (normally would use OpenAI, Hugging Face, etc.)
        time.sleep(0.1)  # Simulate API call time
        
        # Mock 768-dimensional embedding vector
        mock_embedding = [0.1 * (i % 100) + 0.01 * j for j in range(768)]
        
        embedded_chunk = {
            **chunk,
            'embedding': mock_embedding,
            'embedding_model': 'mock-text-embedding-ada-002',
            'embedding_dimensions': 768
        }
        
        embedded_chunks.append(embedded_chunk)
        
        if (i + 1) % 10 == 0:
            print(f"[PROGRESS] 📈 Processed {i + 1}/{len(chunked_texts)} embeddings")
    
    print(f"[SUCCESS] ✅ Embedding generation completed for {len(embedded_chunks)} chunks")
    
    # Push embedded data to XCom
    context['task_instance'].xcom_push(key='embedded_chunks', value=embedded_chunks)
    return f"Generated embeddings for {len(embedded_chunks)} chunks"

def store_in_vector_database(**context):
    """Mock function to store embeddings in vector database"""
    print(f"[INFO] 🗄️ Starting vector database storage...")
    
    # Pull embedded data from previous task
    embedded_chunks = context['task_instance'].xcom_pull(task_ids='generate_embeddings', key='embedded_chunks') or []
    print(f"[INFO] 📊 Total embedded chunks to store: {len(embedded_chunks)}")
    
    # Mock vector database operations
    database_collections = {
        'url': 'rag_url_collection',
        'pdf': 'rag_pdf_collection', 
        'text_file': 'rag_textfile_collection'
    }
    
    storage_stats = {collection: 0 for collection in database_collections.values()}
    
    for i, chunk in enumerate(embedded_chunks):
        source_type = chunk['source_type']
        collection = database_collections.get(source_type, 'rag_default_collection')
        
        # Mock database insertion
        print(f"[INFO] 💾 Storing chunk {chunk['chunk_id']} in collection '{collection}'")
        time.sleep(0.05)  # Simulate database write time
        
        # Mock vector database record
        vector_record = {
            'id': chunk['chunk_id'],
            'vector': chunk['embedding'],
            'metadata': {
                'source': chunk['source'],
                'source_type': chunk['source_type'],
                'content': chunk['content'][:200] + '...',  # Truncated for storage
                'chunk_metadata': chunk['metadata']
            }
        }
        
        storage_stats[collection] += 1
        
        if (i + 1) % 20 == 0:
            print(f"[PROGRESS] 📈 Stored {i + 1}/{len(embedded_chunks)} chunks")
    
    # Print storage summary
    print(f"[INFO] 📋 Storage Summary:")
    total_stored = 0
    for collection, count in storage_stats.items():
        if count > 0:
            print(f"[INFO] 📦 Collection '{collection}': {count} chunks")
            total_stored += count
    
    print(f"[SUCCESS] ✅ Vector database storage completed. Total chunks stored: {total_stored}")
    
    # Create final summary
    summary = {
        'total_chunks_stored': total_stored,
        'collections': storage_stats,
        'processing_date': context['ds'],
        'status': 'completed'
    }
    
    context['task_instance'].xcom_push(key='storage_summary', value=summary)
    return f"Stored {total_stored} chunks in vector database"

def pipeline_summary(**context):
    """Generate final pipeline summary"""
    print(f"[INFO] 📊 Generating RAG ingestion pipeline summary...")
    
    # Pull summary data from all tasks
    storage_summary = context['task_instance'].xcom_pull(task_ids='store_in_vector_db', key='storage_summary') or {}
    
    print(f"""
    [SUMMARY] 🎯 RAG Data Ingestion Pipeline Completed Successfully!
    
    📋 Pipeline Summary:
    ==================
    📅 Processing Date: {context['ds']}
    🗄️ Total Chunks Stored: {storage_summary.get('total_chunks_stored', 'N/A')}
    
    📊 By Source Type:
    🌐 URL Collections: {storage_summary.get('collections', {}).get('rag_url_collection', 0)} chunks
    📚 PDF Collections: {storage_summary.get('collections', {}).get('rag_pdf_collection', 0)} chunks  
    📝 Text File Collections: {storage_summary.get('collections', {}).get('rag_textfile_collection', 0)} chunks
    
    ✅ Status: {storage_summary.get('status', 'Unknown')}
    
    🚀 Your RAG system is now ready for semantic search and question answering!
    """)
    
    return "Pipeline summary completed"

# Define DAG
with DAG(
    dag_id='rag_ingest_pipeline',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    description='Comprehensive RAG Data Ingestion Pipeline with URL, PDF, and Text File Sources',
    tags=['rag', 'data-ingestion', 'ml', 'nlp']
) as dag:

    # Data ingestion tasks (can run in parallel)
    task_ingest_urls = PythonOperator(
        task_id='ingest_url_data',
        python_callable=ingest_from_url,
    )
    
    task_ingest_pdfs = PythonOperator(
        task_id='ingest_pdf_data',
        python_callable=ingest_from_pdfs,
    )
    
    task_ingest_text_files = PythonOperator(
        task_id='ingest_text_file_data',
        python_callable=ingest_from_text_files,
    )
    
    # Text processing tasks (sequential after ingestion)
    task_chunk_texts = PythonOperator(
        task_id='chunk_text_data',
        python_callable=chunk_text_data,
    )
    
    task_generate_embeddings = PythonOperator(
        task_id='generate_embeddings',
        python_callable=generate_embeddings,
    )
    
    task_store_vectors = PythonOperator(
        task_id='store_in_vector_db',
        python_callable=store_in_vector_database,
    )
    
    task_summary = PythonOperator(
        task_id='pipeline_summary',
        python_callable=pipeline_summary,
    )

    # Define task dependencies
    # Parallel data ingestion
    [task_ingest_urls, task_ingest_pdfs, task_ingest_text_files] >> task_chunk_texts
    
    # Sequential processing pipeline
    task_chunk_texts >> task_generate_embeddings >> task_store_vectors >> task_summary
