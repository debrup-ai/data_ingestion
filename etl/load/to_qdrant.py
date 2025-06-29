# to_qdrant.py

from .base import BaseLoader
from typing import List, Union
from llama_index.core.schema import TextNode
from llama_index.vector_stores.qdrant import QdrantVectorStore
from llama_index.core import VectorStoreIndex, StorageContext
from llama_index.embeddings.azure_openai import AzureOpenAIEmbedding
from qdrant_client import QdrantClient

class ToQdrantLoader(BaseLoader):
    def __init__(
        self,
        azure_api_key: str,
        azure_endpoint: str,
        deployment_name: str,
        collection_name: str = "rag_chunks",
        api_version: str = "2024-02-15-preview",
    ):
        # Setup Azure embedder
        self.embed_model = AzureOpenAIEmbedding(
            model="text-embedding-ada-002",  # Must match deployed model
            deployment_name=deployment_name,
            api_key=azure_api_key,
            azure_endpoint=azure_endpoint,
            api_version=api_version,
        )

        # Setup Qdrant client and vector store
        self.client = QdrantClient(url="http://localhost:6333")  # or remote URL
        self.vector_store = QdrantVectorStore(
            client=self.client,
            collection_name=collection_name
        )

    def load(self, data: List[Union[TextNode, dict]]):
        nodes = [
            TextNode.from_dict(d) if isinstance(d, dict) else d
            for d in data
        ]

        # Create index using Azure embedder and Qdrant vector store
        storage_context = StorageContext.from_defaults(vector_store=self.vector_store)
        index = VectorStoreIndex(
            nodes=nodes,
            storage_context=storage_context,
            embed_model=self.embed_model,
        )

        print(f"[SUCCESS] Uploaded {len(nodes)} nodes using AzureOpenAI embeddings to Qdrant")
        return f"{len(nodes)} nodes embedded via Azure and stored"
