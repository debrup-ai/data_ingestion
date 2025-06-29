from .base import BaseLoader
from typing import List, Union
from llama_index.core.schema import TextNode
from llama_index.vector_stores.qdrant import QdrantVectorStore
from qdrant_client import QdrantClient
import concurrent.futures
import os
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
AZURE_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")

class ToQdrantLoader(BaseLoader):
    def __init__(
        self,
        collection_name: str = "rag_chunks",
        batch_size: int = 64,
        parallel_workers: int = 4,
    ):
        self.client = AzureOpenAI(
            api_key=AZURE_API_KEY,
            api_version=AZURE_API_VERSION,
            azure_endpoint=AZURE_ENDPOINT,
        )
        self.deployment_name = DEPLOYMENT_NAME

        self.vector_store = QdrantVectorStore(
            client=QdrantClient(url="http://localhost:6333"),
            collection_name=collection_name,
            batch_size=batch_size,
        )
        self.parallel_workers = parallel_workers

    def _embed_node(self, node: TextNode) -> TextNode:
        text = node.get_content()
        embedding = self.client.embeddings.create(
            model=self.deployment_name,
            input=text,
        ).data[0].embedding
        node.embedding = embedding
        return node

    def load(self, data: List[Union[TextNode, dict]]):
        nodes: List[TextNode] = [
            TextNode.from_dict(d) if isinstance(d, dict) else d
            for d in data
        ]

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
            embedded_nodes = list(executor.map(self._embed_node, nodes))

        # pyright: ignore
        ids = self.vector_store.add(embedded_nodes)
        print(f"[SUCCESS] Uploaded {len(ids)} embedded nodes to Qdrant")
        return f"{len(ids)} nodes embedded and uploaded to Qdrant"
