from .base import BaseTransformer
from langchain.text_splitter import RecursiveCharacterTextSplitter
from llama_index.core.node_parser import LangchainNodeParser



class ChunkTransformer(BaseTransformer):
    def __init__(self, chunk_size: int = 200, chunk_overlap: int = 50):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def transform(self, data):
        parser = LangchainNodeParser(RecursiveCharacterTextSplitter(chunk_size=self.chunk_size, chunk_overlap=self.chunk_overlap))
        nodes = parser.get_nodes_from_documents(data)
        return nodes