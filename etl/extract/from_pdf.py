from .base import AbstractExtractor
from llama_index.core import SimpleDirectoryReader, Document
import os

class DataExtractor(AbstractExtractor):
    def extract(self, source_uri: str) -> list[Document]:
        
        print(f"Extracting from file: {source_uri}")
        reader = SimpleDirectoryReader(input_files=[source_uri])
        
        documents = reader.load_data()
        
        return documents
    
    
