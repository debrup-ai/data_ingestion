from base import AbstractExtractor
from llama_index.readers.google import GoogleDocsReader

class GoogleDriveExtractor(AbstractExtractor):
    def extract(self, document_ids: list[str]) -> str:
        document_ids = document_ids
        documents = GoogleDocsReader().load_data(document_ids=document_ids)
        return documents
    
    


