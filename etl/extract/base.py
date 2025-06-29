from abc import ABC, abstractmethod

class AbstractExtractor(ABC):
    @abstractmethod
    def extract(self, source_uri: str) -> str:
        pass