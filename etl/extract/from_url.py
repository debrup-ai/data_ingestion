from .base import AbstractExtractor
from firecrawl import FirecrawlApp
import os
from dotenv import load_dotenv

load_dotenv()


class URLBaseExtractor(AbstractExtractor):
    def __init__(self, url: str):
        self.url = url
        self.app = FirecrawlApp(api_key=os.getenv("FIRECRAWL_API_KEY"))

    def extract(self):
        scrape_result = self.app.scrape_url(self.url, formats=['markdown', 'html'])
        if scrape_result.success and scrape_result.markdown:
            return scrape_result.markdown[:800]
        else:
            raise Exception(f"Failed to scrape URL: {self.url}")