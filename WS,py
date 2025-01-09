import requests
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import logging
import json
import re
import argparse
from urllib.parse import urljoin, urlparse
from datetime import datetime
from fake_useragent import UserAgent
import pandas as pd
from aiohttp_proxy import ProxyConnector
import time

class AdvancedScraper:
    def __init__(self, base_url, delay=1):
        self.base_url = base_url
        self.delay = delay
        self.visited = set()
        self.data = []
        self.setup_logging()
        self.ua = UserAgent()
        self.proxies = self.load_proxies()
        self.current_proxy = 0
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename=f'scraping_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )

    def load_proxies(self):
        try:
            with open('proxies.txt', 'r') as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            return []

    def rotate_proxy(self):
        if self.proxies:
            self.current_proxy = (self.current_proxy + 1) % len(self.proxies)
            return self.proxies[self.current_proxy]
        return None

    async def fetch_page(self, url, session):
        try:
            headers = {
                'User-Agent': self.ua.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logging.warning(f"Failed to fetch {url}: Status {response.status}")
                    return None
        except Exception as e:
            logging.error(f"Error fetching {url}: {str(e)}")
            return None

    def parse_page(self, html, url):
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        page_data = {
            'url': url,
            'timestamp': datetime.now().isoformat(),
            'title': soup.title.string if soup.title else None,
            'links': [],
            'emails': [],
            'phones': [],
            'forms': [],
            'metadata': {}
        }

        # Extract links
        for link in soup.find_all('a'):
            href = link.get('href')
            if href:
                full_url = urljoin(url, href)
                page_data['links'].append({
                    'text': link.text.strip(),
                    'url': full_url
                })

        # Extract emails
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        emails = re.findall(email_pattern, html)
        page_data['emails'] = list(set(emails))

        # Extract phone numbers
        phone_pattern = r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b'
        phones = re.findall(phone_pattern, html)
        page_data['phones'] = list(set(phones))

        # Extract forms
        for form in soup.find_all('form'):
            form_data = {
                'action': form.get('action'),
                'method': form.get('method'),
                'inputs': []
            }
            
            for input_tag in form.find_all('input'):
                form_data['inputs'].append({
                    'name': input_tag.get('name'),
                    'type': input_tag.get('type'),
                    'id': input_tag.get('id')
                })
            
            page_data['forms'].append(form_data)

        # Extract metadata
        for meta in soup.find_all('meta'):
            name = meta.get('name', meta.get('property', ''))
            content = meta.get('content', '')
            if name and content:
                page_data['metadata'][name] = content

        return page_data

    async def crawl(self, max_pages=None):
        connector = ProxyConnector.from_url(self.rotate_proxy()) if self.proxies else None
        
        async with aiohttp.ClientSession(connector=connector) as session:
            queue = [self.base_url]
            while queue and (max_pages is None or len(self.visited) < max_pages):
                current_url = queue.pop(0)
                
                if current_url in self.visited:
                    continue
                    
                self.visited.add(current_url)
                logging.info(f"Crawling: {current_url}")
                
                html = await self.fetch_page(current_url, session)
                if html:
                    page_data = self.parse_page(html, current_url)
                    self.data.append(page_data)
                    
                    # Add new URLs to queue
                    for link in page_data['links']:
                        if self.should_crawl(link['url']):
                            queue.append(link['url'])
                
                await asyncio.sleep(self.delay)

    def should_crawl(self, url):
        parsed = urlparse(url)
        base_parsed = urlparse(self.base_url)
        return (
            parsed.netloc == base_parsed.netloc and
            url not in self.visited and
            not url.endswith(('.pdf', '.jpg', '.png', '.gif'))
        )

    def export_data(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Export to JSON
        with open(f'scraping_results_{timestamp}.json', 'w') as f:
            json.dump(self.data, f, indent=4)
        
        # Export to CSV
        df = pd.json_normalize(self.data)
        df.to_csv(f'scraping_results_{timestamp}.csv', index=False)
        
        # Generate summary report
        summary = {
            'pages_scraped': len(self.visited),
            'total_links': sum(len(d['links']) for d in self.data),
            'total_emails': sum(len(d['emails']) for d in self.data),
            'total_phones': sum(len(d['phones']) for d in self.data),
            'total_forms': sum(len(d['forms']) for d in self.data),
            'start_time': min(d['timestamp'] for d in self.data),
            'end_time': max(d['timestamp'] for d in self.data)
        }
        
        with open(f'scraping_summary_{timestamp}.json', 'w') as f:
            json.dump(summary, f, indent=4)

def main():
    parser = argparse.ArgumentParser(description='Advanced Web Scraper')
    parser.add_argument('url', help='Starting URL')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay between requests')
    parser.add_argument('--max-pages', type=int, help='Maximum pages to scrape')
    
    args = parser.parse_args()
    
    scraper = AdvancedScraper(args.url, args.delay)
    
    print(f"\nStarting scrape of {args.url}")
    asyncio.run(scraper.crawl(args.max_pages))
    
    scraper.export_data()
    print(f"\nScraping complete. Processed {len(scraper.visited)} pages.")

if __name__ == "__main__":
    main()
