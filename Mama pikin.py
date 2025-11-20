"""
Comprehensive Web Scraper for Nigerian Health Data
Purpose: Analysis only - gathering public health information for research
Ethical scraping with rate limits and robots.txt compliance
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Set
import os
from urllib.robotparser import RobotFileParser

# ========================================
# Configuration
# ========================================
class ScraperConfig:
    # Headers to identify ourselves (ethical scraping)
    HEADERS = {
        'User-Agent': 'ResearchBot/1.0 (Health Data Analysis; contact@example.com)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    }
    
    # Rate limiting (be respectful!)
    REQUEST_DELAY = 2  # seconds between requests
    MAX_RETRIES = 3
    TIMEOUT = 30
    
    # Output
    OUTPUT_DIR = 'scraped_data'
    SAVE_HTML = True  # Save raw HTML for verification

# ========================================
# Target Sources (Public Health Sites)
# ========================================
SOURCES = {
    'fmoh': {
        'name': 'Federal Ministry of Health Nigeria',
        'base_url': 'https://www.health.gov.ng',
        'pages': [
            '/index.php/resources/publications',
            '/index.php/resources/guidelines',
            '/index.php/resources/health-facility-registry',
        ],
        'data_type': 'guidelines'
    },
    'nphcda': {
        'name': 'National Primary Health Care Development Agency',
        'base_url': 'https://nphcda.gov.ng',
        'pages': [
            '/resources/',
            '/publications/',
            '/guidelines/',
        ],
        'data_type': 'guidelines'
    },
    'ncdc': {
        'name': 'Nigeria Centre for Disease Control',
        'base_url': 'https://ncdc.gov.ng',
        'pages': [
            '/diseases/info',
            '/reports',
            '/publications',
        ],
        'data_type': 'disease_data'
    },
    'who_nigeria': {
        'name': 'WHO Nigeria',
        'base_url': 'https://www.afro.who.int/countries/nigeria',
        'pages': [
            '/publications',
            '/news',
        ],
        'data_type': 'reports'
    },
    'unicef_nigeria': {
        'name': 'UNICEF Nigeria',
        'base_url': 'https://www.unicef.org/nigeria',
        'pages': [
            '/reports',
            '/press-releases',
            '/what-we-do/health',
        ],
        'data_type': 'reports'
    }
}

# ========================================
# Utilities
# ========================================
def create_directories():
    """Create output directories"""
    dirs = [
        ScraperConfig.OUTPUT_DIR,
        f"{ScraperConfig.OUTPUT_DIR}/html",
        f"{ScraperConfig.OUTPUT_DIR}/json",
        f"{ScraperConfig.OUTPUT_DIR}/csv",
        f"{ScraperConfig.OUTPUT_DIR}/logs"
    ]
    for dir_path in dirs:
        os.makedirs(dir_path, exist_ok=True)

def log_message(message: str, level: str = 'INFO'):
    """Log with timestamps"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {level}: {message}"
    print(log_entry)
    
    with open(f"{ScraperConfig.OUTPUT_DIR}/logs/scraper.log", 'a', encoding='utf-8') as f:
        f.write(log_entry + '\n')

def check_robots_txt(base_url: str) -> bool:
    """Check if scraping is allowed by robots.txt"""
    try:
        rp = RobotFileParser()
        rp.set_url(urljoin(base_url, '/robots.txt'))
        rp.read()
        
        can_fetch = rp.can_fetch('*', base_url)
        log_message(f"robots.txt check for {base_url}: {'ALLOWED' if can_fetch else 'BLOCKED'}")
        return can_fetch
    except Exception as e:
        log_message(f"Could not read robots.txt for {base_url}: {e}", 'WARNING')
        return True  # Proceed cautiously if robots.txt unavailable

def sanitize_filename(text: str) -> str:
    """Create safe filename from text"""
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '_', text)
    return text[:200]  # Limit length

# ========================================
# HTTP Request Handler
# ========================================
class HTTPClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(ScraperConfig.HEADERS)
        self.request_count = 0
    
    def get(self, url: str) -> requests.Response:
        """Make GET request with retry logic"""
        for attempt in range(ScraperConfig.MAX_RETRIES):
            try:
                time.sleep(ScraperConfig.REQUEST_DELAY)
                
                response = self.session.get(
                    url, 
                    timeout=ScraperConfig.TIMEOUT,
                    allow_redirects=True
                )
                
                self.request_count += 1
                
                if response.status_code == 200:
                    log_message(f"✓ GET {url} [{response.status_code}]")
                    return response
                elif response.status_code == 429:  # Rate limited
                    wait_time = (attempt + 1) * 10
                    log_message(f"Rate limited. Waiting {wait_time}s...", 'WARNING')
                    time.sleep(wait_time)
                else:
                    log_message(f"✗ GET {url} [{response.status_code}]", 'WARNING')
                    
            except requests.exceptions.Timeout:
                log_message(f"Timeout on attempt {attempt + 1}/{ScraperConfig.MAX_RETRIES}", 'WARNING')
            except requests.exceptions.RequestException as e:
                log_message(f"Request error: {e}", 'ERROR')
        
        return None

# ========================================
# Content Extractors
# ========================================
class ContentExtractor:
    """Extract structured data from HTML"""
    
    @staticmethod
    def extract_documents(soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Extract PDF/document links"""
        documents = []
        
        # Find all PDF links
        pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$', re.IGNORECASE))
        
        for link in pdf_links:
            doc_url = urljoin(base_url, link.get('href'))
            title = link.get_text(strip=True) or link.get('title', 'Unknown')
            
            # Extract metadata from surrounding context
            parent = link.find_parent(['div', 'li', 'td', 'article'])
            context = parent.get_text(strip=True)[:500] if parent else ""
            
            documents.append({
                'title': title,
                'url': doc_url,
                'context': context,
                'type': 'pdf',
                'extracted_at': datetime.now().isoformat()
            })
        
        return documents
    
    @staticmethod
    def extract_articles(soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Extract article/news content"""
        articles = []
        
        # Common article containers
        article_selectors = [
            'article',
            '.post',
            '.news-item',
            '.publication',
            '[class*="article"]',
            '[class*="post"]'
        ]
        
        for selector in article_selectors:
            items = soup.select(selector)
            
            for item in items:
                # Extract title
                title_elem = item.find(['h1', 'h2', 'h3', 'h4'])
                title = title_elem.get_text(strip=True) if title_elem else "No title"
                
                # Extract link
                link_elem = item.find('a', href=True)
                url = urljoin(base_url, link_elem['href']) if link_elem else None
                
                # Extract date
                date_elem = item.find(['time', '.date', '[class*="date"]'])
                date = date_elem.get_text(strip=True) if date_elem else None
                
                # Extract summary/content
                content = item.get_text(strip=True)[:1000]
                
                if title and len(title) > 10:  # Filter out noise
                    articles.append({
                        'title': title,
                        'url': url,
                        'date': date,
                        'summary': content,
                        'extracted_at': datetime.now().isoformat()
                    })
        
        return articles
    
    @staticmethod
    def extract_health_data(soup: BeautifulSoup) -> List[Dict]:
        """Extract structured health data (tables, statistics)"""
        data = []
        
        # Find all tables
        tables = soup.find_all('table')
        
        for idx, table in enumerate(tables):
            try:
                # Convert table to pandas DataFrame
                df = pd.read_html(str(table))[0]
                
                # Get table caption/title
                caption = table.find('caption')
                title = caption.get_text(strip=True) if caption else f"Table {idx+1}"
                
                data.append({
                    'title': title,
                    'type': 'table',
                    'data': df.to_dict('records'),
                    'extracted_at': datetime.now().isoformat()
                })
            except Exception as e:
                log_message(f"Could not parse table {idx}: {e}", 'WARNING')
        
        return data
    
    @staticmethod
    def extract_contact_info(soup: BeautifulSoup) -> Dict:
        """Extract contact information"""
        contacts = {
            'emails': [],
            'phones': [],
            'addresses': []
        }
        
        text = soup.get_text()
        
        # Extract emails
        emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
        contacts['emails'] = list(set(emails))
        
        # Extract Nigerian phone numbers
        phones = re.findall(r'(?:\+234|0)[789]\d{9}', text)
        contacts['phones'] = list(set(phones))
        
        return contacts

# ========================================
# Main Scraper
# ========================================
class HealthDataScraper:
    def __init__(self):
        self.client = HTTPClient()
        self.extractor = ContentExtractor()
        self.visited_urls: Set[str] = set()
        self.all_data = {
            'documents': [],
            'articles': [],
            'health_data': [],
            'contacts': {}
        }
    
    def scrape_page(self, url: str, source_name: str) -> Dict:
        """Scrape a single page"""
        if url in self.visited_urls:
            log_message(f"Skipping already visited: {url}")
            return None
        
        self.visited_urls.add(url)
        
        response = self.client.get(url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Save raw HTML if configured
        if ScraperConfig.SAVE_HTML:
            filename = sanitize_filename(urlparse(url).path or source_name)
            html_path = f"{ScraperConfig.OUTPUT_DIR}/html/{filename}.html"
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(soup.prettify())
        
        # Extract different types of content
        page_data = {
            'url': url,
            'source': source_name,
            'scraped_at': datetime.now().isoformat(),
            'title': soup.title.string if soup.title else "No title",
            'documents': self.extractor.extract_documents(soup, url),
            'articles': self.extractor.extract_articles(soup, url),
            'health_data': self.extractor.extract_health_data(soup),
            'contacts': self.extractor.extract_contact_info(soup)
        }
        
        # Aggregate to main data store
        self.all_data['documents'].extend(page_data['documents'])
        self.all_data['articles'].extend(page_data['articles'])
        self.all_data['health_data'].extend(page_data['health_data'])
        
        log_message(f"Extracted: {len(page_data['documents'])} docs, {len(page_data['articles'])} articles, {len(page_data['health_data'])} tables")
        
        return page_data
    
    def scrape_source(self, source_key: str, source_info: Dict):
        """Scrape all pages from a source"""
        log_message(f"\n{'='*60}")
        log_message(f"Scraping: {source_info['name']}")
        log_message(f"{'='*60}")
        
        base_url = source_info['base_url']
        
        # Check robots.txt
        if not check_robots_txt(base_url):
            log_message(f"Skipping {source_info['name']} - blocked by robots.txt", 'WARNING')
            return
        
        # Scrape each page
        for page_path in source_info['pages']:
            full_url = urljoin(base_url, page_path)
            self.scrape_page(full_url, source_info['name'])
    
    def save_results(self):
        """Save all scraped data"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save as JSON
        json_path = f"{ScraperConfig.OUTPUT_DIR}/json/scraped_data_{timestamp}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(self.all_data, f, ensure_ascii=False, indent=2)
        log_message(f"✓ Saved JSON: {json_path}")
        
        # Save documents as CSV
        if self.all_data['documents']:
            df_docs = pd.DataFrame(self.all_data['documents'])
            csv_path = f"{ScraperConfig.OUTPUT_DIR}/csv/documents_{timestamp}.csv"
            df_docs.to_csv(csv_path, index=False, encoding='utf-8')
            log_message(f"✓ Saved documents CSV: {csv_path}")
        
        # Save articles as CSV
        if self.all_data['articles']:
            df_articles = pd.DataFrame(self.all_data['articles'])
            csv_path = f"{ScraperConfig.OUTPUT_DIR}/csv/articles_{timestamp}.csv"
            df_articles.to_csv(csv_path, index=False, encoding='utf-8')
            log_message(f"✓ Saved articles CSV: {csv_path}")
        
        # Generate summary report
        self.generate_summary()
    
    def generate_summary(self):
        """Generate scraping summary report"""
        summary = {
            'scrape_date': datetime.now().isoformat(),
            'total_urls_visited': len(self.visited_urls),
            'total_requests': self.client.request_count,
            'documents_found': len(self.all_data['documents']),
            'articles_found': len(self.all_data['articles']),
            'tables_found': len(self.all_data['health_data']),
            'unique_emails': len(set(
                email for contact in self.all_data.get('contacts', {}).values() 
                for email in contact.get('emails', [])
            )),
        }
        
        # Save summary
        summary_path = f"{ScraperConfig.OUTPUT_DIR}/summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        
        # Print summary
        log_message("\n" + "="*60)
        log_message("SCRAPING SUMMARY")
        log_message("="*60)
        for key, value in summary.items():
            log_message(f"{key.replace('_', ' ').title()}: {value}")
        log_message("="*60)

# ========================================
# Specialized Scrapers
# ========================================
def scrape_ncdc_disease_stats():
    """Specialized scraper for NCDC disease statistics"""
    log_message("\nRunning specialized NCDC disease scraper...")
    
    client = HTTPClient()
    url = "https://ncdc.gov.ng/diseases/sitreps"
    
    response = client.get(url)
    if not response:
        return []
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Look for situation reports
    reports = []
    for link in soup.find_all('a', href=True):
        if 'sitrep' in link['href'].lower() or '.pdf' in link['href'].lower():
            reports.append({
                'title': link.get_text(strip=True),
                'url': urljoin(url, link['href']),
                'type': 'disease_report'
            })
    
    log_message(f"Found {len(reports)} NCDC situation reports")
    return reports

def scrape_health_facility_registry():
    """Scrape Nigerian health facility registry data"""
    log_message("\nScraping health facility registry...")
    
    # Note: This is a hypothetical example - actual registry might be behind auth
    # Always respect data access policies
    
    facilities = []
    # Implementation would go here if registry is public
    
    return facilities

# ========================================
# Main Execution
# ========================================
def main():
    """Main scraping pipeline"""
    log_message("="*60)
    log_message("Nigerian Health Data Web Scraper")
    log_message("Purpose: Research & Analysis Only")
    log_message("="*60)
    
    create_directories()
    
    # Initialize scraper
    scraper = HealthDataScraper()
    
    # Scrape all sources
    for source_key, source_info in SOURCES.items():
        try:
            scraper.scrape_source(source_key, source_info)
        except Exception as e:
            log_message(f"Error scraping {source_info['name']}: {e}", 'ERROR')
            continue
    
    # Run specialized scrapers
    try:
        ncdc_reports = scrape_ncdc_disease_stats()
        scraper.all_data['documents'].extend(ncdc_reports)
    except Exception as e:
        log_message(f"Error in specialized scraper: {e}", 'ERROR')
    
    # Save all results
    scraper.save_results()
    
    log_message("\n✓ Scraping completed successfully!")
    log_message(f"Results saved in: {ScraperConfig.OUTPUT_DIR}/")

if __name__ == "__main__":
    main()
