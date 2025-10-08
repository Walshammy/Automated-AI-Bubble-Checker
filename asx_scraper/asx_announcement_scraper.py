"""
Main ASX Announcement Scraper
Updated to use Selenium for JavaScript form handling
Based on the successful comprehensive_nzx_scraper.py approach
"""
import requests
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from tqdm import tqdm
import pandas as pd
import json
import re

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

import asx_config as config
from asx_database import ASXDatabase

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler()
    ]
)

class ASXAnnouncementScraper:
    """ASX announcement scraper using official JSON API - adapted from successful NZX approach"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = ASXDatabase()
        
        # Session setup with retry strategy (from NZX approach)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        
        # Rate limiting (from NZX approach)
        self.base_delay = config.RATE_LIMIT_DELAY
        self.max_delay = 10.0
        self.current_delay = self.base_delay
        self.consecutive_errors = 0
        
        self.logger.info("ASX Announcement Scraper initialized (API version)")
    
    def is_financial_report(self, title: str) -> bool:
        """Check if announcement is a financial report - enhanced from NZX approach"""
        title_lower = title.lower()
        return any(keyword in title_lower for keyword in config.FINANCIAL_KEYWORDS)
    
    def fetch_announcements(self, ticker: str, count: int = config.DEFAULT_COUNT) -> Optional[List[Dict]]:
        """
        Fetch announcements for a specific ticker using Selenium
        
        Args:
            ticker: ASX ticker code (e.g., 'CBA', 'BHP')
            count: Number of announcements to fetch (not used in Selenium approach)
        
        Returns:
            List of announcement dictionaries or None if error
        """
        driver = None
        
        for attempt in range(config.MAX_RETRIES):
            try:
                self.logger.debug(f"Fetching announcements for {ticker} (attempt {attempt + 1})")
                
                # Setup Chrome driver with headless mode
                chrome_options = Options()
                chrome_options.add_argument('--headless')  # Run without GUI
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--disable-gpu')
                chrome_options.add_argument('--window-size=1920,1080')
                chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
                
                driver = webdriver.Chrome(
                    service=Service(ChromeDriverManager().install()),
                    options=chrome_options
                )
                
                # Navigate to ASX announcements page with ticker parameter
                url = f"{config.ASX_ANNOUNCEMENTS_URL}?by=asxCode&asxCode={ticker.upper()}&timeframe=Y&period=Y3"
                driver.get(url)
                
                # Wait for page to load
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Wait a bit more for JavaScript to execute
                time.sleep(2)
                
                # Parse the page HTML (now with JavaScript executed)
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                announcements = self.parse_announcements_table(soup, ticker)
                
                self.logger.debug(f"Found {len(announcements)} announcements for {ticker}")
                return announcements
                    
            except (TimeoutException, NoSuchElementException) as e:
                if attempt < config.MAX_RETRIES - 1:
                    wait_time = 2 ** attempt
                    self.logger.warning(f"Selenium timeout for {ticker} (attempt {attempt + 1}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Failed to fetch {ticker} after {config.MAX_RETRIES} attempts: {e}")
                    return None
            except Exception as e:
                if attempt < config.MAX_RETRIES - 1:
                    wait_time = 2 ** attempt
                    self.logger.warning(f"Unexpected error for {ticker} (attempt {attempt + 1}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Failed to fetch {ticker} after {config.MAX_RETRIES} attempts: {e}")
                    return None
            finally:
                if driver:
                    driver.quit()
        
        return None
    
    def parse_announcements_table(self, soup: BeautifulSoup, ticker: str) -> List[Dict]:
        """
        Parse announcements from ASX HTML table
        
        Args:
            soup: BeautifulSoup object of the page
            ticker: Company ticker
        
        Returns:
            List of announcement dictionaries
        """
        announcements = []
        
        try:
            # Look for announcement tables - ASX uses different structures
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                # Skip tables with too few rows (likely navigation/forms)
                if len(rows) < 5:
                    continue
                
                # Look for rows with multiple cells (potential announcements)
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 3:
                        continue
                    
                    try:
                        # Extract data from cells
                        date_cell = cells[0] if len(cells) > 0 else None
                        title_cell = cells[1] if len(cells) > 1 else None
                        link_cell = cells[2] if len(cells) > 2 else None
                        
                        if not title_cell:
                            continue
                        
                        title = title_cell.get_text(strip=True)
                        if not title or len(title) < 5:  # Skip empty or very short titles
                            continue
                        
                        # Skip header rows
                        if title.lower() in ['date', 'title', 'document', 'announcement']:
                            continue
                        
                        # Extract date
                        announcement_date = None
                        if date_cell:
                            date_text = date_cell.get_text(strip=True)
                            announcement_date = self.parse_date(date_text)
                        
                        # Extract PDF URL
                        pdf_url = None
                        pdf_filename = None
                        if link_cell:
                            link = link_cell.find('a', href=True)
                            if link:
                                pdf_url = link['href']
                                if not pdf_url.startswith('http'):
                                    pdf_url = 'https://www.asx.com.au' + pdf_url
                                pdf_filename = pdf_url.split('/')[-1]
                        
                        # Generate announcement ID
                        announcement_id = f"{ticker}_{hash(title) % 1000000}"
                        
                        announcement = {
                            'announcement_id': announcement_id,
                            'ticker': ticker.upper(),
                            'company_name': None,
                            'announcement_date': announcement_date,
                            'title': title,
                            'url': pdf_url or '',
                            'file_size': None,
                            'market_sensitive': False,
                            'is_financial_report': self.is_financial_report(title),
                            'pdf_filename': pdf_filename
                        }
                        
                        announcements.append(announcement)
                        
                    except Exception as e:
                        self.logger.debug(f"Error parsing announcement row for {ticker}: {e}")
                        continue
                        
        except Exception as e:
            self.logger.error(f"Error parsing announcements table for {ticker}: {e}")
            
        return announcements
    
    def parse_date(self, date_text: str) -> Optional[datetime]:
        """Parse date text into datetime object"""
        try:
            # Handle various date formats from ASX
            date_patterns = [
                '%d/%m/%Y',      # DD/MM/YYYY
                '%d-%m-%Y',      # DD-MM-YYYY
                '%Y-%m-%d',      # YYYY-MM-DD
                '%d %b %Y',      # DD Mon YYYY
                '%d %B %Y',      # DD Month YYYY
                '%b %d, %Y',     # Mon DD, YYYY
                '%B %d, %Y',     # Month DD, YYYY
            ]
            
            for pattern in date_patterns:
                try:
                    return datetime.strptime(date_text.strip(), pattern)
                except ValueError:
                    continue
            
            # Try to extract date from text using regex
            date_match = re.search(r'(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})', date_text)
            if date_match:
                day, month, year = date_match.groups()
                return datetime(int(year), int(month), int(day))
                
        except Exception as e:
            self.logger.debug(f"Could not parse date '{date_text}': {e}")
            
        return None
    
    def scrape_ticker(self, ticker: str, company_name: str = None) -> int:
        """Scrape all announcements for a single ticker"""
        self.logger.info(f"Scraping {ticker}...")
        
        # Fetch from API (returns list of dicts)
        raw_announcements = self.fetch_announcements(ticker)
        
        if not raw_announcements:
            self.logger.warning(f"No data found for {ticker}")
            return 0
        
        # Filter by date (all time if YEARS_TO_SCRAPE is None)
        cutoff_date = None
        if config.YEARS_TO_SCRAPE is not None:
            cutoff_date = datetime.now() - timedelta(days=365 * config.YEARS_TO_SCRAPE)
        
        new_count = 0
        financial_count = 0
        
        # Process each announcement from HTML parsing
        for announcement in raw_announcements:
            
            # Skip if too old (only if cutoff_date is set)
            if cutoff_date and announcement['announcement_date'] and announcement['announcement_date'] < cutoff_date:
                continue
            
            # Insert into database
            if self.db.insert_announcement(announcement):
                new_count += 1
                if announcement['is_financial_report']:
                    financial_count += 1
        
        self.logger.info(f"✅ {ticker}: {new_count} new announcements ({financial_count} financial)")
        time.sleep(self.current_delay)
        
        return new_count
    
    def scrape_beta_tickers(self) -> int:
        """Scrape beta test tickers"""
        self.logger.info("Starting beta test with major ASX companies...")
        
        total_new = 0
        total_financial = 0
        
        for ticker in config.BETA_TICKERS:
            try:
                new_count = self.scrape_ticker(ticker)
                total_new += new_count
                
                # Get financial count for this ticker
                financial_count = self.db.get_financial_count(ticker)
                total_financial += financial_count
                
            except Exception as e:
                self.logger.error(f"Error scraping {ticker}: {e}")
                continue
        
        self.logger.info(f"🎯 Beta test complete: {total_new} total announcements ({total_financial} financial)")
        return total_new
    
    def scrape_all_tickers(self) -> int:
        """Scrape all ASX tickers from stock list"""
        self.logger.info("Starting full ASX scraping...")
        
        # Load stock list
        try:
            df = pd.read_csv(config.STOCK_LIST_PATH)
            tickers = df['Ticker'].tolist()
            self.logger.info(f"Loaded {len(tickers)} tickers from stock list")
        except Exception as e:
            self.logger.error(f"Error loading stock list: {e}")
            return 0
        
        total_new = 0
        total_financial = 0
        
        # Progress bar
        with tqdm(total=len(tickers), desc="Scraping ASX tickers") as pbar:
            for ticker in tickers:
                try:
                    new_count = self.scrape_ticker(ticker)
                    total_new += new_count
                    
                    # Get financial count for this ticker
                    financial_count = self.db.get_financial_count(ticker)
                    total_financial += financial_count
                    
                    pbar.set_postfix({
                        'New': total_new,
                        'Financial': total_financial,
                        'Current': ticker
                    })
                    
                except Exception as e:
                    self.logger.error(f"Error scraping {ticker}: {e}")
                    continue
                
                pbar.update(1)
        
        self.logger.info(f"🎯 Full scraping complete: {total_new} total announcements ({total_financial} financial)")
        return total_new
    
    def handle_rate_limiting(self):
        """Handle rate limiting with exponential backoff"""
        self.consecutive_errors += 1
        self.current_delay = min(self.base_delay * (2 ** self.consecutive_errors), self.max_delay)
        self.logger.warning(f"Rate limiting: waiting {self.current_delay}s")
        time.sleep(self.current_delay)
    
    def get_statistics(self) -> Dict:
        """Get scraping statistics"""
        return self.db.get_statistics()

def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ASX Announcement Scraper (API Version)')
    parser.add_argument('--beta', action='store_true', help='Run beta test with major companies')
    parser.add_argument('--ticker', '-t', type=str, help='Scrape specific ticker')
    parser.add_argument('--years', '-y', type=int, default=config.YEARS_TO_SCRAPE,
                        help=f'Number of years of data to retrieve (default: {config.YEARS_TO_SCRAPE or "all time"})')
    
    args = parser.parse_args()
    
    # Update config if years specified
    if args.years != config.YEARS_TO_SCRAPE:
        config.YEARS_TO_SCRAPE = args.years
        years_text = "All time" if config.YEARS_TO_SCRAPE is None else f"{config.YEARS_TO_SCRAPE} years"
        print(f"Years to scrape: {years_text}")
    
    scraper = ASXAnnouncementScraper()
    
    if args.ticker:
        # Scrape specific ticker
        scraper.scrape_ticker(args.ticker)
    elif args.beta:
        # Run beta test
        scraper.scrape_beta_tickers()
    else:
        # Run full scraping
        scraper.scrape_all_tickers()
    
    # Show final statistics
    stats = scraper.get_statistics()
    print("\nFinal Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

if __name__ == "__main__":
    main()