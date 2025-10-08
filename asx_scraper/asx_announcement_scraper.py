"""
Main ASX Announcement Scraper
Beta version - Fetches financial announcements from ASX API
Based on the successful comprehensive_nzx_scraper.py approach
"""
import requests
from bs4 import BeautifulSoup
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from tqdm import tqdm
import pandas as pd
import json
import re

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
    """ASX announcement scraper - adapted from successful NZX approach"""
    
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
        
        self.logger.info("ASX Announcement Scraper initialized")
    
    def is_financial_report(self, title: str) -> bool:
        """Check if announcement is a financial report - enhanced from NZX approach"""
        title_lower = title.lower()
        return any(keyword in title_lower for keyword in config.FINANCIAL_KEYWORDS)
    
    def fetch_announcements(self, ticker: str, count: int = config.DEFAULT_COUNT) -> Optional[List[Dict]]:
        """
        Fetch announcements for a specific ticker from ASX website
        
        Args:
            ticker: ASX ticker code (e.g., 'CBA', 'BHP')
            count: Number of announcements to fetch (not used in web scraping)
        
        Returns:
            List of announcement dictionaries or None if error
        """
        url = config.ASX_ANNOUNCEMENTS_URL.format(ticker=ticker.upper())
        
        for attempt in range(config.MAX_RETRIES):
            try:
                self.logger.debug(f"Fetching announcements for {ticker} (attempt {attempt + 1})")
                
                response = self.session.get(url, timeout=config.REQUEST_TIMEOUT)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                announcements = self.parse_announcements_page(soup, ticker)
                
                self.logger.debug(f"Found {len(announcements)} announcements for {ticker}")
                return announcements
                    
            except requests.exceptions.RequestException as e:
                if attempt < config.MAX_RETRIES - 1:
                    wait_time = 2 ** attempt
                    self.logger.warning(f"Request failed for {ticker} (attempt {attempt + 1}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Failed to fetch {ticker} after {config.MAX_RETRIES} attempts: {e}")
                    self.handle_rate_limiting()
                    return None
        
        return None
    
    def parse_announcements_page(self, soup: BeautifulSoup, ticker: str) -> List[Dict]:
        """Parse announcements from ASX webpage"""
        announcements = []
        
        try:
            # Look for announcement rows in the table
            # ASX uses different table structures, try multiple selectors
            table_selectors = [
                'table tr',
                '.announcement-row',
                'tr[data-announcement]',
                'tbody tr'
            ]
            
            rows = []
            for selector in table_selectors:
                rows = soup.select(selector)
                if rows:
                    self.logger.debug(f"Found {len(rows)} rows using selector: {selector}")
                    break
            
            if not rows:
                self.logger.warning(f"No announcement rows found for {ticker}")
                return announcements
            
            for row in rows:
                try:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 2:
                        continue
                    
                    # Extract data from cells
                    date_cell = cells[0] if len(cells) > 0 else None
                    title_cell = cells[1] if len(cells) > 1 else None
                    link_cell = cells[2] if len(cells) > 2 else None
                    
                    if not title_cell:
                        continue
                    
                    title = title_cell.get_text(strip=True)
                    if not title:
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
                    self.logger.warning(f"Error parsing announcement row for {ticker}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error parsing announcements page for {ticker}: {e}")
            
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
            ]
            
            for pattern in date_patterns:
                try:
                    return datetime.strptime(date_text.strip(), pattern)
                except ValueError:
                    continue
            
            # Try to extract date from text
            date_match = re.search(r'(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})', date_text)
            if date_match:
                day, month, year = date_match.groups()
                return datetime(int(year), int(month), int(day))
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error parsing date '{date_text}': {e}")
            return None
    
    def scrape_ticker(self, ticker: str, company_name: str = None) -> int:
        """
        Scrape all announcements for a single ticker
        
        Returns:
            Number of new announcements added
        """
        self.logger.info(f"Scraping {ticker}...")
        
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
        
        # Rate limiting
        time.sleep(self.current_delay)
        
        return new_count
    
    def scrape_beta_tickers(self):
        """Scrape beta test tickers"""
        self.logger.info("=" * 60)
        self.logger.info("ASX ANNOUNCEMENT SCRAPER - BETA TEST")
        self.logger.info("=" * 60)
        self.logger.info(f"Beta tickers: {', '.join(config.BETA_TICKERS)}")
        years_text = "All time" if config.YEARS_TO_SCRAPE is None else f"{config.YEARS_TO_SCRAPE} years"
        self.logger.info(f"Years to scrape: {years_text}")
        self.logger.info("=" * 60)
        
        start_time = datetime.now()
        total_new = 0
        successful_companies = 0
        failed_companies = []
        
        for i, ticker in enumerate(config.BETA_TICKERS, 1):
            self.logger.info(f"[{i}/{len(config.BETA_TICKERS)}] Processing {ticker}...")
            
            try:
                count = self.scrape_ticker(ticker)
                total_new += count
                successful_companies += 1
                
                # Progress update
                if i % 3 == 0 or i == len(config.BETA_TICKERS):
                    elapsed = datetime.now() - start_time
                    avg_time = elapsed.total_seconds() / i
                    remaining = len(config.BETA_TICKERS) - i
                    eta = remaining * avg_time
                    
                    self.logger.info(f"Progress: {i}/{len(config.BETA_TICKERS)} companies")
                    self.logger.info(f"Successful: {successful_companies}, Failed: {len(failed_companies)}")
                    self.logger.info(f"Total announcements: {total_new}")
                    self.logger.info(f"ETA: {eta/60:.1f} minutes")
                
                # Reset error counter on success
                self.consecutive_errors = 0
                self.current_delay = self.base_delay
                
            except Exception as e:
                failed_companies.append(ticker)
                self.logger.error(f"Error processing {ticker}: {str(e)}")
                self.handle_rate_limiting()
                continue
        
        # Final summary
        end_time = datetime.now()
        total_time = end_time - start_time
        
        self.logger.info("=" * 60)
        self.logger.info("BETA SCRAPING COMPLETE!")
        self.logger.info("=" * 60)
        self.logger.info(f"Total companies processed: {len(config.BETA_TICKERS)}")
        self.logger.info(f"Successful companies: {successful_companies}")
        self.logger.info(f"Failed companies: {len(failed_companies)}")
        self.logger.info(f"Total announcements collected: {total_new}")
        self.logger.info(f"Total time: {total_time}")
        self.logger.info(f"Average time per company: {total_time.total_seconds()/len(config.BETA_TICKERS):.1f} seconds")
        
        if failed_companies:
            self.logger.info(f"Failed companies: {', '.join(failed_companies)}")
        
        # Print database statistics
        stats = self.db.get_statistics()
        self.logger.info("\nDatabase Statistics:")
        for key, value in stats.items():
            self.logger.info(f"  {key}: {value}")
        
        return total_new
    
    def scrape_all_tickers(self, ticker_list: List[str] = None):
        """
        Scrape all tickers from list
        
        Args:
            ticker_list: List of tickers, or None to load from CSV
        """
        # Load ticker list
        if ticker_list is None:
            try:
                df = pd.read_csv(config.STOCK_LIST_PATH)
                # Assuming CSV has columns: 'ticker' or 'code'
                if 'ticker' in df.columns:
                    ticker_list = df['ticker'].tolist()
                elif 'code' in df.columns:
                    ticker_list = df['code'].tolist()
                else:
                    ticker_list = df.iloc[:, 0].tolist()  # First column
            except FileNotFoundError:
                self.logger.error(f"Stock list not found: {config.STOCK_LIST_PATH}")
                return
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"ASX ANNOUNCEMENT SCRAPER")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Total companies: {len(ticker_list)}")
        years_text = "All time" if config.YEARS_TO_SCRAPE is None else f"{config.YEARS_TO_SCRAPE} years"
        self.logger.info(f"Years to scrape: {years_text}")
        self.logger.info(f"{'='*60}\n")
        
        total_new = 0
        
        for ticker in tqdm(ticker_list, desc="Scraping tickers"):
            count = self.scrape_ticker(ticker)
            total_new += count
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"SCRAPING COMPLETE")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Total new announcements: {total_new}")
        
        # Print statistics
        stats = self.db.get_statistics()
        self.logger.info(f"\nDatabase Statistics:")
        for key, value in stats.items():
            self.logger.info(f"  {key}: {value}")
    
    def handle_rate_limiting(self):
        """Handle rate limiting by increasing delay - from NZX approach"""
        self.consecutive_errors += 1
        self.current_delay = min(self.base_delay * (2 ** self.consecutive_errors), self.max_delay)
        self.logger.warning(f"Rate limiting detected. Increasing delay to {self.current_delay}s")
        time.sleep(self.current_delay)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ASX Announcement Scraper - Beta Version')
    parser.add_argument('--beta', action='store_true', default=True,
                        help='Run beta test with predefined tickers (default)')
    parser.add_argument('--tickers', nargs='+', default=None,
                        help='Specific tickers to scrape')
    parser.add_argument('--years', '-y', type=int, default=config.YEARS_TO_SCRAPE,
                        help=f'Number of years of data to retrieve (default: {config.YEARS_TO_SCRAPE or "all time"})')
    
    args = parser.parse_args()
    
    scraper = ASXAnnouncementScraper()
    
    if args.beta:
        # Run beta test
        scraper.scrape_beta_tickers()
    elif args.tickers:
        # Scrape specific tickers
        for ticker in args.tickers:
            scraper.scrape_ticker(ticker)
    else:
        # Scrape all tickers from CSV
        scraper.scrape_all_tickers()


if __name__ == "__main__":
    main()
