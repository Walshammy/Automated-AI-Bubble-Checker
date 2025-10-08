"""
Main ASX Announcement Scraper
Updated to use Official ASX JSON API instead of web scraping
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
        Fetch announcements for a specific ticker from ASX API
        
        Args:
            ticker: ASX ticker code (e.g., 'CBA', 'BHP')
            count: Number of announcements to fetch
        
        Returns:
            List of announcement dictionaries or None if error
        """
        url = config.ASX_API_BASE.format(ticker=ticker.upper())
        params = {'count': count}
        
        for attempt in range(config.MAX_RETRIES):
            try:
                self.logger.debug(f"Fetching announcements for {ticker} (attempt {attempt + 1})")
                
                response = self.session.get(
                    url, 
                    params=params, 
                    timeout=config.REQUEST_TIMEOUT
                )
                response.raise_for_status()
                
                # Parse JSON response (NOT HTML!)
                data = response.json()
                
                if 'data' in data:
                    self.logger.debug(f"Found {len(data['data'])} announcements for {ticker}")
                    return data['data']
                else:
                    self.logger.warning(f"Unexpected response format for {ticker}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                if attempt < config.MAX_RETRIES - 1:
                    wait_time = 2 ** attempt
                    self.logger.warning(f"Request failed for {ticker}: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Failed to fetch {ticker} after {config.MAX_RETRIES} attempts: {e}")
                    return None
        
        return None
    
    def parse_announcement(self, raw_ann: Dict, ticker: str) -> Dict:
        """
        Parse raw announcement data from API into standardized format
        
        Args:
            raw_ann: Raw announcement dict from API
            ticker: Company ticker
        
        Returns:
            Parsed announcement dictionary
        """
        # Parse date from API format
        date_str = raw_ann.get('document_release_date', '')
        try:
            # Remove timezone info for simplicity
            date_str_clean = date_str.replace('+1000', '').replace('+1100', '')
            announcement_date = datetime.fromisoformat(date_str_clean)
        except:
            announcement_date = None
        
        title = raw_ann.get('header', '')
        url = raw_ann.get('url', '')
        
        # Generate PDF filename from URL
        pdf_filename = None
        if url:
            pdf_filename = url.split('/')[-1]
        
        # Generate unique announcement ID
        announcement_id = f"{ticker}_{pdf_filename}" if pdf_filename else f"{ticker}_{hash(title) % 1000000}"
        
        return {
            'announcement_id': announcement_id,
            'ticker': ticker.upper(),
            'company_name': None,
            'announcement_date': announcement_date,
            'title': title,
            'url': url,
            'file_size': raw_ann.get('size'),
            'market_sensitive': raw_ann.get('market_sensitive', False),
            'is_financial_report': self.is_financial_report(title),
            'pdf_filename': pdf_filename
        }
    
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
        
        # Parse each raw announcement from API
        for raw_ann in raw_announcements:
            # Parse the raw API data
            announcement = self.parse_announcement(raw_ann, ticker)
            
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
    print("\n📊 Final Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

if __name__ == "__main__":
    main()