#!/usr/bin/env python3
"""
COMPREHENSIVE NZX FINANCIAL ANNOUNCEMENT SCRAPER
===============================================

This scraper captures financial announcements for ALL NZX companies.
It addresses the critical issue where only 46 out of 177 NZX companies
were being captured.

Features:
- Comprehensive company discovery from NZX_ASX.xlsx
- Multi-year historical data collection
- Robust error handling and retry logic
- Progress tracking and resumption capability
- Database integration with existing schema

Author: AI Assistant
Date: 2025-10-06
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import logging
import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import List, Dict, Any, Optional
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('comprehensive_nzx_scraper.log'),
        logging.StreamHandler()
    ]
)

class ComprehensiveNZXScraper:
    """Comprehensive NZX financial announcement scraper"""
    
    def __init__(self, db_path: str = None):
        self.logger = logging.getLogger(__name__)
        
        # Database setup
        if db_path is None:
            current_dir = Path(__file__).parent
            self.db_path = current_dir.parent / "data_collection" / "unified_stock_data.db"
        else:
            self.db_path = Path(db_path)
        
        self.init_database()
        
        # Session setup with retry strategy
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        
        # Rate limiting
        self.base_delay = 1.0
        self.max_delay = 10.0
        self.current_delay = self.base_delay
        self.consecutive_errors = 0
        
        # Load all NZX companies
        self.nzx_companies = self.load_nzx_companies()
        self.logger.info(f"Loaded {len(self.nzx_companies)} NZX companies")
        
    def init_database(self):
        """Initialize database connection and tables"""
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        # Create financial_announcements table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS financial_announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                announcement_id TEXT UNIQUE,
                ticker TEXT NOT NULL,
                title TEXT NOT NULL,
                announcement_url TEXT,
                announcement_date TEXT NOT NULL,
                announcement_type TEXT,
                exchange TEXT DEFAULT 'NZX',
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed BOOLEAN DEFAULT FALSE
            )
        """)
        
        self.conn.commit()
        self.logger.info("Database initialized")
    
    def load_nzx_companies(self) -> List[str]:
        """Load all NZX companies from the Excel file"""
        try:
            excel_path = Path(__file__).parent.parent / "data_collection" / "NZX_ASX.xlsx"
            df = pd.read_excel(excel_path)
            
            # Get all unique tickers from the Code column
            companies = df['Code'].dropna().astype(str).tolist()
            
            # Filter out invalid codes
            valid_companies = []
            for company in companies:
                if len(company.strip()) >= 2 and company.strip().isalpha():
                    valid_companies.append(company.strip())
            
            return sorted(list(set(valid_companies)))
            
        except Exception as e:
            self.logger.error(f"Error loading NZX companies: {e}")
            return []
    
    def get_company_announcements(self, ticker: str, years: int = 3) -> List[Dict[str, Any]]:
        """Get financial announcements for a specific company"""
        announcements = []
        
        try:
            # Try multiple years
            for year_offset in range(years):
                year = datetime.now().year - year_offset
                
                # Construct URL for company announcements
                url = f"https://www.nzx.com/companies/{ticker}/announcements?year={year}"
                
                self.logger.info(f"Scraping {ticker} announcements for {year}")
                
                try:
                    response = self.session.get(url, timeout=10)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Parse announcements from the page
                    year_announcements = self.parse_announcements_page(soup, ticker, year)
                    announcements.extend(year_announcements)
                    
                    # Rate limiting
                    time.sleep(self.current_delay)
                    
                except requests.RequestException as e:
                    self.logger.warning(f"Error scraping {ticker} for {year}: {e}")
                    self.handle_rate_limiting()
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error getting announcements for {ticker}: {e}")
            
        return announcements
    
    def parse_announcements_page(self, soup: BeautifulSoup, ticker: str, year: int) -> List[Dict[str, Any]]:
        """Parse announcements from a company's announcement page"""
        announcements = []
        
        try:
            # Look for announcement rows in the table
            # The NZX website uses React, so we need to look for specific patterns
            
            # Try to find announcement links
            announcement_links = soup.find_all('a', href=re.compile(r'/announcements/\d+'))
            
            for link in announcement_links:
                try:
                    announcement_id = re.search(r'/announcements/(\d+)', link.get('href', ''))
                    if not announcement_id:
                        continue
                    
                    announcement_id = announcement_id.group(1)
                    title = link.get_text(strip=True)
                    
                    if not title:
                        continue
                    
                    # Find the parent row to get date and type
                    row = link.find_parent('tr')
                    if not row:
                        continue
                    
                    # Extract date and type from the row
                    cells = row.find_all('td')
                    if len(cells) < 3:
                        continue
                    
                    date_cell = cells[1] if len(cells) > 1 else None
                    type_cell = cells[2] if len(cells) > 2 else None
                    
                    announcement_date = None
                    announcement_type = None
                    
                    if date_cell:
                        date_text = date_cell.get_text(strip=True)
                        announcement_date = self.parse_date(date_text)
                    
                    if type_cell:
                        announcement_type = type_cell.get_text(strip=True)
                    
                    # Only include financial announcements
                    if self.is_financial_announcement(title, announcement_type):
                        announcement = {
                            'announcement_id': announcement_id,
                            'ticker': ticker,
                            'title': title,
                            'announcement_url': f"https://www.nzx.com/announcements/{announcement_id}",
                            'announcement_date': announcement_date or f"{year}-01-01",
                            'announcement_type': announcement_type or 'GENERAL',
                            'exchange': 'NZX'
                        }
                        announcements.append(announcement)
                        
                except Exception as e:
                    self.logger.warning(f"Error parsing announcement for {ticker}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error parsing announcements page for {ticker}: {e}")
            
        return announcements
    
    def parse_date(self, date_text: str) -> Optional[str]:
        """Parse date text into ISO format"""
        try:
            # Handle various date formats from NZX
            date_patterns = [
                r'(\d{1,2})/(\d{1,2})/(\d{4})',  # DD/MM/YYYY
                r'(\d{1,2})-(\d{1,2})-(\d{4})',  # DD-MM-YYYY
                r'(\d{4})-(\d{1,2})-(\d{1,2})',  # YYYY-MM-DD
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, date_text)
                if match:
                    groups = match.groups()
                    if len(groups) == 3:
                        # Try different interpretations
                        try:
                            if len(groups[0]) == 4:  # YYYY-MM-DD
                                return f"{groups[0]}-{groups[1].zfill(2)}-{groups[2].zfill(2)}"
                            else:  # DD/MM/YYYY or DD-MM-YYYY
                                return f"{groups[2]}-{groups[1].zfill(2)}-{groups[0].zfill(2)}"
                        except:
                            continue
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error parsing date '{date_text}': {e}")
            return None
    
    def is_financial_announcement(self, title: str, announcement_type: str) -> bool:
        """Check if an announcement is financial in nature"""
        financial_keywords = [
            'results', 'financial', 'annual', 'interim', 'quarterly', 'earnings',
            'profit', 'revenue', 'balance sheet', 'cash flow', 'dividend',
            'half year', 'full year', 'preliminary', 'audited', 'unaudited',
            'statement', 'report', 'performance', 'outlook', 'guidance'
        ]
        
        title_lower = title.lower()
        type_lower = (announcement_type or '').lower()
        
        # Check title for financial keywords
        for keyword in financial_keywords:
            if keyword in title_lower:
                return True
        
        # Check announcement type
        financial_types = ['FLLYR', 'HALFYR', 'ANNREP', 'GENERAL', 'MKTUPDTE']
        if type_lower in [t.lower() for t in financial_types]:
            return True
        
        return False
    
    def handle_rate_limiting(self):
        """Handle rate limiting by increasing delay"""
        self.consecutive_errors += 1
        self.current_delay = min(self.base_delay * (2 ** self.consecutive_errors), self.max_delay)
        self.logger.warning(f"Rate limiting detected. Increasing delay to {self.current_delay}s")
        time.sleep(self.current_delay)
    
    def save_announcements(self, announcements: List[Dict[str, Any]]) -> int:
        """Save announcements to database"""
        if not announcements:
            return 0
        
        cursor = self.conn.cursor()
        saved_count = 0
        
        for announcement in announcements:
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO financial_announcements 
                    (announcement_id, ticker, title, announcement_url, announcement_date, 
                     announcement_type, exchange, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    announcement['announcement_id'],
                    announcement['ticker'],
                    announcement['title'],
                    announcement['announcement_url'],
                    announcement['announcement_date'],
                    announcement['announcement_type'],
                    announcement['exchange'],
                    datetime.now().isoformat()
                ))
                saved_count += 1
                
            except sqlite3.IntegrityError:
                # Announcement already exists, skip
                continue
            except Exception as e:
                self.logger.error(f"Error saving announcement {announcement['announcement_id']}: {e}")
                continue
        
        self.conn.commit()
        return saved_count
    
    def run_comprehensive_scraping(self, years: int = 3, resume_from: str = None):
        """Run comprehensive scraping for all NZX companies"""
        self.logger.info("=" * 60)
        self.logger.info("COMPREHENSIVE NZX FINANCIAL ANNOUNCEMENT SCRAPER")
        self.logger.info("=" * 60)
        
        start_time = datetime.now()
        total_announcements = 0
        successful_companies = 0
        failed_companies = []
        
        # Determine starting point if resuming
        start_index = 0
        if resume_from:
            try:
                start_index = self.nzx_companies.index(resume_from)
                self.logger.info(f"Resuming from {resume_from} (index {start_index})")
            except ValueError:
                self.logger.warning(f"Resume ticker {resume_from} not found, starting from beginning")
        
        for i, ticker in enumerate(self.nzx_companies[start_index:], start_index + 1):
            self.logger.info(f"[{i}/{len(self.nzx_companies)}] Processing {ticker}...")
            
            try:
                # Get announcements for this company
                announcements = self.get_company_announcements(ticker, years)
                
                if announcements:
                    # Save to database
                    saved_count = self.save_announcements(announcements)
                    total_announcements += saved_count
                    successful_companies += 1
                    
                    self.logger.info(f"  [OK] {ticker}: {len(announcements)} announcements, {saved_count} saved")
                else:
                    self.logger.warning(f"  [WARN] {ticker}: No announcements found")
                
                # Progress update every 10 companies
                if i % 10 == 0:
                    elapsed = datetime.now() - start_time
                    avg_time = elapsed.total_seconds() / i
                    remaining = len(self.nzx_companies) - i
                    eta = remaining * avg_time
                    
                    self.logger.info(f"  Progress: {i}/{len(self.nzx_companies)} companies")
                    self.logger.info(f"  Successful: {successful_companies}, Failed: {len(failed_companies)}")
                    self.logger.info(f"  Total announcements: {total_announcements}")
                    self.logger.info(f"  ETA: {eta/60:.1f} minutes")
                
                # Reset error counter on success
                self.consecutive_errors = 0
                self.current_delay = self.base_delay
                
            except Exception as e:
                failed_companies.append(ticker)
                self.logger.error(f"  [ERROR] {ticker}: Error - {str(e)}")
                self.handle_rate_limiting()
                continue
        
        # Final summary
        end_time = datetime.now()
        total_time = end_time - start_time
        
        self.logger.info("=" * 60)
        self.logger.info("SCRAPING COMPLETE!")
        self.logger.info("=" * 60)
        self.logger.info(f"Total companies processed: {len(self.nzx_companies)}")
        self.logger.info(f"Successful companies: {successful_companies}")
        self.logger.info(f"Failed companies: {len(failed_companies)}")
        self.logger.info(f"Total announcements collected: {total_announcements}")
        self.logger.info(f"Total time: {total_time}")
        self.logger.info(f"Average time per company: {total_time.total_seconds()/len(self.nzx_companies):.1f} seconds")
        
        if failed_companies:
            self.logger.info(f"Failed companies: {', '.join(failed_companies)}")
        
        return total_announcements

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Comprehensive NZX Financial Announcement Scraper')
    parser.add_argument('--years', '-y', type=int, default=3,
                        help='Number of years of data to retrieve (default: 3)')
    parser.add_argument('--resume-from', '-r', type=str, default=None,
                        help='Resume scraping from this ticker')
    parser.add_argument('--db-path', type=str, default=None,
                        help='Path to database file')
    
    args = parser.parse_args()
    
    scraper = ComprehensiveNZXScraper(db_path=args.db_path)
    scraper.run_comprehensive_scraping(years=args.years, resume_from=args.resume_from)

if __name__ == "__main__":
    main()
