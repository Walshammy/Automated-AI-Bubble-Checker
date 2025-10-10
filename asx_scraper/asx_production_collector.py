#!/usr/bin/env python3
"""
ASX Production Collection System
Streamlined, production-ready ASX announcement collection
"""
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import sqlite3
import os
import logging
from tqdm import tqdm
from asx_database import ASXDatabase
import asx_config as config

class ASXProductionCollector:
    """Production-ready ASX announcement collector"""
    
    def __init__(self):
        self.db = ASXDatabase()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        
        # Production ASX URLs
        self.asx_urls = {
            'CBA': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=CBA',
            'WBC': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=WBC',
            'ANZ': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=ANZ',
            'NAB': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=NAB',
            'BHP': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=BHP',
            'RIO': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=RIO',
            'FMG': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=FMG',
            'WDS': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=WDS',
            'CSL': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=CSL',
            'TLS': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=TLS',
            'WES': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=WES',
            'WOW': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=WOW',
            'COL': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=COL',
            'GMG': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=GMG',
            'SGP': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=SGP',
            'ORG': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=ORG',
            'STO': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=STO',
            'AGL': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=AGL',
            'ALL': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=ALL',
            'CAR': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=CAR',
            'IAG': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=IAG',
            'JHX': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=JHX',
            'QAN': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=QAN',
            'REA': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=REA',
            'SUN': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=SUN',
            'TCL': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=TCL',
        }
        
        # Company names mapping
        self.company_names = {
            'CBA': 'Commonwealth Bank of Australia',
            'WBC': 'Westpac Banking Corporation',
            'ANZ': 'ANZ Group Holdings',
            'NAB': 'National Australia Bank',
            'BHP': 'BHP Group Limited',
            'RIO': 'Rio Tinto Limited',
            'FMG': 'Fortescue Metals Group',
            'WDS': 'Woodside Energy Group',
            'CSL': 'CSL Limited',
            'TLS': 'Telstra Group Limited',
            'WES': 'Wesfarmers Limited',
            'WOW': 'Woolworths Group Limited',
            'COL': 'Coles Group Limited',
            'GMG': 'Goodman Group',
            'SGP': 'Stockland Corporation',
            'ORG': 'Origin Energy Limited',
            'STO': 'Santos Limited',
            'AGL': 'AGL Energy Limited',
            'ALL': 'Aristocrat Leisure Limited',
            'CAR': 'Carsales.com Limited',
            'IAG': 'Insurance Australia Group',
            'JHX': 'James Hardie Industries',
            'QAN': 'Qantas Airways Limited',
            'REA': 'REA Group Limited',
            'SUN': 'Suncorp Group Limited',
            'TCL': 'Transurban Group',
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('asx_production.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def collect_company_announcements(self, ticker, url):
        """Collect announcements for a specific company"""
        self.logger.info(f"Collecting announcements for {ticker}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            announcements = []
            
            # Look for forms (ASX uses form-based search)
            forms = soup.find_all('form')
            if forms:
                self.logger.info(f"Found {len(forms)} forms for {ticker}")
                
                # Try to submit form to get actual data
                announcements = self.submit_form_and_extract(ticker, forms[0])
            
            # Also look for any existing announcement data
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        try:
                            title = cells[1].get_text(strip=True)
                            if len(title) > 10 and self.is_financial_report(title):
                                announcement = self.create_announcement(
                                    ticker, title, cells[0].get_text(strip=True)
                                )
                                if announcement:
                                    announcements.append(announcement)
                        except Exception as e:
                            continue
            
            self.logger.info(f"Found {len(announcements)} announcements for {ticker}")
            return announcements
            
        except Exception as e:
            self.logger.error(f"Error collecting {ticker}: {e}")
            return []
    
    def submit_form_and_extract(self, ticker, form):
        """Submit form and extract announcements"""
        announcements = []
        
        try:
            # Get form action and method
            action = form.get('action', '')
            method = form.get('method', 'get').lower()
            
            # Fix relative URLs
            if action.startswith('/'):
                action = f"https://www.asx.com.au{action}"
            elif not action.startswith('http'):
                action = f"https://www.asx.com.au/asx/v2/statistics/announcements.do"
            
            # Prepare form data
            form_data = {}
            inputs = form.find_all(['input', 'select', 'textarea'])
            
            for input_elem in inputs:
                name = input_elem.get('name')
                value = input_elem.get('value', '')
                input_type = input_elem.get('type', 'text')
                
                if name:
                    if input_type == 'checkbox' or input_type == 'radio':
                        if input_elem.get('checked'):
                            form_data[name] = value
                    else:
                        form_data[name] = value
            
            # Set default values for ASX form
            form_data['asxCode'] = ticker
            form_data['timeframe'] = 'period'  # Search by period
            form_data['period'] = '12'  # Last 12 months
            
            # Submit form
            if method == 'post':
                response = self.session.post(action, data=form_data, timeout=30)
            else:
                response = self.session.get(action, params=form_data, timeout=30)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract announcements from response
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            try:
                                date_text = cells[0].get_text(strip=True)
                                title = cells[1].get_text(strip=True)
                                
                                if len(title) > 10 and self.is_financial_report(title):
                                    announcement = self.create_announcement(
                                        ticker, title, date_text
                                    )
                                    if announcement:
                                        announcements.append(announcement)
                            except Exception as e:
                                continue
            
        except Exception as e:
            self.logger.warning(f"Form submission failed for {ticker}: {e}")
        
        return announcements
    
    def create_announcement(self, ticker, title, date_text=None):
        """Create announcement object"""
        try:
            announcement_date = None
            if date_text:
                announcement_date = self.parse_date(date_text)
            
            announcement_id = f"{ticker}_{hash(title) % 1000000}"
            
            announcement = {
                'announcement_id': announcement_id,
                'ticker': ticker.upper(),
                'company_name': self.company_names.get(ticker, f"{ticker} Company"),
                'announcement_date': announcement_date,
                'title': title,
                'url': '',  # Will be filled when we get real PDF URLs
                'file_size': None,
                'market_sensitive': self.is_market_sensitive(title),
                'is_financial_report': self.is_financial_report(title),
                'is_balance_sheet': self.is_balance_sheet_report(title),
                'pdf_filename': None
            }
            
            return announcement
            
        except Exception as e:
            self.logger.warning(f"Error creating announcement for {ticker}: {e}")
            return None
    
    def is_financial_report(self, title):
        """Check if announcement is a financial report"""
        title_lower = title.lower()
        financial_keywords = [
            'annual report', 'financial', 'balance sheet', 'results', 'earnings',
            'quarterly', 'half year', 'full year', 'financial statements',
            'profit', 'revenue', 'dividend', 'audit', 'interim', 'preliminary',
            'cash flow', 'income statement', 'consolidated', 'unaudited'
        ]
        return any(keyword in title_lower for keyword in financial_keywords)
    
    def is_balance_sheet_report(self, title):
        """Check if announcement specifically contains balance sheet data"""
        title_lower = title.lower()
        balance_sheet_terms = [
            'balance sheet', 'financial statements', 'annual report', 'half year',
            'full year', 'quarterly report', 'consolidated financial statements'
        ]
        return any(term in title_lower for term in balance_sheet_terms)
    
    def is_market_sensitive(self, title):
        """Check if announcement is market sensitive"""
        title_lower = title.lower()
        sensitive_terms = [
            'profit', 'loss', 'earnings', 'revenue', 'dividend', 'acquisition',
            'merger', 'restructure', 'ceo', 'cfo', 'resignation', 'appointment'
        ]
        return any(term in title_lower for term in sensitive_terms)
    
    def parse_date(self, date_text):
        """Parse date text into datetime object"""
        try:
            date_patterns = [
                '%d/%m/%Y',      # DD/MM/YYYY
                '%d-%m-%Y',      # DD-MM-YYYY
                '%Y-%m-%d',      # YYYY-MM-DD
                '%d %b %Y',      # DD Mon YYYY
                '%d %B %Y',      # DD Month YYYY
                '%B %d, %Y',     # Month DD, YYYY
                '%b %d, %Y',     # Mon DD, YYYY
            ]
            
            for pattern in date_patterns:
                try:
                    return datetime.strptime(date_text.strip(), pattern)
                except ValueError:
                    continue
            
            return None
            
        except Exception as e:
            return None
    
    def run_full_collection(self):
        """Run full ASX collection"""
        print("=" * 80)
        print("ASX PRODUCTION COLLECTION SYSTEM")
        print("=" * 80)
        
        total_announcements = 0
        total_financial = 0
        total_balance_sheets = 0
        successful_companies = 0
        
        # Process each company
        for ticker, url in tqdm(self.asx_urls.items(), desc="Processing Companies"):
            try:
                announcements = self.collect_company_announcements(ticker, url)
                
                if announcements:
                    # Save to database
                    new_count = 0
                    financial_count = 0
                    balance_sheet_count = 0
                    
                    for announcement in announcements:
                        if self.db.insert_announcement(announcement):
                            new_count += 1
                            if announcement['is_financial_report']:
                                financial_count += 1
                            if announcement.get('is_balance_sheet', False):
                                balance_sheet_count += 1
                    
                    total_announcements += new_count
                    total_financial += financial_count
                    total_balance_sheets += balance_sheet_count
                    successful_companies += 1
                    
                    self.logger.info(f"SUCCESS: {ticker} - {new_count} announcements ({financial_count} financial)")
                else:
                    self.logger.warning(f"NO DATA: {ticker} - No announcements found")
                
                time.sleep(2)  # Rate limiting
                
            except Exception as e:
                self.logger.error(f"Error processing {ticker}: {e}")
                continue
        
        # Final results
        print(f"\n" + "=" * 60)
        print("ASX PRODUCTION COLLECTION RESULTS")
        print("=" * 60)
        print(f"Companies processed: {successful_companies}/{len(self.asx_urls)}")
        print(f"Total announcements: {total_announcements}")
        print(f"Financial reports: {total_financial}")
        print(f"Balance sheet reports: {total_balance_sheets}")
        
        # Database statistics
        db_stats = self.db.get_statistics()
        print(f"\nUpdated Database Statistics:")
        for key, value in db_stats.items():
            print(f"  {key}: {value}")
        
        return {
            'companies_processed': successful_companies,
            'total_announcements': total_announcements,
            'financial_reports': total_financial,
            'balance_sheet_reports': total_balance_sheets
        }

def main():
    """Main execution function"""
    collector = ASXProductionCollector()
    results = collector.run_full_collection()
    
    print(f"\n" + "=" * 80)
    print("ASX PRODUCTION COLLECTION COMPLETE")
    print("=" * 80)
    print(f"Successfully collected ASX data:")
    print(f"  - {results['companies_processed']} companies processed")
    print(f"  - {results['total_announcements']} announcements")
    print(f"  - {results['financial_reports']} financial reports")
    print(f"  - {results['balance_sheet_reports']} balance sheet reports")

if __name__ == "__main__":
    main()
