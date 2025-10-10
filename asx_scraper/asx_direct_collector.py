#!/usr/bin/env python3
"""
ASX Direct API Collector
Direct API approach for ASX announcement collection
"""
import requests
import json
from datetime import datetime, timedelta
import sqlite3
import os
import logging
from tqdm import tqdm
from asx_database import ASXDatabase
import asx_config as config

class ASXDirectCollector:
    """Direct API collector for ASX announcements"""
    
    def __init__(self):
        self.db = ASXDatabase()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.asx.com.au/asx/v2/statistics/announcements.do',
        })
        
        # ASX API endpoints
        self.api_endpoints = {
            'search': 'https://www.asx.com.au/asx/v2/statistics/announcements.do',
            'company': 'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode={ticker}',
            'json': 'https://www.asx.com.au/asx/v2/statistics/announcements.json'
        }
        
        # Top ASX companies
        self.companies = {
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
                logging.FileHandler('asx_direct_collection.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def search_announcements(self, ticker, days_back=365):
        """Search for announcements using ASX API"""
        try:
            # Try different API approaches
            announcements = []
            
            # Approach 1: Direct company search
            url = self.api_endpoints['company'].format(ticker=ticker)
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                announcements.extend(self.parse_html_response(response.text, ticker))
            
            # Approach 2: JSON API (if available)
            try:
                json_url = f"{self.api_endpoints['json']}?asxCode={ticker}"
                json_response = self.session.get(json_url, timeout=30)
                
                if json_response.status_code == 200:
                    try:
                        data = json_response.json()
                        announcements.extend(self.parse_json_response(data, ticker))
                    except json.JSONDecodeError:
                        pass
            except Exception as e:
                self.logger.debug(f"JSON API failed for {ticker}: {e}")
            
            # Approach 3: Form submission
            announcements.extend(self.submit_search_form(ticker))
            
            return announcements
            
        except Exception as e:
            self.logger.error(f"Error searching announcements for {ticker}: {e}")
            return []
    
    def parse_html_response(self, html_content, ticker):
        """Parse HTML response for announcements"""
        from bs4 import BeautifulSoup
        
        announcements = []
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for announcement tables
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
        
        # Look for announcement links
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            title = link.get_text(strip=True)
            
            if ('announcement' in href.lower() or 'pdf' in href.lower()) and len(title) > 10:
                if self.is_financial_report(title):
                    announcement = self.create_announcement(
                        ticker, title, None, href
                    )
                    if announcement:
                        announcements.append(announcement)
        
        return announcements
    
    def parse_json_response(self, json_data, ticker):
        """Parse JSON response for announcements"""
        announcements = []
        
        try:
            if isinstance(json_data, dict):
                if 'announcements' in json_data:
                    for item in json_data['announcements']:
                        title = item.get('title', '')
                        date = item.get('date', '')
                        url = item.get('url', '')
                        
                        if self.is_financial_report(title):
                            announcement = self.create_announcement(
                                ticker, title, date, url
                            )
                            if announcement:
                                announcements.append(announcement)
            
            elif isinstance(json_data, list):
                for item in json_data:
                    title = item.get('title', '')
                    date = item.get('date', '')
                    url = item.get('url', '')
                    
                    if self.is_financial_report(title):
                        announcement = self.create_announcement(
                            ticker, title, date, url
                        )
                        if announcement:
                            announcements.append(announcement)
        
        except Exception as e:
            self.logger.debug(f"Error parsing JSON for {ticker}: {e}")
        
        return announcements
    
    def submit_search_form(self, ticker):
        """Submit search form for announcements"""
        announcements = []
        
        try:
            # Get the search page first
            search_url = self.api_endpoints['search']
            response = self.session.get(search_url, timeout=30)
            
            if response.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find and submit forms
                forms = soup.find_all('form')
                for form in forms:
                    action = form.get('action', '')
                    method = form.get('method', 'get').lower()
                    
                    # Fix relative URLs
                    if action.startswith('/'):
                        action = f"https://www.asx.com.au{action}"
                    elif not action.startswith('http'):
                        action = search_url
                    
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
                    
                    # Set search parameters
                    form_data['asxCode'] = ticker
                    form_data['timeframe'] = 'period'
                    form_data['period'] = '12'
                    
                    # Submit form
                    if method == 'post':
                        form_response = self.session.post(action, data=form_data, timeout=30)
                    else:
                        form_response = self.session.get(action, params=form_data, timeout=30)
                    
                    if form_response.status_code == 200:
                        announcements.extend(
                            self.parse_html_response(form_response.text, ticker)
                        )
                        break  # Use first successful form
        
        except Exception as e:
            self.logger.debug(f"Form submission failed for {ticker}: {e}")
        
        return announcements
    
    def create_announcement(self, ticker, title, date_text=None, url=None):
        """Create announcement object"""
        try:
            announcement_date = None
            if date_text:
                announcement_date = self.parse_date(date_text)
            
            announcement_id = f"{ticker}_{hash(title) % 1000000}"
            
            announcement = {
                'announcement_id': announcement_id,
                'ticker': ticker.upper(),
                'company_name': self.companies.get(ticker, f"{ticker} Company"),
                'announcement_date': announcement_date,
                'title': title,
                'url': url or '',
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
    
    def run_collection(self):
        """Run full ASX collection"""
        print("=" * 80)
        print("ASX DIRECT API COLLECTION SYSTEM")
        print("=" * 80)
        
        total_announcements = 0
        total_financial = 0
        total_balance_sheets = 0
        successful_companies = 0
        
        # Process each company
        for ticker, company_name in tqdm(self.companies.items(), desc="Processing Companies"):
            try:
                announcements = self.search_announcements(ticker)
                
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
                
                # Rate limiting
                import time
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error processing {ticker}: {e}")
                continue
        
        # Final results
        print(f"\n" + "=" * 60)
        print("ASX DIRECT COLLECTION RESULTS")
        print("=" * 60)
        print(f"Companies processed: {successful_companies}/{len(self.companies)}")
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
    collector = ASXDirectCollector()
    results = collector.run_collection()
    
    print(f"\n" + "=" * 80)
    print("ASX DIRECT COLLECTION COMPLETE")
    print("=" * 80)
    print(f"Successfully collected ASX data:")
    print(f"  - {results['companies_processed']} companies processed")
    print(f"  - {results['total_announcements']} announcements")
    print(f"  - {results['financial_reports']} financial reports")
    print(f"  - {results['balance_sheet_reports']} balance sheet reports")

if __name__ == "__main__":
    main()
