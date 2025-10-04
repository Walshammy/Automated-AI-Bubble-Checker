"""
Enhanced Balance Sheet Scraper
Improved version of the NZX financial scraper with better PDF parsing and database integration
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import time
from datetime import datetime
import pdfplumber
import re
from urllib.parse import urljoin
import logging
import json
from balance_sheet_database import BalanceSheetDatabase
from balance_sheet_processor import FinancialStatementProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class EnhancedBalanceSheetScraper:
    """Enhanced scraper for balance sheets and financial data"""
    
    # Financial announcement types to target (NZX types)
    FINANCIAL_TYPES = ['FLLYR', 'HALFYR', 'INTERIM', 'QUARTERLY', 'ANNUAL', 'FULLYR', 'FLLYR', 'RESULTS']
    
    def __init__(self, base_dir='balance_sheet_data'):
        self.base_url = 'https://announcements.nzx.com'
        self.base_dir = Path(base_dir)
        self.pdf_dir = self.base_dir / 'pdfs'
        self.data_dir = self.base_dir / 'datasets'
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize database and processor
        self.db = BalanceSheetDatabase()
        self.processor = FinancialStatementProcessor()
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_nzsx_companies(self):
        """
        Get comprehensive list of NZSX companies
        """
        # Extended list of NZSX companies
        companies = [
            'AIR', 'ATM', 'FPH', 'MCY', 'SPK', 'RYM', 'SKC', 'TPW',
            'ARG', 'EBO', 'FBU', 'GMT', 'IFT', 'KMD', 'MEL', 'NZX',
            'PYS', 'RAK', 'SKO', 'SML', 'VCT', 'WHS', 'AIA', 'AFI',
            'GNE', 'CEN', 'NTL', 'CVT', 'TRA', 'BRW', 'POT', 'PCT',
            # Additional NZSX companies
            'BRM', 'CNU', 'CNV', 'CRP', 'EIR', 'ENS', 'FPA', 'FTZ',
            'GSH', 'HAU', 'HBL', 'HMU', 'HUM', 'IRT', 'JLG', 'KFL',
            'KYN', 'MAE', 'MDZ', 'MET', 'MLZ', 'MWR', 'NZM', 'OCT',
            'PGW', 'PLX', 'PNH', 'PPH', 'SCT', 'SPN', 'STM', 'SUM',
            'TWR', 'VTL', 'WHK', 'WHS', 'WYN', 'ZKB'
        ]
        
        return pd.DataFrame({'ticker': companies})
    
    def scrape_financial_announcements(self, ticker=None, limit=500):
        """
        Scrape financial announcements with improved filtering
        """
        logging.info(f"Scraping financial announcements{' for ' + ticker if ticker else ''}...")
        
        try:
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            announcements = []
            rows = soup.find_all('tr')[1:limit+1]  # Skip header
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 7:  # NZX has 7 columns
                    # NZX structure: [#, id, company, title, date, type, flags]
                    ann_id = cols[1].text.strip()      # Col 1: actual announcement ID
                    ann_ticker = cols[2].text.strip()   # Col 2: ticker
                    title_link = cols[3].find('a')      # Col 3: title
                    title = title_link.text.strip() if title_link else cols[3].text.strip()
                    link = title_link.get('href') if title_link else None
                    date_time = cols[4].text.strip()    # Col 4: date
                    ann_type = cols[5].text.strip()     # Col 5: type
                    
                    # Enhanced filtering for financial announcements
                    financial_keywords = [
                        'results', 'financial', 'annual report', 'interim', 
                        'earnings', 'revenue', 'profit', 'balance', 'statement',
                        'report', 'performance', 'quarterly', 'annual meeting',
                        'annual results', 'fiscal year', 'climate statement', 
                        'investor update', 'navigator', 'update'
                    ]
                    
                    if (ann_type in self.FINANCIAL_TYPES or 
                        any(term in title.lower() for term in financial_keywords)):
                        
                        # If ticker specified, filter by it
                        if ticker is None or ann_ticker == ticker:
                            announcement_data = {
                                'announcement_id': ann_id,
                                'ticker': ann_ticker,
                                'title': title,
                                'announcement_url': urljoin(self.base_url, link) if link else None,
                                'announcement_date': date_time,
                                'announcement_type': ann_type,
                                'exchange': 'NZX',
                                'scraped_at': datetime.now().isoformat()
                            }
                            
                            # Insert into database
                            self.db.insert_announcement(announcement_data)
                            announcements.append(announcement_data)
            
            logging.info(f"Found {len(announcements)} financial announcements")
            return announcements
            
        except Exception as e:
            logging.error(f"Error scraping: {e}")
            return []
    
    def get_pdf_links(self, announcement_url):
        """Extract PDF URLs with enhanced filtering"""
        try:
            response = self.session.get(announcement_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            pdf_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                link_text = link.text.strip().lower()
                
                if '.pdf' in href.lower():
                    # Enhanced filtering for relevant financial documents
                    if any(term in link_text for term in [
                        'annual report', 'financial report', 'interim report',
                        'quarterly report', 'results', 'financial statements',
                        'balance sheet', 'income statement', 'cash flow'
                    ]):
                        full_url = urljoin(self.base_url, href)
                        pdf_links.append({
                            'url': full_url,
                            'text': link.text.strip(),
                            'document_type': self._categorize_document_type(link_text)
                        })
            
            return pdf_links
        except Exception as e:
            logging.error(f"Error getting PDFs: {e}")
            return []
    
    def _categorize_document_type(self, link_text):
        """Categorize document type based on text"""
        link_text = link_text.lower()
        
        if 'annual report' in link_text:
            return 'annual_report'
        elif 'interim' in link_text:
            return 'interim_report'
        elif 'quarterly' in link_text:
            return 'quarterly_report'
        elif 'financial' in link_text or 'results' in link_text:
            return 'financial_statements'
        else:
            return 'other_financial'
    
    def download_pdf(self, pdf_info, ticker, announcement_id):
        """Download PDF with enhanced error handling and size tracking"""
        try:
            company_dir = self.pdf_dir / ticker
            company_dir.mkdir(exist_ok=True)
            
            # Clean filename
            clean_name = re.sub(r'[^\w\-_\. ]', '_', pdf_info['text'])[:50]
            filename = f"{announcement_id}_{clean_name}.pdf"
            filepath = company_dir / filename
            
            if filepath.exists():
                logging.info(f"PDF exists: {filepath.name}")
                file_size = filepath.stat().st_size / 1024  # KB
                
                # Still update database
                document_data = {
                    'announcement_id': announcement_id,
                    'pdf_url': pdf_info['url'],
                    'pdf_filename': filename,
                    'pdf_path': str(filepath),
                    'document_type': pdf_info['document_type'],
                    'file_size_kb': int(file_size)
                }
                self.db.insert_document(document_data)
                return filepath
            
            response = self.session.get(pdf_info['url'], timeout=60, stream=True)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            file_size = filepath.stat().st_size / 1024  # KB
            logging.info(f"Downloaded: {filepath.name} ({file_size:.1f} KB)")
            
            # Insert document data into database
            document_data = {
                'announcement_id': announcement_id,
                'pdf_url': pdf_info['url'],
                'pdf_filename': filename,
                'pdf_path': str(filepath),
                'document_type': pdf_info['document_type'],
                'file_size_kb': int(file_size)
            }
            self.db.insert_document(document_data)
            
            time.sleep(1)
            return filepath
            
        except Exception as e:
            logging.error(f"Download error: {e}")
            return None
    
    def process_company_financials(self, ticker, years_back=3):
        """
        Enhanced processing of company financial data
        """
        logging.info(f"\n{'='*60}\nProcessing {ticker}\n{'='*60}")
        
        # Get announcements
        announcements = self.scrape_financial_announcements(ticker=ticker)
        
        if not announcements:
            logging.warning(f"No financial announcements found for {ticker}")
            return []
        
        results = []
        
        for announcement in announcements:
            logging.info(f"\nProcessing: {announcement['title']} ({announcement['announcement_date']})")
            
            if not announcement['announcement_url']:
                continue
            
            # Get PDFs
            pdf_links = self.get_pdf_links(announcement['announcement_url'])
            
            for pdf_info in pdf_links:
                # Download PDF
                pdf_path = self.download_pdf(
                    pdf_info,
                    ticker,
                    announcement['announcement_id']
                )
                
                if pdf_path:
                    # Process PDF and extract financial data
                    financial_data = self.processor.extract_comprehensive_financial_data(
                        pdf_path=pdf_path,
                        ticker=ticker,
                        announcement_id=announcement['announcement_id'],
                        report_date=announcement['announcement_date'],
                        report_type=announcement['announcement_type']
                    )
                    
                    if financial_data:
                        # Insert into database
                        success = self.db.insert_balance_sheet_data(financial_data)
                        if success:
                            results.append(financial_data)
                            logging.info(f"Successfully extracted and stored financial data")
                        break  # One PDF per announcement
        
        return results
    
    def build_comprehensive_dataset(self, tickers=None, save_local=True):
        """
        Build comprehensive dataset across multiple companies
        """
        if tickers is None:
            companies_df = self.get_nzsx_companies()
            tickers = companies_df['ticker'].tolist()
        
        total_results = []
        
        for i, ticker in enumerate(tickers, 1):
            try:
                logging.info(f"\nProcessing {ticker} ({i}/{len(tickers)})")
                company_data = self.process_company_financials(ticker)
                total_results.extend(company_data)
                
                # Progress saving
                if save_local and company_data:
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    results_df = pd.DataFrame(company_data)
                    csv_path = self.data_dir / f'{ticker}_financials_{timestamp}.csv'
                    results_df.to_csv(csv_path, index=False)
                    logging.info(f"Saved {ticker} data to {csv_path}")
                
                time.sleep(2)  # Be respectful
                
            except Exception as e:
                logging.error(f"Error processing {ticker}: {e}")
        
        # Final dataset
        if total_results:
            comprehensive_df = pd.DataFrame(total_results)
            
            if save_local:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                csv_path = self.data_dir / f'comprehensive_financials_{timestamp}.csv'
                comprehensive_df.to_csv(csv_path, index=False)
                logging.info(f"\nComprehensive dataset saved to: {csv_path}")
                
                # Summary statistics
                self._print_dataset_summary(comprehensive_df)
        
        return total_results
    
    def _print_dataset_summary(self, df):
        """Print summary of the dataset"""
        if df.empty:
            logging.warning("No data in dataset")
            return
        
        print(f"\n{'='*60}")
        print(f"DATASET SUMMARY")
        print(f"{'='*60}")
        print(f"Total Records: {len(df):,}")
        print(f"Unique Companies: {df['ticker'].nunique()}")
        print(f"Date Range: {df['report_date'].min()} to {df['report_date'].max()}")
        print(f"\nTop Companies by Records:")
        
        top_companies = df.groupby('ticker').size().sort_values(ascending=False).head(10)
        for ticker, count in top_companies.items():
            print(f"  {ticker}: {count} records")
        
        print(f"\nData Quality Metrics:")
        financial_metrics = [
            'revenue', 'total_assets', 'net_income', 'total_equity', 
            'current_ratio', 'debt_to_equity'
        ]
        
        for metric in financial_metrics:
            if metric in df.columns:
                non_null_count = df[metric].notna().sum()
                print(f"  {metric}: {non_null_count}/{len(df)} records ({non_null_count/len(df)*100:.1f}%)")

def main():
    """Enhanced main function"""
    
    scraper = EnhancedBalanceSheetScraper()
    
    print("ENHANCED BALANCE SHEET SCRAPER")
    print("=" * 60)
    
    # Test with specific companies first
    test_tickers = ['AIR', 'FPH', 'MCY', 'SPK']
    
    logging.info(f"Starting with test tickers: {test_tickers}")
    
    for ticker in test_tickers:
        try:
            results = scraper.process_company_financials(ticker)
            logging.info(f"Processed {ticker}: {len(results)} records extracted")
            time.sleep(3)  # Respectful delay
        except Exception as e:
            logging.error(f"Error processing {ticker}: {e}")
    
    # Check database stats
    db_stats = scraper.db.get_database_stats()
    print(f"\nDatabase Statistics:")
    for key, value in db_stats.items():
        print(f"  {key}: {value}")
    
    return scraper

if __name__ == "__main__":
    scraper = main()
