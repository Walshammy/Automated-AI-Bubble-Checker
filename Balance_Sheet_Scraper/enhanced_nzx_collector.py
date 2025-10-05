#!/usr/bin/env python3
"""
Enhanced NZX Data Collector with Detailed Output
===============================================

Improved version that provides detailed information when companies are processed.
Shows progress, results, and data extraction status for each company.

Author: AI Assistant
Date: 2025-10-05
"""

import subprocess
import time
import logging
from datetime import datetime
from pathlib import Path
import sys
import sqlite3
import pandas as pd

# Setup logging with UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_nzx_collection.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

class EnhancedNZXCollector:
    """Enhanced collector with detailed output for each company"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.output_dir = self.base_dir / "enhanced_nzx_data"
        self.output_dir.mkdir(exist_ok=True)
        
        # Database path
        self.db_path = "../data_collection/unified_stock_data.db"
        
        # Major NZX companies for priority collection
        self.major_companies = [
            'AIR', 'ATM', 'FPH', 'MCY', 'SPK', 'RYM', 'SKC', 'TPW',
            'ARG', 'EBO', 'FBU', 'GMT', 'IFT', 'KMD', 'MEL', 'NZX',
            'PYS', 'RAK', 'SKO', 'SML', 'VCT', 'WHS', 'AIA', 'AFI',
            'GNE', 'CEN', 'NTL', 'CVT', 'TRA', 'BRW', 'POT', 'PCT'
        ]
        
        # Extended NZX companies
        self.extended_companies = [
            'BRM', 'CNU', 'CNV', 'CRP', 'EIR', 'ENS', 'FPA', 'FTZ',
            'GSH', 'HAU', 'HBL', 'HMU', 'HUM', 'IRT', 'JLG', 'KFL',
            'KYN', 'MAE', 'MDZ', 'MET', 'MLZ', 'MWR', 'NZM', 'OCT',
            'PGW', 'PLX', 'PNH', 'PPH', 'SCT', 'SPN', 'STM', 'SUM',
            'TWR', 'VTL', 'WHK', 'WYN', 'ZKB'
        ]
    
    def get_company_stats(self, ticker):
        """Get current stats for a specific company"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Count announcements for this company
            cursor.execute("SELECT COUNT(*) FROM financial_announcements WHERE ticker = ?", (ticker,))
            ann_count = cursor.fetchone()[0]
            
            # Count documents for this company
            cursor.execute("""
                SELECT COUNT(*) FROM financial_documents fd
                JOIN financial_announcements fa ON fd.announcement_id = fa.announcement_id
                WHERE fa.ticker = ?
            """, (ticker,))
            doc_count = cursor.fetchone()[0]
            
            # Count balance sheet records for this company
            cursor.execute("SELECT COUNT(*) FROM balance_sheet_data WHERE ticker = ?", (ticker,))
            bs_count = cursor.fetchone()[0]
            
            # Get latest announcement date
            cursor.execute("""
                SELECT MAX(announcement_date) FROM financial_announcements 
                WHERE ticker = ?
            """, (ticker,))
            latest_date = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'announcements': ann_count,
                'documents': doc_count,
                'balance_sheets': bs_count,
                'latest_date': latest_date
            }
            
        except Exception as e:
            logging.error(f"Error getting stats for {ticker}: {e}")
            return {'announcements': 0, 'documents': 0, 'balance_sheets': 0, 'latest_date': None}
    
    def process_company_detailed(self, ticker, years=10):
        """Process a single company with detailed output"""
        print(f"\n{'='*80}")
        print(f"PROCESSING COMPANY: {ticker}")
        print(f"{'='*80}")
        
        # Get initial stats
        initial_stats = self.get_company_stats(ticker)
        print(f"INITIAL STATS:")
        print(f"  Announcements: {initial_stats['announcements']}")
        print(f"  Documents: {initial_stats['documents']}")
        print(f"  Balance Sheets: {initial_stats['balance_sheets']}")
        print(f"  Latest Date: {initial_stats['latest_date'] or 'None'}")
        
        # Run the scraper
        print(f"\nSTARTING DATA COLLECTION...")
        start_time = time.time()
        
        cmd_args = [
            "--tickers", ticker,
            "--years", str(years),
            "--export", "csv",
            "--output-dir", str(self.output_dir)
        ]
        
        try:
            process = subprocess.Popen(
                ["python", "main_balance_sheet_scraper.py"] + cmd_args,
                cwd=str(self.base_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate(timeout=300)  # 5 minute timeout
            duration = time.time() - start_time
            
            print(f"COLLECTION COMPLETED in {duration:.1f} seconds")
            
            # Get final stats
            final_stats = self.get_company_stats(ticker)
            print(f"\nFINAL STATS:")
            print(f"  Announcements: {final_stats['announcements']} (+{final_stats['announcements'] - initial_stats['announcements']})")
            print(f"  Documents: {final_stats['documents']} (+{final_stats['documents'] - initial_stats['documents']})")
            print(f"  Balance Sheets: {final_stats['balance_sheets']} (+{final_stats['balance_sheets'] - initial_stats['balance_sheets']})")
            print(f"  Latest Date: {final_stats['latest_date'] or 'None'}")
            
            # Show results
            if process.returncode == 0:
                print(f"\nSTATUS: SUCCESS")
                if stdout:
                    print(f"OUTPUT: {stdout[-300:]}")  # Last 300 chars
            else:
                print(f"\nSTATUS: FAILED (Return code: {process.returncode})")
                if stderr:
                    print(f"ERROR: {stderr}")
            
            # Calculate success metrics
            new_announcements = final_stats['announcements'] - initial_stats['announcements']
            new_documents = final_stats['documents'] - initial_stats['documents']
            new_balance_sheets = final_stats['balance_sheets'] - initial_stats['balance_sheets']
            
            print(f"\nSUMMARY:")
            print(f"  New Announcements Found: {new_announcements}")
            print(f"  New Documents Downloaded: {new_documents}")
            print(f"  New Balance Sheets Extracted: {new_balance_sheets}")
            
            if new_balance_sheets > 0:
                print(f"  RESULT: SUCCESS - Financial data extracted!")
                print(f"  PDF CLEANUP: PDF files deleted after successful data extraction")
            elif new_documents > 0:
                print(f"  RESULT: PARTIAL - Documents downloaded but no financial data extracted")
                print(f"  PDF CLEANUP: PDF files retained (no data extracted)")
            elif new_announcements > 0:
                print(f"  RESULT: LIMITED - Announcements found but no documents downloaded")
            else:
                print(f"  RESULT: NO DATA - No new data found for this company")
            
            return {
                'success': process.returncode == 0,
                'new_announcements': new_announcements,
                'new_documents': new_documents,
                'new_balance_sheets': new_balance_sheets,
                'duration': duration
            }
            
        except subprocess.TimeoutExpired:
            print(f"TIMEOUT: Collection timed out after 5 minutes")
            process.kill()
            return {'success': False, 'timeout': True}
        except Exception as e:
            print(f"ERROR: {e}")
            return {'success': False, 'error': str(e)}
    
    def collect_major_companies_detailed(self):
        """Collect data for major companies with detailed output"""
        print(f"\n{'='*80}")
        print(f"STARTING DETAILED COLLECTION FOR MAJOR NZX COMPANIES")
        print(f"{'='*80}")
        print(f"Companies to process: {len(self.major_companies)}")
        print(f"Years of data: 10")
        print(f"Output directory: {self.output_dir}")
        
        results = []
        successful = 0
        failed = 0
        
        for i, company in enumerate(self.major_companies, 1):
            print(f"\n[{i}/{len(self.major_companies)}] Processing {company}...")
            
            result = self.process_company_detailed(company, years=10)
            results.append({'company': company, **result})
            
            if result.get('success', False):
                successful += 1
            else:
                failed += 1
            
            # Brief pause between companies
            time.sleep(2)
        
        # Final summary
        print(f"\n{'='*80}")
        print(f"MAJOR COMPANIES COLLECTION COMPLETE")
        print(f"{'='*80}")
        print(f"Total Processed: {len(self.major_companies)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Success Rate: {successful/len(self.major_companies)*100:.1f}%")
        
        # Show top performers
        companies_with_data = [r for r in results if r.get('new_balance_sheets', 0) > 0]
        if companies_with_data:
            print(f"\nCOMPANIES WITH FINANCIAL DATA EXTRACTED:")
            for result in sorted(companies_with_data, key=lambda x: x['new_balance_sheets'], reverse=True):
                print(f"  {result['company']}: {result['new_balance_sheets']} balance sheets")
        
        return results
    
    def get_overall_stats(self):
        """Get overall database statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            stats = {}
            
            # Total counts
            cursor.execute("SELECT COUNT(*) FROM financial_announcements")
            stats['total_announcements'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM financial_documents")
            stats['total_documents'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM balance_sheet_data")
            stats['total_balance_sheets'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT ticker) FROM financial_announcements")
            stats['unique_companies'] = cursor.fetchone()[0]
            
            # Latest data
            cursor.execute("SELECT MAX(scraped_at) FROM financial_announcements")
            stats['last_scraped'] = cursor.fetchone()[0]
            
            conn.close()
            return stats
            
        except Exception as e:
            logging.error(f"Error getting overall stats: {e}")
            return {}
    
    def run_enhanced_collection(self):
        """Run the enhanced collection process"""
        print(f"ENHANCED NZX DATA COLLECTOR")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Show initial stats
        initial_stats = self.get_overall_stats()
        print(f"\nINITIAL DATABASE STATS:")
        for key, value in initial_stats.items():
            print(f"  {key}: {value}")
        
        # Collect major companies
        results = self.collect_major_companies_detailed()
        
        # Show final stats
        final_stats = self.get_overall_stats()
        print(f"\nFINAL DATABASE STATS:")
        for key, value in final_stats.items():
            print(f"  {key}: {value}")
        
        # Calculate improvements
        print(f"\nIMPROVEMENTS:")
        for key in ['total_announcements', 'total_documents', 'total_balance_sheets', 'unique_companies']:
            if key in initial_stats and key in final_stats:
                improvement = final_stats[key] - initial_stats[key]
                print(f"  {key}: +{improvement}")
        
        # Final PDF cleanup
        print(f"\nPDF CLEANUP:")
        print(f"  PDF files are automatically deleted after successful data extraction")
        print(f"  Only PDFs with extracted financial data are removed")
        print(f"  Failed extractions retain PDFs for potential retry")
        
        print(f"\nCollection completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """Main function"""
    collector = EnhancedNZXCollector()
    collector.run_enhanced_collection()

if __name__ == "__main__":
    main()
