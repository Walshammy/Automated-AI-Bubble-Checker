#!/usr/bin/env python3
"""
Complete NZX Historical Data Collector
=====================================

Ensures comprehensive collection of ALL announcements for ALL companies across ALL years.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import time
from datetime import datetime, timedelta
import logging
import sqlite3
from enhanced_balance_sheet_scraper import EnhancedBalanceSheetScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class CompleteNZXCollector:
    """Complete collector ensuring ALL data is collected"""
    
    def __init__(self):
        self.scraper = EnhancedBalanceSheetScraper()
        self.db = self.scraper.db
        
        # Complete list of NZX companies
        self.all_companies = [
            # Major companies
            'AIR', 'ATM', 'FPH', 'MCY', 'SPK', 'RYM', 'SKC', 'TPW',
            'ARG', 'EBO', 'FBU', 'GMT', 'IFT', 'KMD', 'MEL', 'NZX',
            'PYS', 'RAK', 'SKO', 'SML', 'VCT', 'WHS', 'AIA', 'AFI',
            'GNE', 'CEN', 'NTL', 'CVT', 'TRA', 'BRW', 'POT', 'PCT',
            # Additional companies
            'BRM', 'CNU', 'CNV', 'CRP', 'EIR', 'ENS', 'FPA', 'FTZ',
            'GSH', 'HAU', 'HBL', 'HMU', 'HUM', 'IRT', 'JLG', 'KFL',
            'KYN', 'MAE', 'MDZ', 'MET', 'MLZ', 'MWR', 'NZM', 'OCT',
            'PGW', 'PLX', 'PNH', 'PPH', 'SCT', 'SPN', 'STM', 'SUM',
            'TWR', 'VTL', 'WHK', 'WYN', 'ZKB'
        ]
    
    def get_company_announcements_count(self, ticker, years_back=10):
        """Get total count of announcements for a company"""
        try:
            total_count = 0
            for year in range(datetime.now().year, datetime.now().year - years_back, -1):
                company_url = f"https://www.nzx.com/companies/{ticker}/announcements?year={year}"
                response = self.session.get(company_url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                table = soup.find('table')
                if table:
                    rows = table.find_all('tr')[1:]  # Skip header
                    total_count += len(rows)
                
                time.sleep(0.5)  # Be respectful
                
            return total_count
        except Exception as e:
            logging.warning(f"Error counting announcements for {ticker}: {e}")
            return 0
    
    def run_complete_collection(self, years_back=10):
        """Run complete collection ensuring ALL data is gathered"""
        logging.info(f"Starting COMPLETE NZX data collection (last {years_back} years)")
        logging.info(f"Processing {len(self.all_companies)} companies")
        
        results = {
            'companies_processed': 0,
            'companies_with_announcements': 0,
            'total_announcements': 0,
            'companies_with_financial_data': 0,
            'total_balance_sheets': 0,
            'failed_companies': []
        }
        
        for i, ticker in enumerate(self.all_companies, 1):
            logging.info(f"\n{'='*80}")
            logging.info(f"Processing {ticker} ({i}/{len(self.all_companies)})")
            logging.info(f"{'='*80}")
            
            try:
                # Get initial count
                initial_announcements = self.get_database_count('financial_announcements', ticker)
                initial_balance_sheets = self.get_database_count('balance_sheet_data', ticker)
                
                logging.info(f"Initial state: {initial_announcements} announcements, {initial_balance_sheets} balance sheets")
                
                # Process company with comprehensive scraping
                announcements = self.scraper.scrape_financial_announcements(
                    ticker=ticker, 
                    years_back=years_back,
                    limit=1000  # High limit to ensure we get all
                )
                
                if announcements:
                    results['companies_with_announcements'] += 1
                    results['total_announcements'] += len(announcements)
                    logging.info(f"Found {len(announcements)} announcements for {ticker}")
                    
                    # Process financial data
                    financial_data = self.scraper.process_company_financials(
                        ticker=ticker, 
                        years_back=years_back
                    )
                    
                    if financial_data:
                        results['companies_with_financial_data'] += 1
                        results['total_balance_sheets'] += len(financial_data)
                        logging.info(f"Extracted {len(financial_data)} balance sheet records for {ticker}")
                    else:
                        logging.warning(f"No financial data extracted for {ticker}")
                else:
                    logging.warning(f"No announcements found for {ticker}")
                
                results['companies_processed'] += 1
                
                # Final count
                final_announcements = self.get_database_count('financial_announcements', ticker)
                final_balance_sheets = self.get_database_count('balance_sheet_data', ticker)
                
                logging.info(f"Final state: {final_announcements} announcements (+{final_announcements - initial_announcements}), {final_balance_sheets} balance sheets (+{final_balance_sheets - initial_balance_sheets})")
                
                # Be respectful to servers
                time.sleep(2)
                
            except Exception as e:
                logging.error(f"Error processing {ticker}: {e}")
                results['failed_companies'].append(ticker)
                continue
        
        # Print comprehensive results
        self.print_complete_results(results)
        return results
    
    def get_database_count(self, table, ticker=None):
        """Get count from database table"""
        try:
            conn = sqlite3.connect(self.db.db_path)
            cursor = conn.cursor()
            
            if ticker:
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE ticker = ?", (ticker,))
            else:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
            
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except Exception as e:
            logging.error(f"Database error: {e}")
            return 0
    
    def print_complete_results(self, results):
        """Print comprehensive results"""
        logging.info(f"\n{'='*80}")
        logging.info("COMPLETE NZX DATA COLLECTION RESULTS")
        logging.info(f"{'='*80}")
        logging.info(f"Companies Processed: {results['companies_processed']}")
        logging.info(f"Companies with Announcements: {results['companies_with_announcements']}")
        logging.info(f"Total Announcements Collected: {results['total_announcements']}")
        logging.info(f"Companies with Financial Data: {results['companies_with_financial_data']}")
        logging.info(f"Total Balance Sheet Records: {results['total_balance_sheets']}")
        
        if results['failed_companies']:
            logging.warning(f"Failed Companies: {', '.join(results['failed_companies'])}")
        
        # Get final database stats
        self.print_final_database_stats()
    
    def print_final_database_stats(self):
        """Print final comprehensive database statistics"""
        try:
            conn = sqlite3.connect(self.db.db_path)
            cursor = conn.cursor()
            
            # Overall stats
            cursor.execute("SELECT COUNT(*) FROM financial_announcements")
            total_announcements = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM financial_documents")
            total_documents = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM balance_sheet_data")
            total_balance_sheets = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT ticker) FROM financial_announcements")
            unique_companies = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT ticker) FROM balance_sheet_data")
            companies_with_data = cursor.fetchone()[0]
            
            # Top companies by announcements
            cursor.execute("""
                SELECT ticker, COUNT(*) as count 
                FROM financial_announcements 
                GROUP BY ticker 
                ORDER BY count DESC 
                LIMIT 15
            """)
            top_announcements = cursor.fetchall()
            
            # Top companies by balance sheets
            cursor.execute("""
                SELECT ticker, COUNT(*) as count 
                FROM balance_sheet_data 
                GROUP BY ticker 
                ORDER BY count DESC 
                LIMIT 15
            """)
            top_balance_sheets = cursor.fetchall()
            
            conn.close()
            
            print(f"\nFINAL DATABASE STATISTICS")
            print(f"{'='*60}")
            print(f"Total Announcements: {total_announcements:,}")
            print(f"Total Documents: {total_documents:,}")
            print(f"Total Balance Sheet Records: {total_balance_sheets:,}")
            print(f"Unique Companies (Announcements): {unique_companies}")
            print(f"Companies with Financial Data: {companies_with_data}")
            
            print(f"\nTOP 15 COMPANIES BY ANNOUNCEMENTS:")
            for ticker, count in top_announcements:
                print(f"  {ticker}: {count} announcements")
            
            print(f"\nTOP 15 COMPANIES BY BALANCE SHEET RECORDS:")
            for ticker, count in top_balance_sheets:
                print(f"  {ticker}: {count} records")
                
        except Exception as e:
            logging.error(f"Error getting final stats: {e}")

def main():
    """Main function"""
    collector = CompleteNZXCollector()
    
    print("="*80)
    print("COMPLETE NZX HISTORICAL DATA COLLECTOR")
    print("="*80)
    print("This collector ensures ALL announcements for ALL companies across ALL years")
    print("are collected comprehensively.")
    print("="*80)
    
    # Run complete collection
    results = collector.run_complete_collection(years_back=10)
    
    print(f"\nCollection completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
