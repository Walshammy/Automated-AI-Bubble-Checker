#!/usr/bin/env python3
"""
Database Organization and Consolidation Script
Consolidates ASX and NZX data into unified database structure
"""
import sqlite3
import os
import shutil
from datetime import datetime
from pathlib import Path

class DatabaseOrganizer:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.data_dir = self.base_dir / 'consolidated_data'
        self.data_dir.mkdir(exist_ok=True)
        
        # Database paths
        self.asx_db = self.base_dir / 'asx_scraper' / 'asx_data' / 'asx_announcements.db'
        self.nzx_db = self.base_dir / 'Balance_Sheet_Scraper' / 'balance_sheet_data' / 'nzx_financial_data.db'
        self.unified_db = self.data_dir / 'unified_financial_data.db'
        
        # PDF directories
        self.asx_pdfs = self.base_dir / 'asx_scraper' / 'asx_data' / 'pdfs'
        self.nzx_pdfs = self.base_dir / 'Balance_Sheet_Scraper' / 'balance_sheet_data' / 'pdfs'
        self.unified_pdfs = self.data_dir / 'pdfs'
        self.unified_pdfs.mkdir(exist_ok=True)

    def create_unified_schema(self):
        """Create unified database schema"""
        conn = sqlite3.connect(self.unified_db)
        cursor = conn.cursor()
        
        # Create unified announcements table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS financial_announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                announcement_id TEXT UNIQUE NOT NULL,
                ticker TEXT NOT NULL,
                company_name TEXT,
                announcement_date TIMESTAMP,
                title TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL,
                pdf_filename TEXT,
                file_size TEXT,
                market_sensitive BOOLEAN,
                is_financial_report BOOLEAN,
                is_balance_sheet BOOLEAN DEFAULT FALSE,
                download_status TEXT DEFAULT 'pending',
                exchange TEXT NOT NULL,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed BOOLEAN DEFAULT FALSE,
                UNIQUE(ticker, url, exchange)
            )
        ''')
        
        # Create company metadata table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS company_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                company_name TEXT,
                exchange TEXT NOT NULL,
                market_cap REAL,
                sector TEXT,
                industry TEXT,
                first_listed DATE,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, exchange)
            )
        ''')
        
        # Create data collection stats table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collection_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange TEXT NOT NULL,
                total_announcements INTEGER,
                financial_reports INTEGER,
                balance_sheet_reports INTEGER,
                downloaded_pdfs INTEGER,
                companies_covered INTEGER,
                last_collection TIMESTAMP,
                collection_version TEXT,
                UNIQUE(exchange)
            )
        ''')
        
        conn.commit()
        conn.close()
        print("Unified database schema created")

    def migrate_asx_data(self):
        """Migrate ASX data to unified database"""
        if not self.asx_db.exists():
            print("ASX database not found, skipping migration")
            return
        
        print("Migrating ASX data...")
        
        # Connect to source and destination
        asx_conn = sqlite3.connect(self.asx_db)
        unified_conn = sqlite3.connect(self.unified_db)
        
        asx_cursor = asx_conn.cursor()
        unified_cursor = unified_conn.cursor()
        
        try:
            # Get all ASX announcements
            asx_cursor.execute('SELECT * FROM asx_announcements')
            asx_data = asx_cursor.fetchall()
            
            # Get column names
            asx_cursor.execute('PRAGMA table_info(asx_announcements)')
            columns = [col[1] for col in asx_cursor.fetchall()]
            
            migrated_count = 0
            for row in asx_data:
                # Convert to dictionary
                row_dict = dict(zip(columns, row))
                
                # Insert into unified database
                unified_cursor.execute('''
                    INSERT OR IGNORE INTO financial_announcements 
                    (announcement_id, ticker, company_name, announcement_date, title, url, 
                     pdf_filename, file_size, market_sensitive, is_financial_report, 
                     is_balance_sheet, download_status, exchange, scraped_at, processed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row_dict.get('announcement_id'),
                    row_dict.get('ticker'),
                    row_dict.get('company_name'),
                    row_dict.get('announcement_date'),
                    row_dict.get('title'),
                    row_dict.get('url'),
                    row_dict.get('pdf_filename'),
                    row_dict.get('file_size'),
                    row_dict.get('market_sensitive'),
                    row_dict.get('is_financial_report'),
                    row_dict.get('is_balance_sheet'),
                    row_dict.get('download_status'),
                    'ASX',
                    row_dict.get('scraped_at'),
                    row_dict.get('processed')
                ))
                
                if unified_cursor.rowcount > 0:
                    migrated_count += 1
            
            unified_conn.commit()
            print(f"Migrated {migrated_count} ASX announcements")
            
        except Exception as e:
            print(f"Error migrating ASX data: {e}")
        finally:
            asx_conn.close()
            unified_conn.close()

    def migrate_nzx_data(self):
        """Migrate NZX data to unified database"""
        if not self.nzx_db.exists():
            print("NZX database not found, skipping migration")
            return
        
        print("Migrating NZX data...")
        
        # Connect to source and destination
        nzx_conn = sqlite3.connect(self.nzx_db)
        unified_conn = sqlite3.connect(self.unified_db)
        
        nzx_cursor = nzx_conn.cursor()
        unified_cursor = unified_conn.cursor()
        
        try:
            # Get all NZX announcements
            nzx_cursor.execute('SELECT * FROM nzx_announcements')
            nzx_data = nzx_cursor.fetchall()
            
            # Get column names
            nzx_cursor.execute('PRAGMA table_info(nzx_announcements)')
            columns = [col[1] for col in nzx_cursor.fetchall()]
            
            migrated_count = 0
            for row in nzx_data:
                # Convert to dictionary
                row_dict = dict(zip(columns, row))
                
                # Insert into unified database
                unified_cursor.execute('''
                    INSERT OR IGNORE INTO financial_announcements 
                    (announcement_id, ticker, company_name, announcement_date, title, url, 
                     pdf_filename, file_size, market_sensitive, is_financial_report, 
                     is_balance_sheet, download_status, exchange, scraped_at, processed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row_dict.get('announcement_id'),
                    row_dict.get('ticker'),
                    row_dict.get('company_name'),
                    row_dict.get('announcement_date'),
                    row_dict.get('title'),
                    row_dict.get('url'),
                    row_dict.get('pdf_filename'),
                    row_dict.get('file_size'),
                    row_dict.get('market_sensitive'),
                    row_dict.get('is_financial_report'),
                    row_dict.get('is_balance_sheet'),
                    row_dict.get('download_status'),
                    'NZX',
                    row_dict.get('scraped_at'),
                    row_dict.get('processed')
                ))
                
                if unified_cursor.rowcount > 0:
                    migrated_count += 1
            
            unified_conn.commit()
            print(f"Migrated {migrated_count} NZX announcements")
            
        except Exception as e:
            print(f"Error migrating NZX data: {e}")
        finally:
            nzx_conn.close()
            unified_conn.close()

    def consolidate_pdfs(self):
        """Consolidate PDF files from both exchanges"""
        print("Consolidating PDF files...")
        
        # Copy ASX PDFs
        if self.asx_pdfs.exists():
            asx_dest = self.unified_pdfs / 'ASX'
            asx_dest.mkdir(exist_ok=True)
            
            for company_dir in self.asx_pdfs.iterdir():
                if company_dir.is_dir():
                    dest_dir = asx_dest / company_dir.name
                    dest_dir.mkdir(exist_ok=True)
                    
                    for pdf_file in company_dir.glob('*.pdf'):
                        dest_file = dest_dir / pdf_file.name
                        if not dest_file.exists():
                            shutil.copy2(pdf_file, dest_file)
            
            print("ASX PDFs consolidated")
        
        # Copy NZX PDFs
        if self.nzx_pdfs.exists():
            nzx_dest = self.unified_pdfs / 'NZX'
            nzx_dest.mkdir(exist_ok=True)
            
            for company_dir in self.nzx_pdfs.iterdir():
                if company_dir.is_dir():
                    dest_dir = nzx_dest / company_dir.name
                    dest_dir.mkdir(exist_ok=True)
                    
                    for pdf_file in company_dir.glob('*.pdf'):
                        dest_file = dest_dir / pdf_file.name
                        if not dest_file.exists():
                            shutil.copy2(pdf_file, dest_file)
            
            print("NZX PDFs consolidated")

    def update_collection_stats(self):
        """Update collection statistics"""
        conn = sqlite3.connect(self.unified_db)
        cursor = conn.cursor()
        
        try:
            # ASX stats
            cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE exchange = "ASX"')
            asx_total = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE exchange = "ASX" AND is_financial_report = 1')
            asx_financial = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE exchange = "ASX" AND is_balance_sheet = 1')
            asx_balance_sheets = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE exchange = "ASX" AND download_status = "downloaded"')
            asx_downloaded = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM financial_announcements WHERE exchange = "ASX"')
            asx_companies = cursor.fetchone()[0]
            
            # NZX stats
            cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE exchange = "NZX"')
            nzx_total = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE exchange = "NZX" AND is_financial_report = 1')
            nzx_financial = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE exchange = "NZX" AND is_balance_sheet = 1')
            nzx_balance_sheets = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE exchange = "NZX" AND download_status = "downloaded"')
            nzx_downloaded = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM financial_announcements WHERE exchange = "NZX"')
            nzx_companies = cursor.fetchone()[0]
            
            # Insert/update stats
            cursor.execute('''
                INSERT OR REPLACE INTO collection_stats 
                (exchange, total_announcements, financial_reports, balance_sheet_reports, 
                 downloaded_pdfs, companies_covered, last_collection, collection_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', ('ASX', asx_total, asx_financial, asx_balance_sheets, asx_downloaded, asx_companies, 
                  datetime.now().isoformat(), '1.0'))
            
            cursor.execute('''
                INSERT OR REPLACE INTO collection_stats 
                (exchange, total_announcements, financial_reports, balance_sheet_reports, 
                 downloaded_pdfs, companies_covered, last_collection, collection_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', ('NZX', nzx_total, nzx_financial, nzx_balance_sheets, nzx_downloaded, nzx_companies, 
                  datetime.now().isoformat(), '1.0'))
            
            conn.commit()
            print("Collection statistics updated")
            
        except Exception as e:
            print(f"Error updating stats: {e}")
        finally:
            conn.close()

    def generate_summary_report(self):
        """Generate comprehensive summary report"""
        conn = sqlite3.connect(self.unified_db)
        cursor = conn.cursor()
        
        print("\n" + "="*80)
        print("UNIFIED FINANCIAL DATABASE SUMMARY")
        print("="*80)
        
        try:
            # Overall stats
            cursor.execute('SELECT COUNT(*) FROM financial_announcements')
            total_announcements = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE is_financial_report = 1')
            total_financial = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE is_balance_sheet = 1')
            total_balance_sheets = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE download_status = "downloaded"')
            total_downloaded = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM financial_announcements')
            total_companies = cursor.fetchone()[0]
            
            print(f"Total Announcements: {total_announcements}")
            print(f"Financial Reports: {total_financial}")
            print(f"Balance Sheet Reports: {total_balance_sheets}")
            print(f"Downloaded PDFs: {total_downloaded}")
            print(f"Companies Covered: {total_companies}")
            
            # By exchange
            print(f"\nBy Exchange:")
            cursor.execute('''
                SELECT exchange, COUNT(*) as total, 
                       SUM(CASE WHEN is_financial_report = 1 THEN 1 ELSE 0 END) as financial,
                       SUM(CASE WHEN is_balance_sheet = 1 THEN 1 ELSE 0 END) as balance_sheets,
                       SUM(CASE WHEN download_status = "downloaded" THEN 1 ELSE 0 END) as downloaded,
                       COUNT(DISTINCT ticker) as companies
                FROM financial_announcements 
                GROUP BY exchange
            ''')
            
            for row in cursor.fetchall():
                exchange, total, financial, balance_sheets, downloaded, companies = row
                print(f"  {exchange}: {total} total ({financial} financial, {balance_sheets} balance sheets, {downloaded} downloaded, {companies} companies)")
            
            # Top companies by balance sheet reports
            print(f"\nTop Companies by Balance Sheet Reports:")
            cursor.execute('''
                SELECT ticker, exchange, COUNT(*) as balance_sheet_count
                FROM financial_announcements 
                WHERE is_balance_sheet = 1
                GROUP BY ticker, exchange
                ORDER BY balance_sheet_count DESC
                LIMIT 10
            ''')
            
            for row in cursor.fetchall():
                ticker, exchange, count = row
                print(f"  {ticker} ({exchange}): {count} balance sheet reports")
            
        except Exception as e:
            print(f"Error generating summary: {e}")
        finally:
            conn.close()

    def organize_all(self):
        """Run complete organization process"""
        print("Starting database organization and consolidation...")
        
        self.create_unified_schema()
        self.migrate_asx_data()
        self.migrate_nzx_data()
        self.consolidate_pdfs()
        self.update_collection_stats()
        self.generate_summary_report()
        
        print(f"\nOrganization complete!")
        print(f"Unified database: {self.unified_db}")
        print(f"Unified PDFs: {self.unified_pdfs}")

if __name__ == "__main__":
    organizer = DatabaseOrganizer()
    organizer.organize_all()
