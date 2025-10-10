#!/usr/bin/env python3
"""
ASX Production System - Simplified
Complete production system for ASX data collection and processing
"""
import os
import logging
from datetime import datetime
from asx_database import ASXDatabase
from asx_pdf_downloader import ASXPDFDownloader
import asx_config as config

class ASXProductionSystem:
    """Complete ASX production system"""
    
    def __init__(self):
        self.db = ASXDatabase()
        self.pdf_downloader = ASXPDFDownloader()
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('asx_production_system.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def get_pending_downloads(self):
        """Get announcements pending PDF download"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT announcement_id, ticker, title, url, pdf_filename
                FROM asx_announcements 
                WHERE download_status = 'pending' 
                AND url IS NOT NULL 
                AND url != ''
                ORDER BY announcement_date DESC
            ''')
            
            return cursor.fetchall()
        except Exception as e:
            self.logger.error(f"Error getting pending downloads: {e}")
            return []
        finally:
            conn.close()
    
    def download_pending_pdfs(self):
        """Download PDFs for pending announcements"""
        pending = self.get_pending_downloads()
        
        if not pending:
            self.logger.info("No pending PDF downloads")
            return 0, 0
        
        self.logger.info(f"Starting PDF downloads for {len(pending)} announcements")
        
        successful_downloads = 0
        failed_downloads = 0
        
        for announcement_id, ticker, title, url, pdf_filename in pending:
            try:
                # Create company directory
                company_dir = os.path.join(config.PDF_DIR, ticker)
                os.makedirs(company_dir, exist_ok=True)
                
                # Download PDF
                success = self.pdf_downloader.download_pdf(
                    url, 
                    os.path.join(company_dir, pdf_filename or f"{announcement_id}.pdf"),
                    ticker
                )
                
                if success:
                    # Update database
                    self.db.update_download_status(announcement_id, 'downloaded')
                    successful_downloads += 1
                    self.logger.info(f"Downloaded: {ticker} - {title[:50]}...")
                else:
                    self.db.update_download_status(announcement_id, 'failed')
                    failed_downloads += 1
                    self.logger.warning(f"Failed: {ticker} - {title[:50]}...")
                
            except Exception as e:
                self.logger.error(f"Error downloading {ticker}: {e}")
                self.db.update_download_status(announcement_id, 'failed')
                failed_downloads += 1
        
        self.logger.info(f"PDF Downloads Complete: {successful_downloads} successful, {failed_downloads} failed")
        return successful_downloads, failed_downloads
    
    def print_system_status(self):
        """Print comprehensive system status"""
        stats = self.db.get_statistics()
        
        print("=" * 80)
        print("ASX PRODUCTION SYSTEM STATUS")
        print("=" * 80)
        
        # Database Statistics
        print(f"\nDatabase Statistics:")
        print(f"  Total Announcements: {stats.get('total_announcements', 0)}")
        print(f"  Financial Reports: {stats.get('financial_reports', 0)}")
        print(f"  Balance Sheet Reports: {stats.get('balance_sheet_reports', 0)}")
        print(f"  Downloaded PDFs: {stats.get('downloaded', 0)}")
        print(f"  Companies Covered: {stats.get('companies_covered', 0)}")
        print(f"  Last Scraped: {stats.get('last_scraped', 'Never')}")
        
        # Get recent announcements
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT ticker, title, announcement_date, download_status
                FROM asx_announcements 
                ORDER BY announcement_date DESC 
                LIMIT 5
            ''')
            recent = cursor.fetchall()
            
            if recent:
                print(f"\nRecent Announcements:")
                for ticker, title, date, status in recent:
                    print(f"  {ticker}: {title[:60]}... ({date}) [{status}]")
            
            # Company coverage
            cursor.execute('''
                SELECT ticker, COUNT(*) as announcement_count,
                       SUM(CASE WHEN is_financial_report = 1 THEN 1 ELSE 0 END) as financial_count,
                       SUM(CASE WHEN download_status = 'downloaded' THEN 1 ELSE 0 END) as downloaded_count
                FROM asx_announcements 
                GROUP BY ticker 
                ORDER BY announcement_count DESC
                LIMIT 10
            ''')
            coverage = cursor.fetchall()
            
            if coverage:
                print(f"\nTop Companies by Announcement Count:")
                for ticker, total, financial, downloaded in coverage:
                    print(f"  {ticker}: {total} total ({financial} financial, {downloaded} downloaded)")
        
        except Exception as e:
            self.logger.error(f"Error getting status details: {e}")
        finally:
            conn.close()
        
        print("=" * 80)
    
    def run_production_cycle(self):
        """Run complete production cycle"""
        self.logger.info("Starting ASX Production Cycle")
        
        # 1. Show current status
        self.print_system_status()
        
        # 2. Download pending PDFs
        self.logger.info("Starting PDF download process")
        successful, failed = self.download_pending_pdfs()
        
        # 3. Show final status
        self.logger.info("Production cycle complete")
        self.print_system_status()
        
        return {
            'successful_downloads': successful,
            'failed_downloads': failed,
            'total_processed': successful + failed
        }

def main():
    """Main execution function"""
    system = ASXProductionSystem()
    results = system.run_production_cycle()
    
    print(f"\n" + "=" * 80)
    print("ASX PRODUCTION CYCLE COMPLETE")
    print("=" * 80)
    print(f"PDF Downloads:")
    print(f"  - Successful: {results['successful_downloads']}")
    print(f"  - Failed: {results['failed_downloads']}")
    print(f"  - Total Processed: {results['total_processed']}")

if __name__ == "__main__":
    main()