#!/usr/bin/env python3
"""
NZX Data Collection Monitor
===========================

Real-time monitoring of the comprehensive NZX data collection process.
Provides status updates and progress tracking.

Author: AI Assistant
Date: 2025-10-05
"""

import sqlite3
import time
import os
from datetime import datetime
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class NZXCollectionMonitor:
    """Monitor NZX data collection progress"""
    
    def __init__(self):
        self.db_path = "../data_collection/unified_stock_data.db"
        self.output_dir = Path("./comprehensive_nzx_data")
        self.start_time = datetime.now()
        
    def get_database_stats(self):
        """Get current database statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            stats = {}
            
            # Count announcements
            cursor.execute("SELECT COUNT(*) FROM financial_announcements")
            stats['announcements'] = cursor.fetchone()[0]
            
            # Count documents
            cursor.execute("SELECT COUNT(*) FROM financial_documents")
            stats['documents'] = cursor.fetchone()[0]
            
            # Count balance sheet records
            cursor.execute("SELECT COUNT(*) FROM balance_sheet_data")
            stats['balance_sheets'] = cursor.fetchone()[0]
            
            # Count unique companies
            cursor.execute("SELECT COUNT(DISTINCT ticker) FROM financial_announcements")
            stats['companies'] = cursor.fetchone()[0]
            
            # Get latest scraped time
            cursor.execute("SELECT MAX(scraped_at) FROM financial_announcements")
            stats['last_scraped'] = cursor.fetchone()[0]
            
            conn.close()
            return stats
            
        except Exception as e:
            logging.error(f"Error getting database stats: {e}")
            return {}
    
    def get_file_stats(self):
        """Get file system statistics"""
        stats = {}
        
        # Count PDF files
        pdf_count = 0
        pdf_size = 0
        if self.output_dir.exists():
            for pdf_file in self.output_dir.rglob("*.pdf"):
                if pdf_file.is_file():
                    pdf_count += 1
                    pdf_size += pdf_file.stat().st_size
        
        stats['pdf_files'] = pdf_count
        stats['pdf_size_mb'] = pdf_size / (1024 * 1024)
        
        # Count CSV files
        csv_count = 0
        if self.output_dir.exists():
            for csv_file in self.output_dir.rglob("*.csv"):
                if csv_file.is_file():
                    csv_count += 1
        
        stats['csv_files'] = csv_count
        
        # Count Excel files
        excel_count = 0
        if self.output_dir.exists():
            for excel_file in self.output_dir.rglob("*.xlsx"):
                if excel_file.is_file():
                    excel_count += 1
        
        stats['excel_files'] = excel_count
        
        return stats
    
    def print_status(self):
        """Print current status"""
        db_stats = self.get_database_stats()
        file_stats = self.get_file_stats()
        
        elapsed = datetime.now() - self.start_time
        
        print(f"\n{'='*80}")
        print(f"NZX COMPREHENSIVE DATA COLLECTION STATUS")
        print(f"{'='*80}")
        print(f"⏱️  Elapsed Time: {elapsed}")
        print(f"📁 Output Directory: {self.output_dir}")
        
        print(f"\n📊 DATABASE STATISTICS:")
        print(f"   📰 Financial Announcements: {db_stats.get('announcements', 0):,}")
        print(f"   📄 PDF Documents: {db_stats.get('documents', 0):,}")
        print(f"   📈 Balance Sheet Records: {db_stats.get('balance_sheets', 0):,}")
        print(f"   🏢 Unique Companies: {db_stats.get('companies', 0):,}")
        print(f"   🕒 Last Scraped: {db_stats.get('last_scraped', 'Never')}")
        
        print(f"\n💾 FILE SYSTEM STATISTICS:")
        print(f"   📄 PDF Files: {file_stats.get('pdf_files', 0):,}")
        print(f"   📊 CSV Files: {file_stats.get('csv_files', 0):,}")
        print(f"   📈 Excel Files: {file_stats.get('excel_files', 0):,}")
        print(f"   💾 Total PDF Size: {file_stats.get('pdf_size_mb', 0):.1f} MB")
        
        # Calculate progress
        total_companies = 70  # Approximate total NZX companies
        processed_companies = db_stats.get('companies', 0)
        progress_pct = (processed_companies / total_companies) * 100
        
        print(f"\n🎯 PROGRESS:")
        print(f"   📊 Companies Processed: {processed_companies}/{total_companies} ({progress_pct:.1f}%)")
        
        if db_stats.get('balance_sheets', 0) > 0:
            print(f"   ✅ Balance Sheet Data: EXTRACTED")
        else:
            print(f"   ⚠️  Balance Sheet Data: PENDING")
        
        print(f"{'='*80}")
    
    def monitor_continuously(self, interval=60):
        """Monitor continuously with specified interval"""
        print("🚀 Starting NZX Data Collection Monitor")
        print("Press Ctrl+C to stop monitoring")
        
        try:
            while True:
                self.print_status()
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n⏹️  Monitoring stopped by user")
            self.print_status()

def main():
    """Main function"""
    monitor = NZXCollectionMonitor()
    monitor.monitor_continuously(interval=30)  # Update every 30 seconds

if __name__ == "__main__":
    main()
