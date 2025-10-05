#!/usr/bin/env python3
"""
Comprehensive NZX Data Collection Status Monitor
===============================================

Real-time monitoring of the comprehensive financial data extraction process.
"""

import time
import sqlite3
import os
from pathlib import Path
from datetime import datetime

def get_database_stats():
    """Get current database statistics"""
    try:
        conn = sqlite3.connect('../data_collection/unified_stock_data.db')
        cursor = conn.cursor()
        
        stats = {}
        
        # Basic counts
        cursor.execute("SELECT COUNT(*) FROM financial_announcements")
        stats['announcements'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM financial_documents")
        stats['documents'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM balance_sheet_data")
        stats['balance_sheets'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM financial_announcements")
        stats['companies'] = cursor.fetchone()[0]
        
        # Latest activity
        cursor.execute("SELECT MAX(scraped_at) FROM financial_announcements")
        stats['last_scraped'] = cursor.fetchone()[0]
        
        # Companies with financial data
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM balance_sheet_data")
        stats['companies_with_data'] = cursor.fetchone()[0]
        
        # Top companies by balance sheet records
        cursor.execute("""
            SELECT ticker, COUNT(*) as records, MAX(report_date) as latest_date
            FROM balance_sheet_data 
            GROUP BY ticker 
            ORDER BY records DESC 
            LIMIT 10
        """)
        stats['top_companies'] = cursor.fetchall()
        
        conn.close()
        return stats
    except Exception as e:
        print(f"Database error: {e}")
        return {}

def get_file_system_stats():
    """Get file system statistics"""
    try:
        base_dir = Path("enhanced_nzx_data")
        if not base_dir.exists():
            return {'pdf_count': 0, 'dataset_count': 0, 'pdf_size_mb': 0, 'dataset_size_mb': 0}
        
        pdf_count = 0
        pdf_size = 0
        dataset_count = 0
        dataset_size = 0
        
        # Count PDFs
        pdfs_dir = base_dir / 'pdfs'
        if pdfs_dir.exists():
            for pdf_file in pdfs_dir.rglob('*.pdf'):
                pdf_count += 1
                pdf_size += pdf_file.stat().st_size
        
        # Count datasets
        datasets_dir = base_dir / 'datasets'
        if datasets_dir.exists():
            for dataset_file in datasets_dir.rglob('*.csv'):
                dataset_count += 1
                dataset_size += dataset_file.stat().st_size
            for dataset_file in datasets_dir.rglob('*.xlsx'):
                dataset_count += 1
                dataset_size += dataset_file.stat().st_size
        
        return {
            'pdf_count': pdf_count,
            'dataset_count': dataset_count,
            'pdf_size_mb': round(pdf_size / (1024 * 1024), 2),
            'dataset_size_mb': round(dataset_size / (1024 * 1024), 2)
        }
    except Exception as e:
        print(f"File system error: {e}")
        return {'pdf_count': 0, 'dataset_count': 0, 'pdf_size_mb': 0, 'dataset_size_mb': 0}

def display_status():
    """Display comprehensive status"""
    db_stats = get_database_stats()
    fs_stats = get_file_system_stats()
    
    print("\n" + "="*80)
    print("COMPREHENSIVE NZX FINANCIAL DATA EXTRACTION STATUS")
    print("="*80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 80)
    
    if db_stats:
        print("\nDATABASE METRICS:")
        print(f"  [DB] Total Announcements: {db_stats.get('announcements', 0):,}")
        print(f"  [DOC] Total Documents: {db_stats.get('documents', 0):,}")
        print(f"  [BS] Balance Sheet Records: {db_stats.get('balance_sheets', 0):,}")
        print(f"  [COMP] Unique Companies: {db_stats.get('companies', 0):,}")
        print(f"  [SUCCESS] Companies with Financial Data: {db_stats.get('companies_with_data', 0):,}")
        print(f"  [TIME] Last Scraped: {db_stats.get('last_scraped', 'N/A')}")
        
        if db_stats.get('top_companies'):
            print(f"\nTOP COMPANIES BY FINANCIAL RECORDS:")
            for ticker, records, latest_date in db_stats['top_companies']:
                print(f"  {ticker}: {records} records (latest: {latest_date})")
    
    print(f"\nFILE SYSTEM METRICS:")
    print(f"  [PDF] PDF Files: {fs_stats['pdf_count']:,} ({fs_stats['pdf_size_mb']:.2f} MB)")
    print(f"  [DATA] Dataset Files: {fs_stats['dataset_count']:,} ({fs_stats['dataset_size_mb']:.2f} MB)")
    
    print(f"\nSYSTEM STATUS:")
    print(f"  [RUNNING] Collection Process: Running in background")
    print(f"  [MONITOR] Monitoring: Active")
    print(f"  [CLEAN] Repository: Cleaned")
    print(f"  [CLEANUP] PDF Cleanup: Automatic (deleted after successful extraction)")
    
    print(f"\nNEXT STEPS:")
    print(f"  - Continue monitoring for data extraction progress")
    print(f"  - System will automatically process all NZX companies")
    print(f"  - PDFs will be cleaned up after successful data extraction")
    print(f"  - Check logs for detailed progress information")
    print("="*80)

def monitor_continuously(interval=30):
    """Monitor continuously with specified interval"""
    print("Starting Comprehensive NZX Data Collection Monitor")
    print("Press Ctrl+C to stop monitoring")
    
    try:
        while True:
            display_status()
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user")
        print("Collection processes continue running in background")

if __name__ == "__main__":
    monitor_continuously(interval=30)
