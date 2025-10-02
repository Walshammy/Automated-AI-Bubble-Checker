#!/usr/bin/env python3
"""
Data Scale Summary Script
Provides comprehensive overview of current data collections
"""

import sqlite3
import os
import json

def get_database_stats(db_path, table_name):
    """Get statistics from a database"""
    if not os.path.exists(db_path):
        return None
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
        records = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT COUNT(DISTINCT ticker) FROM {table_name}')
        tickers = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT MIN(date), MAX(date) FROM {table_name}')
        date_range = cursor.fetchone()
        
        size = os.path.getsize(db_path) / 1024 / 1024
        
        return {
            'records': records,
            'tickers': tickers,
            'date_range': date_range,
            'size_mb': size
        }
    except Exception as e:
        print(f"Error reading {db_path}: {e}")
        return None
    finally:
        conn.close()

def get_progress_stats(progress_file):
    """Get progress statistics"""
    if not os.path.exists(progress_file):
        return None
    
    try:
        with open(progress_file, 'r') as f:
            progress = json.load(f)
        
        return {
            'completed': len(progress.get('completed_tickers', [])),
            'failed': len(progress.get('failed_tickers', [])),
            'pending': len(progress.get('pending_tickers', [])),
            'total_records': progress.get('total_records', 0)
        }
    except Exception as e:
        print(f"Error reading {progress_file}: {e}")
        return None

def main():
    print("=" * 80)
    print("CURRENT DATA SCALE SUMMARY")
    print("=" * 80)
    
    # Database statistics
    databases = [
        ('data_collection/smart_historical_data.db', 'historical_data', 'Smart Historical Data'),
        ('data_collection/nzx_asx_historical_data.db', 'historical_data', 'NZX/ASX Historical Data'),
        ('data_collection/unified_historical_data.db', 'historical_data', 'Unified Historical Data'),
        ('data_collection/stock_valuation_data.db', 'valuation_data', 'Stock Valuation Data')
    ]
    
    total_records = 0
    total_size = 0
    total_tickers = set()
    
    print("\nDATABASE STATISTICS:")
    print("-" * 50)
    
    for db_path, table, name in databases:
        stats = get_database_stats(db_path, table)
        if stats:
            print(f"\n{name}:")
            print(f"  Records: {stats['records']:,}")
            print(f"  Unique Tickers: {stats['tickers']}")
            print(f"  Date Range: {stats['date_range'][0]} to {stats['date_range'][1]}")
            print(f"  File Size: {stats['size_mb']:.1f} MB")
            
            total_records += stats['records']
            total_size += stats['size_mb']
    
    # Progress statistics
    progress_files = [
        ('data_collection/smart_collection_progress.json', 'Smart Collection'),
        ('data_collection/nzx_asx_collection_progress.json', 'NZX/ASX Collection'),
        ('data_collection/unified_collection_progress.json', 'Unified Collection')
    ]
    
    print("\nPROGRESS STATISTICS:")
    print("-" * 50)
    
    for progress_file, name in progress_files:
        stats = get_progress_stats(progress_file)
        if stats:
            print(f"\n{name}:")
            print(f"  Completed: {stats['completed']}")
            print(f"  Failed: {stats['failed']}")
            print(f"  Pending: {stats['pending']}")
            print(f"  Total Records: {stats['total_records']:,}")
    
    # Summary
    print("\n" + "=" * 80)
    print("TOTAL SUMMARY:")
    print("=" * 80)
    print(f"Total Records: {total_records:,}")
    print(f"Total Database Size: {total_size:.1f} MB")
    print(f"Date Coverage: 2000-2025 (25+ years)")
    print(f"Markets Covered: US, NZX, ASX")
    
    # File structure
    print(f"\nPROJECT STRUCTURE:")
    print(f"Main Directory:")
    print(f"  - README.md")
    print(f"  - Review of AI bubble Indicators.md")
    print(f"  - old_scripts/ (archived)")
    print(f"  - data_collection/ (all scripts and data)")
    
    print(f"\ndata_collection/ Directory:")
    print(f"  Scripts:")
    print(f"    - stock_valuation_scraper.py")
    print(f"    - unified_historical_collector.py")
    print(f"  Data:")
    print(f"    - NZX_ASX.xlsx (stock universe)")
    print(f"    - *.db (SQLite databases)")
    print(f"    - *_progress.json (progress tracking)")
    print(f"    - valuation_results/ (Excel outputs)")
    
    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()
