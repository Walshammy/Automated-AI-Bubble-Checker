#!/usr/bin/env python3
"""
Simple Database Review Script
Shows database size, record counts, and basic statistics
"""

import sqlite3
import os
import json
from datetime import datetime

def review_database():
    """Review database size and statistics"""
    
    # Database path
    db_path = 'data_collection/improved_data.db'
    
    if not os.path.exists(db_path):
        print("Database file not found!")
        return
    
    # Get file size
    file_size_mb = os.path.getsize(db_path) / (1024 * 1024)
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 60)
    print("DATABASE REVIEW")
    print("=" * 60)
    
    # Historical prices
    cursor.execute('SELECT COUNT(*) FROM historical_prices')
    price_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT ticker) FROM historical_prices')
    price_tickers = cursor.fetchone()[0]
    
    cursor.execute('SELECT MIN(date), MAX(date) FROM historical_prices')
    price_range = cursor.fetchone()
    
    # Current fundamentals
    cursor.execute('SELECT COUNT(*) FROM current_fundamentals')
    fund_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT ticker) FROM current_fundamentals')
    fund_tickers = cursor.fetchone()[0]
    
    # Exchange breakdown
    cursor.execute('''
        SELECT exchange, COUNT(*) as records, COUNT(DISTINCT ticker) as tickers
        FROM historical_prices 
        GROUP BY exchange 
        ORDER BY records DESC
    ''')
    exchange_stats = cursor.fetchall()
    
    conn.close()
    
    # Progress file
    progress_file = 'data_collection/improved_collection_progress.json'
    progress_info = ""
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            progress = json.load(f)
        
        completed = len(progress.get("completed_tickers", []))
        failed = len(progress.get("failed_tickers", []))
        pending = len(progress.get("pending_tickers", [])) - completed - failed
        total_stocks = completed + failed + pending
        
        if total_stocks > 0:
            completion_pct = (completed / total_stocks) * 100
            progress_info = f"Progress: {completed}/{total_stocks} ({completion_pct:.1f}%) | Failed: {failed}"
    
    # Display results
    print(f"Database File:")
    print(f"   Path: {db_path}")
    print(f"   Size: {file_size_mb:.1f} MB")
    print(f"   Last Modified: {datetime.fromtimestamp(os.path.getmtime(db_path)).strftime('%Y-%m-%d %H:%M:%S')}")
    
    print(f"\nHistorical Prices:")
    print(f"   Records: {price_count:,}")
    print(f"   Stocks: {price_tickers}")
    print(f"   Date Range: {price_range[0]} to {price_range[1]}")
    if price_tickers > 0:
        print(f"   Avg Records/Stock: {price_count/price_tickers:.1f}")
    
    print(f"\nCurrent Fundamentals:")
    print(f"   Records: {fund_count:,}")
    print(f"   Stocks: {fund_tickers}")
    
    print(f"\nExchange Breakdown:")
    for exchange, records, tickers in exchange_stats:
        print(f"   {exchange}: {records:,} records ({tickers} stocks)")
    
    if progress_info:
        print(f"\nCollection Progress:")
        print(f"   {progress_info}")
    
    print(f"\n" + "=" * 60)
    print(f"SUMMARY")
    print(f"=" * 60)
    print(f"+ Database: {file_size_mb:.1f} MB")
    print(f"+ Price Records: {price_count:,}")
    print(f"+ Fundamental Records: {fund_count:,}")
    print(f"+ Total Stocks: {price_tickers}")
    print(f"+ Date Coverage: {price_range[0]} to {price_range[1]}")
    print(f"=" * 60)

if __name__ == "__main__":
    review_database()
