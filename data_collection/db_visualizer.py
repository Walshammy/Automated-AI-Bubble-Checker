#!/usr/bin/env python3
"""
DATABASE SIZE VISUALIZER
========================

Simple script to visualize database growth with progress bars and charts.
Run this repeatedly to see real-time collection progress.

Author: AI Assistant
Date: 2025-10-03
"""

import sqlite3
import os
import time
from datetime import datetime

def get_database_stats(db_path):
    """Get comprehensive database statistics"""
    if not os.path.exists(db_path):
        return None
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    stats = {}
    
    # Get file size
    file_size = os.path.getsize(db_path)
    stats['file_size_mb'] = round(file_size / (1024 * 1024), 2)
    stats['file_size_bytes'] = file_size
    
    # Get record counts by table
    tables = [
        'historical_prices', 'current_fundamentals', 'analyst_ratings',
        'earnings_history', 'corporate_actions', 'institutional_holdings',
        'extended_price_data', 'sector_performance', 'market_indicators'
    ]
    
    total_records = 0
    table_counts = {}
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            table_counts[table] = count
            total_records += count
        except:
            table_counts[table] = 0
    
    stats['total_records'] = total_records
    stats['table_counts'] = table_counts
    
    # Get unique ticker counts
    try:
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM historical_prices")
        stats['unique_tickers'] = cursor.fetchone()[0]
    except:
        stats['unique_tickers'] = 0
    
    # Get date range
    try:
        cursor.execute("SELECT MIN(date), MAX(date) FROM historical_prices")
        min_date, max_date = cursor.fetchone()
        stats['date_range'] = f"{min_date} to {max_date}"
    except:
        stats['date_range'] = "N/A"
    
    conn.close()
    return stats

def create_progress_bar(current, total, width=50):
    """Create a visual progress bar"""
    if total == 0:
        return "[" + " " * width + "] 0%"
    
    percentage = (current / total) * 100
    filled = int((current / total) * width)
    bar = "=" * filled + "-" * (width - filled)
    return f"[{bar}] {percentage:.1f}%"

def create_size_bar(current_mb, max_mb=500, width=30):
    """Create a size visualization bar"""
    if max_mb == 0:
        return "[" + " " * width + "] 0 MB"
    
    percentage = min((current_mb / max_mb) * 100, 100)
    filled = int((current_mb / max_mb) * width)
    bar = "=" * filled + "-" * (width - filled)
    return f"[{bar}] {current_mb:.1f} MB"

def visualize_database():
    """Main visualization function"""
    # Clear screen (works on Windows)
    os.system('cls' if os.name == 'nt' else 'clear')
    
    print("=" * 80)
    print("DATABASE SIZE VISUALIZER")
    print("=" * 80)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Check both databases
    main_db = "data_collection/unified_stock_data.db"
    backup_db = r"C:\Users\james\Downloads\Stock Valuation\unified_stock_data.db"
    
    main_stats = get_database_stats(main_db)
    backup_stats = get_database_stats(backup_db)
    
    if not main_stats:
        print("ERROR: Main database not found!")
        return
    
    # File size visualization
    print("DATABASE SIZE")
    print("-" * 40)
    print(f"Main DB:   {create_size_bar(main_stats['file_size_mb'])}")
    if backup_stats:
        print(f"Backup DB: {create_size_bar(backup_stats['file_size_mb'])}")
    print()
    
    # Stock universe progress
    print("STOCK UNIVERSE PROGRESS")
    print("-" * 40)
    target_stocks = 3270
    current_stocks = main_stats['unique_tickers']
    print(f"Target:    {target_stocks:,} stocks")
    print(f"Current:   {current_stocks:,} stocks")
    print(f"Progress:  {create_progress_bar(current_stocks, target_stocks)}")
    print()
    
    # Record counts visualization
    print("RECORD COUNTS BY TABLE")
    print("-" * 40)
    
    table_names = {
        'historical_prices': 'Historical Prices',
        'current_fundamentals': 'Fundamentals', 
        'analyst_ratings': 'Analyst Ratings',
        'earnings_history': 'Earnings History',
        'corporate_actions': 'Corporate Actions',
        'institutional_holdings': 'Institutional Holdings',
        'extended_price_data': 'Extended Price Data',
        'sector_performance': 'Sector Performance',
        'market_indicators': 'Market Indicators'
    }
    
    max_records = max(main_stats['table_counts'].values()) if main_stats['table_counts'] else 1
    
    for table, display_name in table_names.items():
        count = main_stats['table_counts'].get(table, 0)
        if count > 0:
            bar = create_progress_bar(count, max_records, 20)
            print(f"{display_name:<25} {bar} {count:,}")
    
    print()
    
    # Summary stats
    print("SUMMARY STATISTICS")
    print("-" * 40)
    print(f"Total Records:     {main_stats['total_records']:,}")
    print(f"Unique Stocks:     {main_stats['unique_tickers']:,}")
    print(f"Date Range:        {main_stats['date_range']}")
    print(f"Database Size:     {main_stats['file_size_mb']:.1f} MB")
    print()
    
    # Collection status
    completion_pct = (current_stocks / target_stocks) * 100
    if completion_pct >= 100:
        status = "COMPLETE"
        color = "[GREEN]"
    elif completion_pct >= 75:
        status = "NEARLY COMPLETE"
        color = "[YELLOW]"
    elif completion_pct >= 50:
        status = "IN PROGRESS"
        color = "[ORANGE]"
    else:
        status = "STARTING"
        color = "[RED]"
    
    print(f"{color} COLLECTION STATUS: {status}")
    print(f"   Progress: {completion_pct:.1f}% ({current_stocks:,}/{target_stocks:,} stocks)")
    print()
    
    # Instructions
    print("INSTRUCTIONS")
    print("-" * 40)
    print("* Run this script repeatedly to see real-time progress")
    print("* Press Ctrl+C to stop")
    print("* Database updates every few seconds during collection")
    print()
    print("Refreshing in 5 seconds... (Press Ctrl+C to stop)")

def main():
    """Main function with auto-refresh"""
    try:
        while True:
            visualize_database()
            time.sleep(5)  # Refresh every 5 seconds
    except KeyboardInterrupt:
        print("\n\nVisualization stopped!")
        print("Run 'python data_collection\\db_visualizer.py' to restart")

if __name__ == "__main__":
    main()
