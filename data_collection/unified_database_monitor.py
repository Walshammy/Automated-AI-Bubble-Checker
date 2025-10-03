#!/usr/bin/env python3
"""
UNIFIED DATABASE MONITOR
========================

Monitors the unified stock data database with all data types in one place.

Author: AI Assistant
Date: 2025-10-03
"""

import sqlite3
import os
from datetime import datetime
import json

def monitor_unified_database():
    """Monitor the unified database status and statistics"""
    
    # Database paths
    unified_db = "data_collection/unified_stock_data.db"
    backup_db = r"C:\Users\james\Downloads\Stock Valuation\unified_stock_data.db"
    
    print("=" * 100)
    print("UNIFIED STOCK DATA DATABASE MONITOR")
    print("=" * 100)
    print(f"Monitoring Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)
    
    # Check both databases
    databases = [
        ("Local Database", unified_db),
        ("Backup Database", backup_db)
    ]
    
    for db_name, db_path in databases:
        print(f"\n{db_name.upper()}")
        print("-" * 80)
        
        if os.path.exists(db_path):
            file_size_mb = os.path.getsize(db_path) / (1024 * 1024)
            last_modified = datetime.fromtimestamp(os.path.getmtime(db_path))
            
            print(f"+ Database exists: {db_path}")
            print(f"+ File size: {file_size_mb:.1f} MB")
            print(f"+ Last modified: {last_modified.strftime('%Y-%m-%d %H:%M:%S')}")
            
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Get all table statistics
                tables = [
                    ('historical_prices', 'Historical Prices'),
                    ('current_fundamentals', 'Current Fundamentals'),
                    ('analyst_ratings', 'Analyst Ratings'),
                    ('earnings_history', 'Earnings History'),
                    ('corporate_actions', 'Corporate Actions'),
                    ('institutional_holdings', 'Institutional Holdings'),
                    ('extended_price_data', 'Extended Price Data'),
                    ('sector_performance', 'Sector Performance'),
                    ('market_indicators', 'Market Indicators')
                ]
                
                total_records = 0
                total_stocks = set()
                
                print(f"\nTABLE BREAKDOWN:")
                for table_name, display_name in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT ticker) FROM {table_name}")
                        count, unique_stocks = cursor.fetchone()
                        total_records += count
                        
                        if unique_stocks > 0:
                            print(f"   {display_name}: {count:,} records ({unique_stocks} stocks)")
                            
                            # Add stocks to total set
                            cursor.execute(f"SELECT DISTINCT ticker FROM {table_name}")
                            stocks = [row[0] for row in cursor.fetchall()]
                            total_stocks.update(stocks)
                        else:
                            print(f"   {display_name}: {count:,} records")
                            
                    except Exception as e:
                        print(f"   {display_name}: Error - {e}")
                
                print(f"\nSUMMARY:")
                print(f"   Total Records: {total_records:,}")
                print(f"   Unique Stocks: {len(total_stocks)}")
                
                # Get date ranges for historical prices
                try:
                    cursor.execute("SELECT MIN(date), MAX(date) FROM historical_prices")
                    min_date, max_date = cursor.fetchone()
                    if min_date and max_date:
                        print(f"   Date Range: {min_date} to {max_date}")
                except:
                    pass
                
                # Get exchange breakdown
                try:
                    cursor.execute("SELECT exchange, COUNT(*), COUNT(DISTINCT ticker) FROM historical_prices GROUP BY exchange")
                    exchange_stats = cursor.fetchall()
                    if exchange_stats:
                        print(f"\nEXCHANGE BREAKDOWN:")
                        for exchange, records, tickers in exchange_stats:
                            print(f"   {exchange}: {records:,} records ({tickers} stocks)")
                except:
                    pass
                
                conn.close()
                
            except Exception as e:
                print(f"- Error reading database: {e}")
        else:
            print(f"- Database not found: {db_path}")
    
    # Summary
    print(f"\n" + "=" * 100)
    print(f"UNIFIED DATABASE SUMMARY")
    print(f"=" * 100)
    
    local_exists = os.path.exists(unified_db)
    backup_exists = os.path.exists(backup_db)
    
    if local_exists:
        local_size = os.path.getsize(unified_db) / (1024 * 1024)
        print(f"+ Local Database: {local_size:.1f} MB")
    else:
        print(f"- Local Database: Not found")
    
    if backup_exists:
        backup_size = os.path.getsize(backup_db) / (1024 * 1024)
        print(f"+ Backup Database: {backup_size:.1f} MB")
    else:
        print(f"- Backup Database: Not found")
    
    print(f"+ Monitoring Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"=" * 100)

if __name__ == "__main__":
    monitor_unified_database()
