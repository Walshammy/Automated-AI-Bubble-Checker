#!/usr/bin/env python3
"""
Dual Database Monitor
Monitors both improved_data.db and enhanced_data.db in Stock Valuation folder
"""

import sqlite3
import os
import json
from datetime import datetime
import time

def monitor_dual_databases():
    """Monitor both databases in Stock Valuation folder"""
    
    # Database paths in Stock Valuation folder
    improved_db = r"C:\Users\james\Downloads\Stock Valuation\improved_data.db"
    enhanced_db = r"C:\Users\james\Downloads\Stock Valuation\enhanced_data.db"
    
    print("=" * 100)
    print("DUAL DATABASE MONITOR - STOCK VALUATION FOLDER")
    print("=" * 100)
    print(f"Monitoring Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)
    
    # Monitor Improved Database
    print("\n1. IMPROVED DATABASE (Historical Prices + Fundamentals)")
    print("-" * 80)
    
    if os.path.exists(improved_db):
        file_size_mb = os.path.getsize(improved_db) / (1024 * 1024)
        last_modified = datetime.fromtimestamp(os.path.getmtime(improved_db))
        
        print(f"+ Database exists: {improved_db}")
        print(f"+ File size: {file_size_mb:.1f} MB")
        print(f"+ Last modified: {last_modified.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            conn = sqlite3.connect(improved_db)
            cursor = conn.cursor()
            
            # Historical prices stats
            cursor.execute("SELECT COUNT(*), COUNT(DISTINCT ticker), MIN(date), MAX(date) FROM historical_prices")
            price_count, price_tickers, min_price_date, max_price_date = cursor.fetchone()
            
            # Current fundamentals stats
            cursor.execute("SELECT COUNT(*), COUNT(DISTINCT ticker) FROM current_fundamentals")
            fund_count, fund_tickers = cursor.fetchone()
            
            # Exchange breakdown
            cursor.execute("SELECT exchange, COUNT(*), COUNT(DISTINCT ticker) FROM historical_prices GROUP BY exchange")
            exchange_stats = cursor.fetchall()
            
            print(f"\nHISTORICAL PRICES:")
            print(f"   Records: {price_count:,}")
            print(f"   Stocks: {price_tickers}")
            print(f"   Date Range: {min_price_date} to {max_price_date}")
            if price_tickers > 0:
                print(f"   Avg Records/Stock: {price_count/price_tickers:.1f}")
            
            print(f"\nCURRENT FUNDAMENTALS:")
            print(f"   Records: {fund_count:,}")
            print(f"   Stocks: {fund_tickers}")
            
            print(f"\nEXCHANGE BREAKDOWN:")
            for exchange, records, tickers in exchange_stats:
                print(f"   {exchange}: {records:,} records ({tickers} stocks)")
            
            conn.close()
            
        except Exception as e:
            print(f"- Error reading improved database: {e}")
    else:
        print(f"- Database not found: {improved_db}")
    
    # Monitor Enhanced Database
    print(f"\n2. ENHANCED DATABASE (Additional Free Data)")
    print("-" * 80)
    
    if os.path.exists(enhanced_db):
        file_size_mb = os.path.getsize(enhanced_db) / (1024 * 1024)
        last_modified = datetime.fromtimestamp(os.path.getmtime(enhanced_db))
        
        print(f"+ Database exists: {enhanced_db}")
        print(f"+ File size: {file_size_mb:.1f} MB")
        print(f"+ Last modified: {last_modified.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            conn = sqlite3.connect(enhanced_db)
            cursor = conn.cursor()
            
            # Check each enhanced data table
            tables_to_check = [
                ('analyst_ratings', 'Analyst Ratings'),
                ('earnings_history', 'Earnings History'),
                ('corporate_actions', 'Corporate Actions'),
                ('institutional_holdings', 'Institutional Holdings'),
                ('extended_price_data', 'Extended Price Data'),
                ('sector_performance', 'Sector Performance'),
                ('market_indicators', 'Market Indicators')
            ]
            
            print(f"\nENHANCED DATA BREAKDOWN:")
            total_enhanced_records = 0
            
            for table_name, display_name in tables_to_check:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    total_enhanced_records += count
                    print(f"   {display_name}: {count:,} records")
                except Exception as e:
                    print(f"   {display_name}: Table not found or error - {e}")
            
            print(f"\nTOTAL ENHANCED RECORDS: {total_enhanced_records:,}")
            
            # Get unique tickers across all tables
            try:
                cursor.execute("""
                    SELECT COUNT(DISTINCT ticker) FROM (
                        SELECT ticker FROM analyst_ratings
                        UNION SELECT ticker FROM earnings_history
                        UNION SELECT ticker FROM corporate_actions
                        UNION SELECT ticker FROM institutional_holdings
                        UNION SELECT ticker FROM extended_price_data
                    )
                """)
                enhanced_tickers = cursor.fetchone()[0]
                print(f"STOCKS WITH ENHANCED DATA: {enhanced_tickers}")
            except Exception as e:
                print(f"STOCKS WITH ENHANCED DATA: Unable to calculate - {e}")
            
            conn.close()
            
        except Exception as e:
            print(f"- Error reading enhanced database: {e}")
    else:
        print(f"- Database not found: {enhanced_db}")
    
    # Check progress files
    print(f"\n3. COLLECTION PROGRESS FILES")
    print("-" * 80)
    
    progress_file = "data_collection/improved_collection_progress.json"
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                progress = json.load(f)
            
            completed = len(progress.get('completed_tickers', []))
            failed = len(progress.get('failed_tickers', []))
            total_stocks = len(progress.get('stock_universe', {}))
            
            if total_stocks > 0:
                completion_pct = (completed / total_stocks) * 100
                print(f"+ Main Collector Progress: {completed}/{total_stocks} stocks ({completion_pct:.1f}%)")
                print(f"+ Failed stocks: {failed}")
                print(f"+ Total price records: {progress.get('total_price_records', 0):,}")
                print(f"+ Total fundamental records: {progress.get('total_fundamental_records', 0):,}")
            else:
                print(f"+ Progress file exists but no stock universe data")
                
        except Exception as e:
            print(f"- Error reading progress file: {e}")
    else:
        print(f"- Progress file not found: {progress_file}")
    
    # Summary
    print(f"\n" + "=" * 100)
    print(f"DUAL DATABASE SUMMARY")
    print(f"=" * 100)
    
    improved_exists = os.path.exists(improved_db)
    enhanced_exists = os.path.exists(enhanced_db)
    
    if improved_exists:
        improved_size = os.path.getsize(improved_db) / (1024 * 1024)
        print(f"+ Improved Database: {improved_size:.1f} MB")
    else:
        print(f"- Improved Database: Not found")
    
    if enhanced_exists:
        enhanced_size = os.path.getsize(enhanced_db) / (1024 * 1024)
        print(f"+ Enhanced Database: {enhanced_size:.1f} MB")
    else:
        print(f"- Enhanced Database: Not found")
    
    print(f"+ Monitoring Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"=" * 100)

def continuous_monitor(interval_minutes=5):
    """Continuously monitor databases at specified intervals"""
    print(f"Starting continuous monitoring every {interval_minutes} minutes...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            monitor_dual_databases()
            print(f"\n⏰ Next check in {interval_minutes} minutes...")
            time.sleep(interval_minutes * 60)
    except KeyboardInterrupt:
        print(f"\n🛑 Monitoring stopped by user")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--continuous":
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        continuous_monitor(interval)
    else:
        monitor_dual_databases()
