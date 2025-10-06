#!/usr/bin/env python3
"""
Database Status Checker
"""

import sqlite3
import os

def check_stock_database():
    """Check stock data collection status"""
    db_path = "data_collection/unified_stock_data.db"
    if not os.path.exists(db_path):
        print("Stock database not found!")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT COUNT(*) FROM historical_prices')
        historical_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM current_fundamentals')
        fundamentals_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT ticker) FROM historical_prices')
        unique_tickers = cursor.fetchone()[0]
        
        print("STOCK DATA COLLECTION STATUS:")
        print(f"  Historical prices: {historical_count:,} records")
        print(f"  Current fundamentals: {fundamentals_count:,} records")
        print(f"  Unique tickers: {unique_tickers:,}")
        
    except Exception as e:
        print(f"Error checking stock database: {e}")
    finally:
        conn.close()

def check_balance_sheet_database():
    """Check balance sheet data collection status"""
    db_path = "Balance_Sheet_Scraper/balance_sheet_data.db"
    if not os.path.exists(db_path):
        print("Balance sheet database not found!")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        print("\nBALANCE SHEET DATA COLLECTION STATUS:")
        print("Available tables:")
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"  {table_name}: {count:,} records")
        
        # Check for specific tables
        if any('balance_sheet' in table[0] for table in tables):
            cursor.execute("SELECT COUNT(DISTINCT ticker) FROM balance_sheet_data")
            unique_companies = cursor.fetchone()[0]
            print(f"  Unique NZX companies with financial data: {unique_companies}")
        
    except Exception as e:
        print(f"Error checking balance sheet database: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_stock_database()
    check_balance_sheet_database()
