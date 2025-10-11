#!/usr/bin/env python3
"""
Real-time Collection Progress Monitor
=====================================

Comprehensive monitoring tool for the AI Bubble Detection Project data collection.
Tracks real-time progress, database growth, and collection statistics.

Features:
- Real-time progress monitoring
- Database size and growth tracking
- Collection statistics and metrics
- Performance analysis and estimates
- Continuous monitoring mode

Usage:
    python monitor_collection.py              # Single status check
    python monitor_collection.py --monitor    # Continuous monitoring
    python monitor_collection.py --help       # Show help

Author: AI Assistant
Date: 2025-10-11
"""

import json
import sqlite3
import time
from pathlib import Path
from datetime import datetime

def get_collection_progress():
    """Get current collection progress"""
    print("="*80)
    print("COMPREHENSIVE DATA COLLECTION PROGRESS")
    print("="*80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # Check completed tickers
    completed_file = Path("data_collection/completed_tickers.json")
    if completed_file.exists():
        with open(completed_file, 'r') as f:
            completed_tickers = json.load(f)
        print(f"Completed tickers: {len(completed_tickers)}")
    else:
        completed_tickers = set()
        print("Completed tickers: 0 (no progress file found)")
    
    # Check database size (using the actual database path from collector)
    db_file = Path(r"C:\Users\james\Downloads\StockDB\unified_stock_data.db")
    if db_file.exists():
        db_size_mb = db_file.stat().st_size / (1024 * 1024)
        print(f"Database size: {db_size_mb:.1f} MB")
        print(f"Last modified: {datetime.fromtimestamp(db_file.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print("Database not found!")
        return
    
    # Get database statistics
    try:
        conn = sqlite3.connect(r"C:\Users\james\Downloads\StockDB\unified_stock_data.db")
        cursor = conn.cursor()
        
        # Historical prices
        cursor.execute("SELECT COUNT(*) FROM historical_prices")
        price_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM historical_prices")
        price_tickers = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(date), MAX(date) FROM historical_prices")
        min_date, max_date = cursor.fetchone()
        
        # Current fundamentals
        cursor.execute("SELECT COUNT(*) FROM current_fundamentals")
        fund_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM current_fundamentals")
        fund_tickers = cursor.fetchone()[0]
        
        # Analyst ratings
        cursor.execute("SELECT COUNT(*) FROM analyst_ratings")
        analyst_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM analyst_ratings")
        analyst_tickers = cursor.fetchone()[0]
        
        # Corporate actions
        cursor.execute("SELECT COUNT(*) FROM corporate_actions")
        action_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM corporate_actions")
        action_tickers = cursor.fetchone()[0]
        
        conn.close()
        
        print(f"\nDATABASE STATISTICS:")
        print(f"Historical Prices: {price_records:,} records, {price_tickers:,} tickers")
        print(f"Date Range: {min_date} to {max_date}")
        print(f"Current Fundamentals: {fund_records:,} records, {fund_tickers:,} tickers")
        print(f"Analyst Ratings: {analyst_records:,} records, {analyst_tickers:,} tickers")
        print(f"Corporate Actions: {action_records:,} records, {action_tickers:,} tickers")
        
        # Calculate progress
        total_universe = 12617  # From our expansion
        progress_pct = (len(completed_tickers) / total_universe) * 100
        remaining = total_universe - len(completed_tickers)
        
        print(f"\nCOLLECTION PROGRESS:")
        print(f"Total Universe: {total_universe:,} stocks")
        print(f"Completed: {len(completed_tickers):,} stocks ({progress_pct:.1f}%)")
        print(f"Remaining: {remaining:,} stocks")
        
        # Estimate completion time (rough)
        if len(completed_tickers) > 0:
            # Assume ~1 stock per second average (very rough estimate)
            estimated_seconds = remaining
            estimated_hours = estimated_seconds / 3600
            print(f"Estimated time remaining: ~{estimated_hours:.1f} hours")
        
    except Exception as e:
        print(f"Error reading database: {e}")
    
    print("="*80)

def monitor_collection():
    """Monitor collection progress in real-time"""
    print("Starting real-time collection monitoring...")
    print("Press Ctrl+C to stop monitoring")
    print("="*80)
    
    try:
        while True:
            get_collection_progress()
            print("\nWaiting 30 seconds for next update...")
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")
        print("="*80)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--monitor":
        monitor_collection()
    else:
        get_collection_progress()
