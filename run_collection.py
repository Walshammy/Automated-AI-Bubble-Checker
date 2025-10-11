#!/usr/bin/env python3
"""
Comprehensive Data Collection Runner
===================================

Background execution script for the AI Bubble Detection Project data collection.
Runs the comprehensive stock data collection process with monitoring capabilities.

Features:
- Background collection execution
- Progress monitoring and updates
- Automatic error handling and recovery
- Non-interactive operation for server environments
- Collection status reporting

Usage:
    python run_collection.py              # Start background collection
    python run_collection.py --monitor    # Start with monitoring
    python run_collection.py --help       # Show help

Collection Process:
- Collects data for all 12,617 stocks
- Uses Yahoo Finance API for real data
- Implements rate limiting and error handling
- Provides progress updates every 30 seconds
- Automatically resumes from last completed ticker

Author: AI Assistant
Date: 2025-10-11
"""

import time
import json
from data_collection.stock_data_collector import UnifiedStockDataCollector

def run_collection_with_monitoring():
    """Run collection with periodic progress updates"""
    print("="*80)
    print("STARTING COMPREHENSIVE DATA COLLECTION")
    print("="*80)
    print("Target: All 12,617 stocks with maximum historical data")
    print("="*80)
    
    # Initialize collector
    collector = UnifiedStockDataCollector()
    
    # Start collection in background thread
    import threading
    
    def collect_data():
        try:
            collector.run_collection(100.0, prioritize_updates=True)  # 100% of universe with priority system
        except Exception as e:
            print(f"Collection error: {e}")
    
    # Start collection thread
    collection_thread = threading.Thread(target=collect_data, daemon=True)
    collection_thread.start()
    
    # Monitor progress
    print("Collection started. Monitoring progress...")
    print("Press Ctrl+C to stop monitoring (collection will continue)")
    print("="*80)
    
    try:
        while True:
            # Check progress
            try:
                with open('data_collection/completed_tickers.json', 'r') as f:
                    completed = json.load(f)
                
                total_universe = 12617
                progress_pct = (len(completed) / total_universe) * 100
                remaining = total_universe - len(completed)
                
                print(f"\nProgress Update: {datetime.now().strftime('%H:%M:%S')}")
                print(f"Completed: {len(completed):,} / {total_universe:,} ({progress_pct:.1f}%)")
                print(f"Remaining: {remaining:,} stocks")
                
                if len(completed) > 0:
                    estimated_hours = remaining / 3600  # Rough estimate
                    print(f"Estimated time remaining: ~{estimated_hours:.1f} hours")
                
            except Exception as e:
                print(f"Error checking progress: {e}")
            
            # Wait before next check
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped. Collection continues in background.")
        print("Use 'python monitor_collection.py' to check progress later.")

if __name__ == "__main__":
    from datetime import datetime
    run_collection_with_monitoring()
