#!/usr/bin/env python3
"""
Data Collection Scale Analysis
Shows the massive amount of data the unified_historical_collector.py will collect
"""

from unified_historical_collector import UnifiedHistoricalCollector
from datetime import date

def main():
    print("=" * 80)
    print("MASSIVE DATA COLLECTION SCALE ANALYSIS")
    print("=" * 80)
    
    collector = UnifiedHistoricalCollector()
    
    # Stock universe breakdown
    nzx_stocks = sum(1 for t in collector.stock_universe.keys() if ".NZ" in t)
    asx_stocks = sum(1 for t in collector.stock_universe.keys() if ".AX" in t)
    us_stocks = sum(1 for t in collector.stock_universe.keys() if ".NZ" not in t and ".AX" not in t)
    
    print(f"\nSTOCK UNIVERSE:")
    print(f"  Total Stocks: {len(collector.stock_universe):,}")
    print(f"  NZX Stocks: {nzx_stocks:,}")
    print(f"  ASX Stocks: {asx_stocks:,}")
    print(f"  US Stocks: {us_stocks:,}")
    
    # Date range
    years_of_data = (collector.end_date - collector.start_date).days / 365.25
    print(f"\nDATE RANGE:")
    print(f"  Start Date: {collector.start_date}")
    print(f"  End Date: {collector.end_date}")
    print(f"  Years of Data: {years_of_data:.1f} years")
    
    # Data volume estimates
    print(f"\nESTIMATED DATA VOLUME:")
    
    # Weekly data points per stock
    weeks_per_stock = (collector.end_date - collector.start_date).days / 7
    print(f"  Weekly records per stock: ~{weeks_per_stock:.0f}")
    
    # Total records
    total_records = len(collector.stock_universe) * weeks_per_stock
    print(f"  Total estimated records: {total_records:,.0f}")
    
    # Database size estimate (rough calculation)
    avg_record_size = 0.0005  # MB per record (estimate)
    estimated_db_size = total_records * avg_record_size
    print(f"  Estimated database size: {estimated_db_size:.1f} MB")
    
    # Collection time estimate
    print(f"\nCOLLECTION TIME ESTIMATE:")
    avg_delay = (collector.min_delay + collector.max_delay) / 2
    total_time_seconds = len(collector.stock_universe) * avg_delay
    total_time_hours = total_time_seconds / 3600
    print(f"  Average delay per request: {avg_delay:.1f} seconds")
    print(f"  Total collection time: {total_time_hours:.1f} hours")
    print(f"  Total collection time: {total_time_hours/24:.1f} days")
    
    # Current progress
    print(f"\nCURRENT PROGRESS:")
    print(f"  Completed: {len(collector.progress['completed_tickers'])}")
    print(f"  Failed: {len(collector.progress['failed_tickers'])}")
    print(f"  Pending: {len(collector.progress['pending_tickers']) - len(collector.progress['completed_tickers']) - len(collector.progress['failed_tickers'])}")
    
    # Comparison to current data
    print(f"\nCOMPARISON TO CURRENT DATA:")
    print(f"  Current records collected: 16,645")
    print(f"  Potential total records: {total_records:,.0f}")
    print(f"  Completion percentage: {(16465 / total_records) * 100:.4f}%")
    
    print(f"\n" + "=" * 80)
    print("CONCLUSION: YES, THIS WILL COLLECT MASSIVE AMOUNTS OF DATA!")
    print("=" * 80)
    print(f"• {total_records:,.0f} total records")
    print(f"• {estimated_db_size:.1f} MB database size")
    print(f"• {total_time_hours:.1f} hours collection time")
    print(f"• 25+ years of historical data")
    print(f"• 3 major markets (US, ASX, NZX)")
    print("=" * 80)

if __name__ == "__main__":
    main()
