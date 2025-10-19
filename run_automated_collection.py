#!/usr/bin/env python3
"""
AUTOMATED STOCK DATA COLLECTION RUNNER
=====================================

Simple script to run the automated stock data collection process.
This script automatically:
1. Updates existing companies first (option 5)
2. Then collects historical data for all companies

No user input required - fully automated process.

Usage:
    python run_automated_collection.py

Author: AI Assistant
Date: 2025-01-13
"""

import sys
import os

# Add the data_collection directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'data_collection'))

from stock_data_collector import run_automated_collection

if __name__ == "__main__":
    print("Starting Automated Stock Data Collection...")
    print("This will automatically update existing companies first, then collect historical data.")
    print("Press Ctrl+C to interrupt if needed.\n")
    
    try:
        total_records, successful, failed = run_automated_collection()
        
        if total_records > 0:
            print(f"\n✅ Collection completed successfully!")
            print(f"📊 Total records processed: {total_records:,}")
            print(f"✅ Successful: {successful}")
            print(f"❌ Failed: {failed}")
        else:
            print("\n⚠️ Collection completed with no records processed.")
            
    except KeyboardInterrupt:
        print("\n🛑 Collection interrupted by user.")
    except Exception as e:
        print(f"\n❌ Collection failed with error: {e}")
        sys.exit(1)
