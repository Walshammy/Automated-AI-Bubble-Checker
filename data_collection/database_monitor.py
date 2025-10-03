#!/usr/bin/env python3
"""
UNIFIED DATABASE MONITOR
========================

Single script to monitor the unified stock database with comprehensive statistics.

Features:
- Database size and file information
- Record counts by table
- Stock coverage analysis
- Exchange breakdown
- Date range analysis
- Collection progress tracking
- Performance metrics

Author: AI Assistant
Date: 2025-10-03
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime, date
from typing import Dict, Any, Tuple

class UnifiedDatabaseMonitor:
    """Monitor unified stock database"""
    
    def __init__(self, db_path: str = "data_collection/unified_stock_data.db",
                 backup_path: str = r"C:\Users\james\Downloads\Stock Valuation\unified_stock_data.db"):
        self.db_path = db_path
        self.backup_path = backup_path
        
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get file information"""
        if not os.path.exists(file_path):
            return {"exists": False}
        
        stat = os.stat(file_path)
        size_mb = stat.st_size / (1024 * 1024)
        modified = datetime.fromtimestamp(stat.st_mtime)
        
        return {
            "exists": True,
            "size_mb": round(size_mb, 2),
            "last_modified": modified.strftime("%Y-%m-%d %H:%M:%S"),
            "file_path": file_path
        }
    
    def get_table_stats(self, conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
        """Get comprehensive table statistics"""
        cursor = conn.cursor()
        stats = {}
        
        tables = [
            'historical_prices', 'current_fundamentals', 'analyst_ratings',
            'earnings_history', 'corporate_actions', 'institutional_holdings',
            'extended_price_data', 'sector_performance', 'market_indicators'
        ]
        
        for table in tables:
            try:
                # Get record count
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                # Get unique tickers
                if table in ['historical_prices', 'current_fundamentals', 'analyst_ratings', 
                           'earnings_history', 'corporate_actions', 'institutional_holdings', 
                           'extended_price_data']:
                    cursor.execute(f"SELECT COUNT(DISTINCT ticker) FROM {table}")
                    unique_tickers = cursor.fetchone()[0]
                else:
                    unique_tickers = 0
                
                # Get date range for time-series tables
                date_range = None
                if table in ['historical_prices', 'analyst_ratings', 'earnings_history', 
                           'corporate_actions', 'institutional_holdings', 'extended_price_data',
                           'sector_performance', 'market_indicators']:
                    date_col = 'date' if table != 'analyst_ratings' else 'rating_date'
                    if table == 'earnings_history':
                        date_col = 'quarter_date'
                    elif table == 'corporate_actions':
                        date_col = 'action_date'
                    elif table == 'institutional_holdings':
                        date_col = 'snapshot_date'
                    elif table == 'extended_price_data':
                        date_col = 'snapshot_date'
                    
                    try:
                        cursor.execute(f"SELECT MIN({date_col}), MAX({date_col}) FROM {table}")
                        min_date, max_date = cursor.fetchone()
                        if min_date and max_date:
                            date_range = f"{min_date} to {max_date}"
                    except:
                        pass
                
                stats[table] = {
                    'records': count,
                    'unique_tickers': unique_tickers,
                    'date_range': date_range
                }
                
            except Exception as e:
                stats[table] = {
                    'records': 0,
                    'unique_tickers': 0,
                    'date_range': f"Error: {str(e)}"
                }
        
        return stats
    
    def get_exchange_breakdown(self, conn: sqlite3.Connection) -> Dict[str, Dict[str, int]]:
        """Get breakdown by exchange"""
        cursor = conn.cursor()
        breakdown = {}
        
        try:
            # Historical prices breakdown
            cursor.execute("""
                SELECT exchange, COUNT(DISTINCT ticker) as tickers, COUNT(*) as records
                FROM historical_prices 
                GROUP BY exchange
                ORDER BY records DESC
            """)
            breakdown['historical_prices'] = dict(cursor.fetchall())
        except:
            breakdown['historical_prices'] = {}
        
        try:
            # Current fundamentals breakdown
            cursor.execute("""
                SELECT exchange, COUNT(DISTINCT ticker) as tickers, COUNT(*) as records
                FROM current_fundamentals 
                GROUP BY exchange
                ORDER BY records DESC
            """)
            breakdown['current_fundamentals'] = dict(cursor.fetchall())
        except:
            breakdown['current_fundamentals'] = {}
        
        return breakdown
    
    def get_top_stocks(self, conn: sqlite3.Connection, limit: int = 10) -> Dict[str, list]:
        """Get top stocks by record count"""
        cursor = conn.cursor()
        top_stocks = {}
        
        try:
            cursor.execute("""
                SELECT ticker, COUNT(*) as records
                FROM historical_prices 
                GROUP BY ticker
                ORDER BY records DESC
                LIMIT ?
            """, (limit,))
            top_stocks['historical_prices'] = cursor.fetchall()
        except:
            top_stocks['historical_prices'] = []
        
        try:
            cursor.execute("""
                SELECT ticker, COUNT(*) as records
                FROM analyst_ratings 
                GROUP BY ticker
                ORDER BY records DESC
                LIMIT ?
            """, (limit,))
            top_stocks['analyst_ratings'] = cursor.fetchall()
        except:
            top_stocks['analyst_ratings'] = []
        
        return top_stocks
    
    def get_collection_progress(self) -> Dict[str, Any]:
        """Get collection progress from progress file"""
        progress_file = "data_collection/collection_progress.json"
        
        if not os.path.exists(progress_file):
            return {"progress_file": "Not found"}
        
        try:
            import json
            with open(progress_file, 'r') as f:
                progress = json.load(f)
            return progress
        except Exception as e:
            return {"progress_file": f"Error reading: {e}"}
    
    def monitor_database(self, file_path: str) -> Dict[str, Any]:
        """Monitor a single database file"""
        print(f"\n{'='*60}")
        print(f"DATABASE: {os.path.basename(file_path)}")
        print(f"{'='*60}")
        
        # File information
        file_info = self.get_file_info(file_path)
        if not file_info["exists"]:
            print("Database file not found!")
            return {"error": "File not found"}
        
        print(f"File Size: {file_info['size_mb']} MB")
        print(f"Last Modified: {file_info['last_modified']}")
        print(f"Path: {file_info['file_path']}")
        
        # Database statistics
        try:
            conn = sqlite3.connect(file_path)
            
            # Table statistics
            print(f"\n{'TABLE STATISTICS':<30} {'RECORDS':<12} {'TICKERS':<10} {'DATE RANGE'}")
            print("-" * 80)
            
            table_stats = self.get_table_stats(conn)
            total_records = 0
            
            for table, stats in table_stats.items():
                records = stats['records']
                tickers = stats['unique_tickers']
                date_range = stats['date_range'] or "N/A"
                
                print(f"{table:<30} {records:<12,} {tickers:<10} {date_range}")
                total_records += records
            
            print("-" * 80)
            print(f"{'TOTAL':<30} {total_records:<12,}")
            
            # Exchange breakdown
            print(f"\n{'EXCHANGE BREAKDOWN'}")
            print("-" * 50)
            
            exchange_breakdown = self.get_exchange_breakdown(conn)
            
            print("Historical Prices:")
            for exchange, count in exchange_breakdown.get('historical_prices', {}).items():
                print(f"  {exchange}: {count} records")
            
            print("\nCurrent Fundamentals:")
            for exchange, count in exchange_breakdown.get('current_fundamentals', {}).items():
                print(f"  {exchange}: {count} records")
            
            # Top stocks
            print(f"\n{'TOP STOCKS BY RECORDS'}")
            print("-" * 40)
            
            top_stocks = self.get_top_stocks(conn)
            
            print("Historical Prices:")
            for ticker, records in top_stocks['historical_prices'][:5]:
                print(f"  {ticker}: {records:,} records")
            
            print("\nAnalyst Ratings:")
            for ticker, records in top_stocks['analyst_ratings'][:5]:
                print(f"  {ticker}: {records:,} records")
            
            conn.close()
            
            return {
                "file_info": file_info,
                "table_stats": table_stats,
                "total_records": total_records,
                "exchange_breakdown": exchange_breakdown,
                "top_stocks": top_stocks
            }
            
        except Exception as e:
            print(f"Error accessing database: {e}")
            return {"error": str(e)}
    
    def run_monitor(self):
        """Run comprehensive database monitoring"""
        print("UNIFIED DATABASE MONITOR")
        print("=" * 60)
        print("Comprehensive database statistics and analysis")
        print("=" * 60)
        
        # Monitor main database
        main_stats = self.monitor_database(self.db_path)
        
        # Monitor backup database
        backup_stats = self.monitor_database(self.backup_path)
        
        # Collection progress
        print(f"\n{'COLLECTION PROGRESS'}")
        print("-" * 30)
        
        progress = self.get_collection_progress()
        for key, value in progress.items():
            print(f"{key}: {value}")
        
        # Summary
        print(f"\n{'SUMMARY'}")
        print("-" * 20)
        
        if "total_records" in main_stats:
            print(f"Main Database Records: {main_stats['total_records']:,}")
        
        if "total_records" in backup_stats:
            print(f"Backup Database Records: {backup_stats['total_records']:,}")
        
        # Data quality metrics
        if "table_stats" in main_stats:
            stats = main_stats["table_stats"]
            
            print(f"\n{'DATA QUALITY METRICS'}")
            print("-" * 30)
            
            # Coverage metrics
            historical_tickers = stats.get('historical_prices', {}).get('unique_tickers', 0)
            fundamentals_tickers = stats.get('current_fundamentals', {}).get('unique_tickers', 0)
            analyst_tickers = stats.get('analyst_ratings', {}).get('unique_tickers', 0)
            
            print(f"Stocks with Price Data: {historical_tickers}")
            print(f"Stocks with Fundamentals: {fundamentals_tickers}")
            print(f"Stocks with Analyst Ratings: {analyst_tickers}")
            
            # Data richness
            avg_price_records = stats.get('historical_prices', {}).get('records', 0) / max(historical_tickers, 1)
            avg_analyst_records = stats.get('analyst_ratings', {}).get('records', 0) / max(analyst_tickers, 1)
            
            print(f"Avg Price Records per Stock: {avg_price_records:.1f}")
            print(f"Avg Analyst Records per Stock: {avg_analyst_records:.1f}")
        
        print(f"\n{'MONITORING COMPLETE'}")
        print("=" * 60)

def main():
    """Main function"""
    monitor = UnifiedDatabaseMonitor()
    monitor.run_monitor()

if __name__ == "__main__":
    main()
