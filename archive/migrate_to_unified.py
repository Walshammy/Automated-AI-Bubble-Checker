#!/usr/bin/env python3
"""
DATA MIGRATION SCRIPT
=====================

Migrates existing data from both improved_data.db and enhanced_data.db
into the new unified_stock_data.db system.

Author: AI Assistant
Date: 2025-10-03
"""

import sqlite3
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataMigrator:
    """Migrates data from old databases to unified system"""
    
    def __init__(self):
        self.improved_db = "data_collection/improved_data.db"
        self.enhanced_db = "db_additions_test/enhanced_data.db"
        self.unified_db = "data_collection/unified_stock_data.db"
        
        # Backup paths
        self.improved_backup = r"C:\Users\james\Downloads\Stock Valuation\improved_data.db"
        self.enhanced_backup = r"C:\Users\james\Downloads\Stock Valuation\enhanced_data.db"
        
    def check_source_databases(self):
        """Check if source databases exist"""
        logger.info("Checking source databases...")
        
        sources = {
            "Improved DB (Local)": self.improved_db,
            "Improved DB (Backup)": self.improved_backup,
            "Enhanced DB (Local)": self.enhanced_db,
            "Enhanced DB (Backup)": self.enhanced_backup
        }
        
        available_sources = {}
        for name, path in sources.items():
            if os.path.exists(path):
                size_mb = os.path.getsize(path) / (1024 * 1024)
                logger.info(f"+ {name}: {size_mb:.1f} MB")
                available_sources[name] = path
            else:
                logger.info(f"- {name}: Not found")
        
        return available_sources
    
    def migrate_historical_prices(self, source_db: str):
        """Migrate historical prices from improved database"""
        logger.info("Migrating historical prices...")
        
        try:
            # Connect to source and destination
            source_conn = sqlite3.connect(source_db)
            dest_conn = sqlite3.connect(self.unified_db)
            
            source_cursor = source_conn.cursor()
            dest_cursor = dest_conn.cursor()
            
            # Get count from source
            source_cursor.execute("SELECT COUNT(*) FROM historical_prices")
            source_count = source_cursor.fetchone()[0]
            
            # Get count from destination
            dest_cursor.execute("SELECT COUNT(*) FROM historical_prices")
            dest_count = dest_cursor.fetchone()[0]
            
            logger.info(f"Source: {source_count:,} records, Destination: {dest_count:,} records")
            
            if source_count == 0:
                logger.info("No historical prices to migrate")
                return 0
            
            # Migrate data
            source_cursor.execute("""
                SELECT ticker, date, open_price, high_price, low_price, close_price,
                       volume, adjusted_close, exchange, is_delisted, delisted_date
                FROM historical_prices
            """)
            
            migrated_count = 0
            batch_size = 1000
            
            while True:
                batch = source_cursor.fetchmany(batch_size)
                if not batch:
                    break
                
                dest_cursor.executemany("""
                    INSERT OR REPLACE INTO historical_prices (
                        ticker, date, open_price, high_price, low_price, close_price,
                        volume, adjusted_close, exchange, is_delisted, delisted_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                
                migrated_count += len(batch)
                dest_conn.commit()
                
                if migrated_count % 10000 == 0:
                    logger.info(f"Migrated {migrated_count:,} price records...")
            
            logger.info(f"Migrated {migrated_count:,} historical price records")
            
            source_conn.close()
            dest_conn.close()
            
            return migrated_count
            
        except Exception as e:
            logger.error(f"Error migrating historical prices: {e}")
            return 0
    
    def migrate_current_fundamentals(self, source_db: str):
        """Migrate current fundamentals from improved database"""
        logger.info("Migrating current fundamentals...")
        
        try:
            # Connect to source and destination
            source_conn = sqlite3.connect(source_db)
            dest_conn = sqlite3.connect(self.unified_db)
            
            source_cursor = source_conn.cursor()
            dest_cursor = dest_conn.cursor()
            
            # Get count from source
            source_cursor.execute("SELECT COUNT(*) FROM current_fundamentals")
            source_count = source_cursor.fetchone()[0]
            
            # Get count from destination
            dest_cursor.execute("SELECT COUNT(*) FROM current_fundamentals")
            dest_count = dest_cursor.fetchone()[0]
            
            logger.info(f"Source: {source_count:,} records, Destination: {dest_count:,} records")
            
            if source_count == 0:
                logger.info("No fundamentals to migrate")
                return 0
            
            # Migrate data
            source_cursor.execute("""
                SELECT ticker, snapshot_date, market_cap, pe_ratio, pb_ratio, peg_ratio,
                       ps_ratio, dividend_yield, roe, roa, roic, debt_to_equity, current_ratio,
                       fcf_yield, eps_ttm, eps_growth_5y, revenue_growth_5y, gross_margin,
                       operating_margin, net_margin, beta, volatility_1y, max_drawdown_5y,
                       sector, industry, exchange, is_delisted, delisted_date
                FROM current_fundamentals
            """)
            
            batch = source_cursor.fetchall()
            
            dest_cursor.executemany("""
                INSERT OR REPLACE INTO current_fundamentals (
                    ticker, snapshot_date, market_cap, pe_ratio, pb_ratio, peg_ratio,
                    ps_ratio, dividend_yield, roe, roa, roic, debt_to_equity, current_ratio,
                    fcf_yield, eps_ttm, eps_growth_5y, revenue_growth_5y, gross_margin,
                    operating_margin, net_margin, beta, volatility_1y, max_drawdown_5y,
                    sector, industry, exchange, is_delisted, delisted_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            
            dest_conn.commit()
            
            logger.info(f"Migrated {len(batch):,} fundamental records")
            
            source_conn.close()
            dest_conn.close()
            
            return len(batch)
            
        except Exception as e:
            logger.error(f"Error migrating fundamentals: {e}")
            return 0
    
    def migrate_enhanced_data(self, source_db: str):
        """Migrate enhanced data from enhanced database"""
        logger.info("Migrating enhanced data...")
        
        try:
            # Connect to source and destination
            source_conn = sqlite3.connect(source_db)
            dest_conn = sqlite3.connect(self.unified_db)
            
            source_cursor = source_conn.cursor()
            dest_cursor = dest_conn.cursor()
            
            total_migrated = 0
            
            # Tables to migrate
            tables = [
                ('analyst_ratings', 'analyst_ratings'),
                ('earnings_history', 'earnings_history'),
                ('corporate_actions', 'corporate_actions'),
                ('institutional_holdings', 'institutional_holdings'),
                ('extended_price_data', 'extended_price_data'),
                ('sector_performance', 'sector_performance'),
                ('market_indicators', 'market_indicators')
            ]
            
            for source_table, dest_table in tables:
                try:
                    # Check if source table exists
                    source_cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{source_table}'")
                    if not source_cursor.fetchone():
                        logger.info(f"Table {source_table} not found in source, skipping...")
                        continue
                    
                    # Get count from source
                    source_cursor.execute(f"SELECT COUNT(*) FROM {source_table}")
                    source_count = source_cursor.fetchone()[0]
                    
                    if source_count == 0:
                        logger.info(f"No {source_table} records to migrate")
                        continue
                    
                    # Get all data from source
                    source_cursor.execute(f"SELECT * FROM {source_table}")
                    rows = source_cursor.fetchall()
                    
                    # Get column names
                    source_cursor.execute(f"PRAGMA table_info({source_table})")
                    columns = [col[1] for col in source_cursor.fetchall()]
                    
                    # Create placeholders for INSERT
                    placeholders = ', '.join(['?' for _ in columns])
                    
                    # Insert into destination
                    dest_cursor.executemany(f"""
                        INSERT OR REPLACE INTO {dest_table} ({', '.join(columns)})
                        VALUES ({placeholders})
                    """, rows)
                    
                    dest_conn.commit()
                    
                    logger.info(f"Migrated {len(rows):,} {source_table} records")
                    total_migrated += len(rows)
                    
                except Exception as e:
                    logger.error(f"Error migrating {source_table}: {e}")
                    continue
            
            logger.info(f"Total enhanced records migrated: {total_migrated:,}")
            
            source_conn.close()
            dest_conn.close()
            
            return total_migrated
            
        except Exception as e:
            logger.error(f"Error migrating enhanced data: {e}")
            return 0
    
    def run_migration(self):
        """Run complete data migration"""
        logger.info("Starting data migration...")
        
        # Check source databases
        available_sources = self.check_source_databases()
        
        if not available_sources:
            logger.error("No source databases found!")
            return
        
        # Initialize unified database
        from unified_stock_data_collector import UnifiedStockDataCollector
        collector = UnifiedStockDataCollector()
        logger.info("Unified database initialized")
        
        total_migrated = 0
        
        # Migrate from improved database (prioritize backup if available)
        improved_source = None
        if "Improved DB (Backup)" in available_sources:
            improved_source = available_sources["Improved DB (Backup)"]
        elif "Improved DB (Local)" in available_sources:
            improved_source = available_sources["Improved DB (Local)"]
        
        if improved_source:
            logger.info(f"Migrating from improved database: {improved_source}")
            
            price_records = self.migrate_historical_prices(improved_source)
            fundamental_records = self.migrate_current_fundamentals(improved_source)
            
            total_migrated += price_records + fundamental_records
        
        # Migrate from enhanced database (prioritize backup if available)
        enhanced_source = None
        if "Enhanced DB (Backup)" in available_sources:
            enhanced_source = available_sources["Enhanced DB (Backup)"]
        elif "Enhanced DB (Local)" in available_sources:
            enhanced_source = available_sources["Enhanced DB (Local)"]
        
        if enhanced_source:
            logger.info(f"Migrating from enhanced database: {enhanced_source}")
            
            enhanced_records = self.migrate_enhanced_data(enhanced_source)
            total_migrated += enhanced_records
        
        # Copy unified database to backup location
        try:
            import shutil
            backup_dir = os.path.dirname(collector.backup_path)
            os.makedirs(backup_dir, exist_ok=True)
            shutil.copy2(self.unified_db, collector.backup_path)
            logger.info(f"Unified database copied to backup: {collector.backup_path}")
        except Exception as e:
            logger.error(f"Error copying to backup: {e}")
        
        logger.info("=" * 60)
        logger.info("MIGRATION COMPLETED!")
        logger.info("=" * 60)
        logger.info(f"Total records migrated: {total_migrated:,}")
        logger.info(f"Unified database: {self.unified_db}")
        logger.info(f"Backup location: {collector.backup_path}")
        logger.info("=" * 60)

def main():
    """Main function"""
    print("=" * 60)
    print("DATA MIGRATION SCRIPT")
    print("=" * 60)
    print("This script migrates data from:")
    print("+ improved_data.db (historical prices + fundamentals)")
    print("+ enhanced_data.db (analyst ratings, earnings, etc.)")
    print("Into the new unified_stock_data.db system")
    print("=" * 60)
    
    migrator = DataMigrator()
    
    try:
        migrator.run_migration()
    except Exception as e:
        logger.error(f"Migration failed: {e}")

if __name__ == "__main__":
    main()
