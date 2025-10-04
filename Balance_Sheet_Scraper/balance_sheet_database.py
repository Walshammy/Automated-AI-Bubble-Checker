"""
Balance Sheet Database Manager
Handles database operations for balance sheet and financial statement data
"""

import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class BalanceSheetDatabase:
    """Database manager for balance sheet and financial statement data"""
    
    def __init__(self, db_path: str = "../data_collection/unified_stock_data.db"):
        self.db_path = db_path
        self.init_tables()
    
    def init_tables(self):
        """Initialize balance sheet related tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Financial Announcements Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS financial_announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                announcement_id TEXT UNIQUE NOT NULL,
                ticker TEXT NOT NULL,
                title TEXT,
                announcement_url TEXT,
                announcement_date TEXT,
                announcement_type TEXT,
                exchange TEXT DEFAULT 'NZX',
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed BOOLEAN DEFAULT FALSE
            )
        """)
        
        # PDF Documents Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS financial_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                announcement_id TEXT NOT NULL,
                pdf_url TEXT,
                pdf_filename TEXT,
                pdf_path TEXT,
                document_type TEXT,
                download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'downloaded',
                file_size_kb INTEGER,
                FOREIGN KEY (announcement_id) REFERENCES financial_announcements(announcement_id)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS balance_sheet_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                announcement_id TEXT NOT NULL,
                report_date DATE,
                report_type TEXT,
                statement_type TEXT,
                
                -- Balance Sheet Items
                total_assets REAL,
                current_assets REAL,
                non_current_assets REAL,
                cash_and_equivalents REAL,
                accounts_receivable REAL,
                inventory REAL,
                total_liabilities REAL,
                current_liabilities REAL,
                non_current_liabilities REAL,
                accounts_payable REAL,
                long_term_debt REAL,
                total_equity REAL,
                retained_earnings REAL,
                
                -- Profit & Loss Items
                revenue REAL,
                gross_profit REAL,
                operating_income REAL,
                ebitda REAL,
                ebit REAL,
                net_income REAL,
                
                -- Cash Flow Items
                operating_cash_flow REAL,
                investing_cash_flow REAL,
                financing_cash_flow REAL,
                free_cash_flow REAL,
                
                -- Key Ratios (calculated)
                current_ratio REAL,
                quick_ratio REAL,
                debt_to_equity REAL,
                return_on_assets REAL,
                return_on_equity REAL,
                gross_margin REAL,
                operating_margin REAL,
                net_margin REAL,
                
                -- Metadata
                extraction_confidence REAL,
                data_source TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (announcement_id) REFERENCES financial_announcements(announcement_id)
            )
        """)
        
        # Financial Metrics History Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS financial_metrics_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                metric_date DATE NOT NULL,
                metric_type TEXT NOT NULL,
                metric_value REAL NOT NULL,
                annual_period BOOLEAN DEFAULT TRUE,
                quarter_period BOOLEAN DEFAULT FALSE,
                data_source TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, metric_date, metric_type)
            )
        """)
        
        # Create indices for better performance
        indices = [
            "CREATE INDEX IF NOT EXISTS idx_financial_announcements_ticker ON financial_announcements(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_financial_announcements_date ON financial_announcements(announcement_date)",
            "CREATE INDEX IF NOT EXISTS idx_balance_sheet_ticker ON balance_sheet_data(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_balance_sheet_date ON balance_sheet_data(report_date)",
            "CREATE INDEX IF NOT EXISTS idx_metrics_ticker ON financial_metrics_history(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_metrics_date ON financial_metrics_history(metric_date)",
            "CREATE INDEX IF NOT EXISTS idx_metrics_type ON financial_metrics_history(metric_type)"
        ]
        
        for index_sql in indices:
            cursor.execute(index_sql)
        
        conn.commit()
        conn.close()
        logging.info("Balance sheet database tables initialized")
    
    def insert_announcement(self, announcement_data: dict) -> bool:
        """Insert financial announcement data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO financial_announcements
                (announcement_id, ticker, title, announcement_url, announcement_date, announcement_type, exchange)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                announcement_data.get('announcement_id'),
                announcement_data.get('ticker'),
                announcement_data.get('title'),
                announcement_data.get('announcement_url'),
                announcement_data.get('announcement_date'),
                announcement_data.get('announcement_type'),
                announcement_data.get('exchange', 'NZX')
            ))
            
            conn.commit()
            conn.close()
            logging.info(f"Inserted announcement {announcement_data.get('announcement_id')}")
            return True
            
        except Exception as e:
            logging.error(f"Error inserting announcement: {e}")
            return False
    
    def insert_document(self, document_data: dict) -> bool:
        """Insert PDF document data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO financial_documents
                (announcement_id, pdf_url, pdf_filename, pdf_path, document_type, file_size_kb)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                document_data.get('announcement_id'),
                document_data.get('pdf_url'),
                document_data.get('pdf_filename'),
                document_data.get('pdf_path'),
                document_data.get('document_type'),
                document_data.get('file_size_kb')
            ))
            
            conn.commit()
            conn.close()
            logging.info(f"Inserted document for announcement {document_data.get('announcement_id')}")
            return True
            
        except Exception as e:
            logging.error(f"Error inserting document: {e}")
            return False
    
    def insert_balance_sheet_data(self, financial_data: dict) -> bool:
        """Insert balance sheet financial data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO balance_sheet_data
                (ticker, announcement_id, report_date, report_type, statement_type,
                 total_assets, current_assets, non_current_assets, cash_and_equivalents,
                 accounts_receivable, inventory, total_liabilities, current_liabilities,
                 non_current_liabilities, accounts_payable, long_term_debt, total_equity,
                 retained_earnings, revenue, gross_profit, operating_income, ebitda,
                 ebit, net_income, operating_cash_flow, investing_cash_flow,
                 financing_cash_flow, free_cash_flow, current_ratio, quick_ratio,
                 debt_to_equity, return_on_assets, return_on_equity, gross_margin,
                 operating_margin, net_margin, extraction_confidence, data_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                financial_data.get('ticker'),
                financial_data.get('announcement_id'),
                financial_data.get('report_date'),
                financial_data.get('report_type'),
                financial_data.get('statement_type'),
                financial_data.get('total_assets'),
                financial_data.get('current_assets'),
                financial_data.get('non_current_assets'),
                financial_data.get('cash_and_equivalents'),
                financial_data.get('accounts_receivable'),
                financial_data.get('inventory'),
                financial_data.get('total_liabilities'),
                financial_data.get('current_liabilities'),
                financial_data.get('non_current_liabilities'),
                financial_data.get('accounts_payable'),
                financial_data.get('long_term_debt'),
                financial_data.get('total_equity'),
                financial_data.get('retained_earnings'),
                financial_data.get('revenue'),
                financial_data.get('gross_profit'),
                financial_data.get('operating_income'),
                financial_data.get('ebitda'),
                financial_data.get('ebit'),
                financial_data.get('net_income'),
                financial_data.get('operating_cash_flow'),
                financial_data.get('investing_cash_flow'),
                financial_data.get('financing_cash_flow'),
                financial_data.get('free_cash_flow'),
                financial_data.get('current_ratio'),
                financial_data.get('quick_ratio'),
                financial_data.get('debt_to_equity'),
                financial_data.get('return_on_assets'),
                financial_data.get('return_on_equity'),
                financial_data.get('gross_margin'),
                financial_data.get('operating_margin'),
                financial_data.get('net_margin'),
                financial_data.get('extraction_confidence'),
                financial_data.get('data_source')
            ))
            
            conn.commit()
            conn.close()
            logging.info(f"Inserted balance sheet data for {financial_data.get('ticker')}")
            return True
            
        except Exception as e:
            logging.error(f"Error inserting balance sheet data: {e}")
            return False
    
    def get_tickers_with_financial_data(self) -> pd.DataFrame:
        """Get all tickers that have balance sheet data"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = """
                SELECT DISTINCT ticker, COUNT(*) as report_count, 
                       MAX(report_date) as latest_report
                FROM balance_sheet_data 
                GROUP BY ticker
                ORDER BY report_count DESC
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
            
        except Exception as e:
            logging.error(f"Error getting tickers: {e}")
            return pd.DataFrame()
    
    def get_latest_financial_data(self, ticker: str) -> pd.DataFrame:
        """Get latest financial data for a specific ticker"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = """
                SELECT * FROM balance_sheet_data 
                WHERE ticker = ? 
                ORDER BY report_date DESC 
                LIMIT 1
            """
            
            df = pd.read_sql_query(query, conn, params=(ticker,))
            conn.close()
            return df
            
        except Exception as e:
            logging.error(f"Error getting latest data for {ticker}: {e}")
            return pd.DataFrame()
    
    def get_financial_history(self, ticker: str, years: int = 5) -> pd.DataFrame:
        """Get financial history for a ticker over specified years"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = """
                SELECT * FROM balance_sheet_data 
                WHERE ticker = ? 
                AND report_date >= date('now', '-{} years')
                ORDER BY report_date DESC
            """.format(years)
            
            df = pd.read_sql_query(query, conn, params=(ticker,))
            conn.close()
            return df
            
        except Exception as e:
            logging.error(f"Error getting financial history for {ticker}: {e}")
            return pd.DataFrame()
    
    def get_database_stats(self) -> dict:
        """Get statistics about the balance sheet database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            stats = {}
            
            # Count announcements
            cursor.execute("SELECT COUNT(*) FROM financial_announcements")
            stats['total_announcements'] = cursor.fetchone()[0]
            
            # Count documents
            cursor.execute("SELECT COUNT(*) FROM financial_documents")
            stats['total_documents'] = cursor.fetchone()[0]
            
            # Count balance sheet records
            cursor.execute("SELECT COUNT(*) FROM balance_sheet_data")
            stats['total_balance_sheet_records'] = cursor.fetchone()[0]
            
            # Count unique tickers
            cursor.execute("SELECT COUNT(DISTINCT ticker) FROM financial_announcements")
            stats['unique_tickers'] = cursor.fetchone()[0]
            
            # Most recent data
            cursor.execute("SELECT MAX(scraped_at) FROM financial_announcements")
            stats['last_scraped'] = cursor.fetchone()[0]
            
            conn.close()
            return stats
            
        except Exception as e:
            logging.error(f"Error getting database stats: {e}")
            return {}

if __name__ == "__main__":
    # Test database initialization
    db = BalanceSheetDatabase()
    stats = db.get_database_stats()
    print("Balance Sheet Database Stats:")
    for key, value in stats.items():
        print(f"{key}: {value}")
