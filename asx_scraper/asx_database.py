"""
Database management for ASX announcements
Based on the successful NZX balance sheet database approach
"""
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional
import logging
import asx_config as config

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT
)

class ASXDatabase:
    """Database manager for ASX announcements - adapted from NZX approach"""
    
    def __init__(self, db_path: str = config.DATABASE_PATH):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self.create_tables()
    
    def get_connection(self):
        """Create database connection"""
        return sqlite3.connect(self.db_path)
    
    def create_tables(self):
        """Create database tables - enhanced schema based on NZX success"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Main announcements table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS asx_announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                announcement_id TEXT UNIQUE NOT NULL,
                ticker TEXT NOT NULL,
                company_name TEXT,
                announcement_date TIMESTAMP,
                title TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL,
                pdf_filename TEXT,
                file_size TEXT,
                market_sensitive BOOLEAN,
                is_financial_report BOOLEAN,
                is_balance_sheet BOOLEAN DEFAULT FALSE,
                download_status TEXT DEFAULT 'pending',
                exchange TEXT DEFAULT 'ASX',
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed BOOLEAN DEFAULT FALSE,
                UNIQUE(ticker, url)
            )
        ''')
        
        # PDF documents table (similar to NZX approach)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS asx_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                announcement_id TEXT NOT NULL,
                pdf_url TEXT,
                pdf_filename TEXT,
                pdf_path TEXT,
                document_type TEXT,
                download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'downloaded',
                file_size_kb INTEGER,
                FOREIGN KEY (announcement_id) REFERENCES asx_announcements(announcement_id)
            )
        ''')
        
        # Financial data table (for future PDF processing)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS asx_financial_data (
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
                
                FOREIGN KEY (announcement_id) REFERENCES asx_announcements(announcement_id)
            )
        ''')
        
        # Create indices for better performance
        indices = [
            "CREATE INDEX IF NOT EXISTS idx_ticker ON asx_announcements(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_date ON asx_announcements(announcement_date)",
            "CREATE INDEX IF NOT EXISTS idx_financial ON asx_announcements(is_financial_report)",
            "CREATE INDEX IF NOT EXISTS idx_download_status ON asx_announcements(download_status)",
            "CREATE INDEX IF NOT EXISTS idx_announcement_id ON asx_announcements(announcement_id)",
            "CREATE INDEX IF NOT EXISTS idx_financial_data_ticker ON asx_financial_data(ticker)",
            "CREATE INDEX IF NOT EXISTS idx_financial_data_date ON asx_financial_data(report_date)"
        ]
        
        for index_sql in indices:
            cursor.execute(index_sql)
        
        conn.commit()
        conn.close()
        self.logger.info("ASX database tables initialized")
    
    def insert_announcement(self, announcement: Dict) -> bool:
        """Insert announcement into database"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO asx_announcements 
                (announcement_id, ticker, company_name, announcement_date, title, url, 
                 file_size, market_sensitive, is_financial_report, is_balance_sheet, pdf_filename)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                announcement['announcement_id'],
                announcement['ticker'],
                announcement.get('company_name'),
                announcement['announcement_date'],
                announcement['title'],
                announcement['url'],
                announcement.get('file_size'),
                announcement.get('market_sensitive', False),
                announcement.get('is_financial_report', False),
                announcement.get('is_balance_sheet', False),
                announcement.get('pdf_filename')
            ))
            
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.IntegrityError:
            return False
        except Exception as e:
            self.logger.error(f"Error inserting announcement: {e}")
            return False
        finally:
            conn.close()
    
    def get_pending_downloads(self) -> List[Dict]:
        """Get announcements that need to be downloaded"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, announcement_id, ticker, title, url, pdf_filename
            FROM asx_announcements
            WHERE download_status = 'pending' AND is_financial_report = 1
        ''')
        
        columns = [description[0] for description in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return results
    
    def update_download_status(self, announcement_id: str, status: str, filename: str = None):
        """Update download status"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if filename:
                cursor.execute('''
                    UPDATE asx_announcements 
                    SET download_status = ?, pdf_filename = ?
                    WHERE announcement_id = ?
                ''', (status, filename, announcement_id))
            else:
                cursor.execute('''
                    UPDATE asx_announcements 
                    SET download_status = ?
                    WHERE announcement_id = ?
                ''', (status, announcement_id))
            
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error updating download status: {e}")
        finally:
            conn.close()
    
    def get_statistics(self) -> Dict:
        """Get database statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT COUNT(*) FROM asx_announcements')
            total = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM asx_announcements WHERE is_financial_report = 1')
            financial = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM asx_announcements WHERE is_balance_sheet = 1')
            balance_sheets = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM asx_announcements WHERE download_status = "downloaded"')
            downloaded = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM asx_announcements')
            companies = cursor.fetchone()[0]
            
            cursor.execute('SELECT MAX(scraped_at) FROM asx_announcements')
            last_scraped = cursor.fetchone()[0]
            
            return {
                'total_announcements': total,
                'financial_reports': financial,
                'balance_sheet_reports': balance_sheets,
                'downloaded': downloaded,
                'companies_covered': companies,
                'last_scraped': last_scraped
            }
        except Exception as e:
            self.logger.error(f"Error getting statistics: {e}")
            return {}
        finally:
            conn.close()
    
    def get_tickers_with_data(self) -> List[str]:
        """Get list of tickers that have announcements"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT DISTINCT ticker FROM asx_announcements ORDER BY ticker')
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error getting tickers: {e}")
            return []
        finally:
            conn.close()
    
    def get_announcements_by_ticker(self, ticker: str) -> List[Dict]:
        """Get all announcements for a specific ticker"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT * FROM asx_announcements 
                WHERE ticker = ? 
                ORDER BY announcement_date DESC
            ''', (ticker,))
            
            columns = [description[0] for description in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return results
        except Exception as e:
            self.logger.error(f"Error getting announcements for {ticker}: {e}")
            return []
        finally:
            conn.close()
    
    def insert_financial_data(self, financial_data: Dict) -> bool:
        """Insert financial data extracted from PDFs (for future use)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO asx_financial_data
                (ticker, announcement_id, report_date, report_type, statement_type,
                 total_assets, current_assets, non_current_assets, cash_and_equivalents,
                 accounts_receivable, inventory, total_liabilities, current_liabilities,
                 non_current_liabilities, accounts_payable, long_term_debt, total_equity,
                 retained_earnings, revenue, gross_profit, operating_income, ebitda,
                 ebit, net_income, operating_cash_flow, investing_cash_flow,
                 financing_cash_flow, free_cash_flow, current_ratio, quick_ratio,
                 debt_to_equity, return_on_assets, return_on_equity, gross_margin,
                 operating_margin, net_margin, extraction_confidence, data_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
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
            return True
        except Exception as e:
            self.logger.error(f"Error inserting financial data: {e}")
            return False
        finally:
            conn.close()


if __name__ == "__main__":
    # Test database initialization
    db = ASXDatabase()
    stats = db.get_statistics()
    print("ASX Database Stats:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
