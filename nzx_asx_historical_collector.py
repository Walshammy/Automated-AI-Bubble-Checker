import pandas as pd
import numpy as np
import yfinance as yf
import sqlite3
import time
import os
import random
from datetime import datetime, timedelta, date
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import json
import warnings

warnings.filterwarnings('ignore')

@dataclass
class HistoricalDataPoint:
    """Single historical data point"""
    ticker: str
    date: date
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    adjusted_close: float
    market_cap: float
    pe_ratio: float
    pb_ratio: float
    peg_ratio: float
    dividend_yield: float
    roe: float
    debt_to_equity: float
    current_ratio: float
    fcf_yield: float
    eps_ttm: float
    eps_growth_5y: float
    revenue_growth_5y: float
    roa: float
    roic: float
    gross_margin: float
    operating_margin: float
    net_margin: float
    beta: float
    sector: str
    industry: str
    is_delisted: bool
    delisted_date: Optional[date]
    created_at: datetime

class ComprehensiveNZXASXCollector:
    """Comprehensive collector for all NZX and ASX stocks with aggressive rate limiting"""
    
    def __init__(self, db_path: str = "nzx_asx_historical_data.db"):
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Database setup
        self.db_path = db_path
        self.init_database()
        
        # Load stock universe from Excel file
        self.load_stock_universe()
        
        # Collection settings (very conservative to avoid rate limiting)
        self.start_date = date(2000, 1, 1)
        self.end_date = date.today()
        self.min_delay = 5  # Minimum 5 seconds between requests
        self.max_delay = 12  # Maximum 12 seconds between requests
        self.max_retries = 3
        self.batch_size = 3  # Process only 3 tickers at a time
        
        # Progress tracking
        self.progress_file = "nzx_asx_collection_progress.json"
        self.load_progress()
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'total_records': 0,
            'start_time': None,
            'last_update': None
        }
        
    def init_database(self):
        """Initialize optimized SQLite database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create main historical data table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS historical_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    date DATE NOT NULL,
                    open_price REAL,
                    high_price REAL,
                    low_price REAL,
                    close_price REAL,
                    volume INTEGER,
                    adjusted_close REAL,
                    market_cap REAL,
                    pe_ratio REAL,
                    pb_ratio REAL,
                    peg_ratio REAL,
                    dividend_yield REAL,
                    roe REAL,
                    debt_to_equity REAL,
                    current_ratio REAL,
                    fcf_yield REAL,
                    eps_ttm REAL,
                    eps_growth_5y REAL,
                    revenue_growth_5y REAL,
                    roa REAL,
                    roic REAL,
                    gross_margin REAL,
                    operating_margin REAL,
                    net_margin REAL,
                    beta REAL,
                    sector TEXT,
                    industry TEXT,
                    is_delisted BOOLEAN DEFAULT 0,
                    delisted_date DATE,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, date)
                )
            ''')
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker_date ON historical_data(ticker, date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON historical_data(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON historical_data(ticker)')
            
            # Create progress tracking table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS collection_progress (
                    ticker TEXT PRIMARY KEY,
                    status TEXT DEFAULT 'pending',
                    last_attempt DATETIME,
                    attempts INTEGER DEFAULT 0,
                    records_collected INTEGER DEFAULT 0,
                    last_error TEXT,
                    is_delisted BOOLEAN DEFAULT 0,
                    delisted_date DATE,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create collection log table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS collection_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_start DATETIME,
                    session_end DATETIME,
                    tickers_processed INTEGER,
                    records_added INTEGER,
                    errors_count INTEGER,
                    total_time_seconds REAL,
                    notes TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            self.logger.info(f"Database initialized: {self.db_path}")
            
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise
    
    def load_stock_universe(self):
        """Load stock universe from NZX_ASX.xlsx file"""
        try:
            # Load NZX stocks (Sheet1)
            nzx_df = pd.read_excel('NZX_ASX.xlsx', sheet_name='Sheet1')
            nzx_stocks = {}
            
            for _, row in nzx_df.iterrows():
                code = str(row['Code']).strip()
                company = str(row['Company']).strip()
                if code and company and code != 'nan':
                    # Add .NZ suffix for NZX stocks
                    ticker = f"{code}.NZ"
                    nzx_stocks[ticker] = company
            
            # Load ASX stocks (Sheet3)
            asx_df = pd.read_excel('NZX_ASX.xlsx', sheet_name='Sheet3')
            asx_stocks = {}
            
            for _, row in asx_df.iterrows():
                code = str(row['Code']).strip()
                company = str(row['Company']).strip()
                if code and company and code != 'nan':
                    # Add .AX suffix for ASX stocks
                    ticker = f"{code}.AX"
                    asx_stocks[ticker] = company
            
            # Combine all stocks
            self.stock_universe = {**nzx_stocks, **asx_stocks}
            
            self.logger.info(f"Loaded stock universe: {len(nzx_stocks)} NZX stocks, {len(asx_stocks)} ASX stocks")
            self.logger.info(f"Total stocks: {len(self.stock_universe)}")
            
        except Exception as e:
            self.logger.error(f"Error loading stock universe: {e}")
            raise
    
    def load_progress(self):
        """Load collection progress"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    self.progress = json.load(f)
            else:
                self.progress = {
                    'completed_tickers': [],
                    'failed_tickers': [],
                    'pending_tickers': list(self.stock_universe.keys()),
                    'session_start': None,
                    'total_records': 0
                }
        except Exception as e:
            self.logger.error(f"Error loading progress: {e}")
            self.progress = {
                'completed_tickers': [],
                'failed_tickers': [],
                'pending_tickers': list(self.stock_universe.keys()),
                'session_start': None,
                'total_records': 0
            }
    
    def save_progress(self):
        """Save collection progress"""
        try:
            self.progress['last_update'] = datetime.now().isoformat()
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving progress: {e}")
    
    def get_sector(self, ticker: str) -> str:
        """Determine sector based on ticker"""
        # This is a simplified sector mapping - in practice you'd want more comprehensive data
        if '.NZ' in ticker:
            return 'NZX'
        elif '.AX' in ticker:
            return 'ASX'
        else:
            return 'Unknown'
    
    def safe_get(self, data: dict, key: str, default: float = 0.0) -> float:
        """Safely extract numeric values from data"""
        value = data.get(key, default)
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return default
        return float(value)
    
    def calculate_fcf_yield(self, stock_info: dict) -> float:
        """Calculate FCF yield"""
        fcf = self.safe_get(stock_info, 'freeCashflow')
        market_cap = self.safe_get(stock_info, 'marketCap')
        
        if market_cap > 0 and fcf > 0:
            return (fcf / market_cap) * 100
        return 0.0
    
    def collect_ticker_data(self, ticker: str) -> Tuple[List[HistoricalDataPoint], bool, str]:
        """Collect historical data for a single ticker with robust error handling"""
        try:
            self.logger.info(f"Collecting data for {ticker}")
            
            # Get ticker info first
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Check if delisted
            is_delisted = False
            delisted_date = None
            
            if 'delistedDate' in info and info['delistedDate']:
                is_delisted = True
                delisted_date = pd.to_datetime(info['delistedDate']).date()
            elif 'regularMarketPrice' not in info or info.get('regularMarketPrice') is None:
                # Check if we can get recent data
                try:
                    recent_data = stock.history(period="1mo")
                    if recent_data.empty:
                        is_delisted = True
                except:
                    is_delisted = True
            
            # Get historical price data with multiple fallback strategies
            historical_data = None
            
            # Strategy 1: Try weekly data first
            try:
                historical_data = stock.history(start=self.start_date, end=self.end_date, interval="1wk")
                if not historical_data.empty:
                    self.logger.info(f"Got weekly data for {ticker}: {len(historical_data)} records")
            except Exception as e:
                self.logger.warning(f"Weekly data failed for {ticker}: {e}")
            
            # Strategy 2: Try daily data and resample
            if historical_data is None or historical_data.empty:
                try:
                    daily_data = stock.history(start=self.start_date, end=self.end_date)
                    if not daily_data.empty:
                        # Resample to weekly (Friday close)
                        historical_data = daily_data.resample('W-FRI').agg({
                            'Open': 'first',
                            'High': 'max',
                            'Low': 'min',
                            'Close': 'last',
                            'Volume': 'sum',
                            'Adj Close': 'last'
                        }).dropna()
                        self.logger.info(f"Got daily data and resampled for {ticker}: {len(historical_data)} records")
                except Exception as e:
                    self.logger.warning(f"Daily data failed for {ticker}: {e}")
            
            # Strategy 3: Try maximum available data
            if historical_data is None or historical_data.empty:
                try:
                    historical_data = stock.history(period="max")
                    if not historical_data.empty:
                        # Filter to our date range and resample
                        historical_data = historical_data[
                            (historical_data.index.date >= self.start_date) & 
                            (historical_data.index.date <= self.end_date)
                        ]
                        if not historical_data.empty:
                            historical_data = historical_data.resample('W-FRI').agg({
                                'Open': 'first',
                                'High': 'max',
                                'Low': 'min',
                                'Close': 'last',
                                'Volume': 'sum',
                                'Adj Close': 'last'
                            }).dropna()
                        self.logger.info(f"Got max period data for {ticker}: {len(historical_data)} records")
                except Exception as e:
                    self.logger.warning(f"Max period data failed for {ticker}: {e}")
            
            if historical_data is None or historical_data.empty:
                return [], is_delisted, "No historical data available"
            
            # Convert to data points
            data_points = []
            
            for date_idx, row in historical_data.iterrows():
                try:
                    # Handle missing Adj Close column gracefully
                    adj_close = row.get('Adj Close', row.get('Close', 0))
                    if pd.isna(adj_close):
                        adj_close = row.get('Close', 0)
                    
                    data_point = HistoricalDataPoint(
                        ticker=ticker,
                        date=date_idx.date(),
                        open_price=float(row['Open']) if not pd.isna(row['Open']) else 0,
                        high_price=float(row['High']) if not pd.isna(row['High']) else 0,
                        low_price=float(row['Low']) if not pd.isna(row['Low']) else 0,
                        close_price=float(row['Close']) if not pd.isna(row['Close']) else 0,
                        volume=int(row['Volume']) if not pd.isna(row['Volume']) else 0,
                        adjusted_close=float(adj_close) if not pd.isna(adj_close) else 0,
                        market_cap=self.safe_get(info, 'marketCap'),
                        pe_ratio=self.safe_get(info, 'trailingPE'),
                        pb_ratio=self.safe_get(info, 'priceToBook'),
                        peg_ratio=self.safe_get(info, 'pegRatio'),
                        dividend_yield=self.safe_get(info, 'dividendYield', 0) * 100,
                        roe=self.safe_get(info, 'returnOnEquity', 0) * 100,
                        debt_to_equity=self.safe_get(info, 'debtToEquity'),
                        current_ratio=self.safe_get(info, 'currentRatio'),
                        fcf_yield=self.calculate_fcf_yield(info),
                        eps_ttm=self.safe_get(info, 'trailingEps'),
                        eps_growth_5y=self.safe_get(info, 'earningsGrowth', 0) * 100,
                        revenue_growth_5y=self.safe_get(info, 'revenueGrowth', 0) * 100,
                        roa=self.safe_get(info, 'returnOnAssets', 0) * 100,
                        roic=self.safe_get(info, 'returnOnInvestedCapital', 0) * 100,
                        gross_margin=self.safe_get(info, 'grossMargins', 0) * 100,
                        operating_margin=self.safe_get(info, 'operatingMargins', 0) * 100,
                        net_margin=self.safe_get(info, 'profitMargins', 0) * 100,
                        beta=self.safe_get(info, 'beta', 1.0),
                        sector=self.get_sector(ticker),
                        industry=info.get('industry', 'Unknown'),
                        is_delisted=is_delisted,
                        delisted_date=delisted_date,
                        created_at=datetime.now()
                    )
                    
                    data_points.append(data_point)
                    
                except Exception as e:
                    self.logger.warning(f"Error processing data point for {ticker} on {date_idx.date()}: {e}")
                    continue
            
            self.logger.info(f"Successfully collected {len(data_points)} data points for {ticker}")
            return data_points, is_delisted, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting data for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            return [], True, error_msg
    
    def save_data_points(self, data_points: List[HistoricalDataPoint], ticker: str, status: str, error_msg: str = ""):
        """Save data points to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if data_points:
                # Prepare data for bulk insert
                data_to_insert = []
                for dp in data_points:
                    data_to_insert.append((
                        dp.ticker, dp.date, dp.open_price, dp.high_price, dp.low_price,
                        dp.close_price, dp.volume, dp.adjusted_close, dp.market_cap,
                        dp.pe_ratio, dp.pb_ratio, dp.peg_ratio, dp.dividend_yield,
                        dp.roe, dp.debt_to_equity, dp.current_ratio, dp.fcf_yield,
                        dp.eps_ttm, dp.eps_growth_5y, dp.revenue_growth_5y, dp.roa,
                        dp.roic, dp.gross_margin, dp.operating_margin, dp.net_margin,
                        dp.beta, dp.sector, dp.industry, dp.is_delisted, dp.delisted_date
                    ))
                
                # Bulk insert
                cursor.executemany('''
                    INSERT OR REPLACE INTO historical_data (
                        ticker, date, open_price, high_price, low_price, close_price,
                        volume, adjusted_close, market_cap, pe_ratio, pb_ratio, peg_ratio,
                        dividend_yield, roe, debt_to_equity, current_ratio, fcf_yield,
                        eps_ttm, eps_growth_5y, revenue_growth_5y, roa, roic,
                        gross_margin, operating_margin, net_margin, beta, sector,
                        industry, is_delisted, delisted_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data_to_insert)
            
            # Update progress
            cursor.execute('''
                INSERT OR REPLACE INTO collection_progress (
                    ticker, status, last_attempt, attempts, records_collected,
                    last_error, is_delisted, delisted_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ticker, status, datetime.now(), 1, len(data_points),
                error_msg, data_points[0].is_delisted if data_points else False,
                data_points[0].delisted_date if data_points else None
            ))
            
            conn.commit()
            conn.close()
            
            return len(data_points)
            
        except Exception as e:
            self.logger.error(f"Error saving data points for {ticker}: {e}")
            return 0
    
    def smart_delay(self):
        """Implement smart delay with randomization"""
        delay = random.uniform(self.min_delay, self.max_delay)
        self.logger.info(f"Rate limiting: waiting {delay:.1f} seconds...")
        time.sleep(delay)
    
    def run_collection_session(self, max_tickers: int = None):
        """Run a collection session with smart rate limiting"""
        session_start = datetime.now()
        self.stats['start_time'] = session_start
        
        # Get pending tickers
        pending_tickers = [t for t in self.stock_universe.keys() 
                          if t not in self.progress['completed_tickers']]
        
        if max_tickers:
            pending_tickers = pending_tickers[:max_tickers]
        
        self.logger.info(f"Starting NZX/ASX collection session for {len(pending_tickers)} tickers")
        self.logger.info(f"Rate limiting: {self.min_delay}-{self.max_delay} seconds between requests")
        self.logger.info(f"Batch size: {self.batch_size} tickers per session")
        
        session_records = 0
        session_errors = 0
        
        for i, ticker in enumerate(pending_tickers):
            try:
                self.logger.info(f"Processing {ticker} ({i+1}/{len(pending_tickers)}) - {self.stock_universe[ticker]}")
                
                # Collect data
                data_points, is_delisted, error_msg = self.collect_ticker_data(ticker)
                
                # Save data
                records_saved = self.save_data_points(data_points, ticker, 
                                                    "completed" if data_points else "failed", 
                                                    error_msg)
                
                # Update statistics
                if data_points:
                    self.progress['completed_tickers'].append(ticker)
                    session_records += records_saved
                    self.stats['successful'] += 1
                    self.stats['total_records'] += records_saved
                    self.logger.info(f"✓ {ticker}: {records_saved} records saved")
                else:
                    self.progress['failed_tickers'].append(ticker)
                    session_errors += 1
                    self.stats['failed'] += 1
                    self.logger.warning(f"✗ {ticker}: {error_msg}")
                
                self.stats['total_processed'] += 1
                self.stats['last_update'] = datetime.now()
                
                # Save progress
                self.save_progress()
                
                # Smart delay (except for last ticker)
                if i < len(pending_tickers) - 1:
                    self.smart_delay()
                
            except Exception as e:
                self.logger.error(f"Unexpected error processing {ticker}: {e}")
                self.progress['failed_tickers'].append(ticker)
                session_errors += 1
                self.stats['failed'] += 1
                self.stats['total_processed'] += 1
        
        # Log session results
        session_end = datetime.now()
        session_duration = (session_end - session_start).total_seconds()
        
        self.log_collection_session(session_start, session_end, len(pending_tickers),
                                  session_records, session_errors, session_duration)
        
        self.logger.info(f"Session complete: {session_records} records, {session_errors} errors, {session_duration:.1f}s")
        
        return session_records, session_errors
    
    def log_collection_session(self, start_time: datetime, end_time: datetime,
                             tickers_processed: int, records_added: int,
                             errors_count: int, total_time: float):
        """Log collection session statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO collection_log (
                    session_start, session_end, tickers_processed,
                    records_added, errors_count, total_time_seconds, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                start_time, end_time, tickers_processed,
                records_added, errors_count, total_time,
                f"NZX/ASX collection session - Rate limited"
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error logging session: {e}")
    
    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get basic stats
            cursor.execute('SELECT COUNT(*) FROM historical_data')
            total_records = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM historical_data')
            unique_tickers = cursor.fetchone()[0]
            
            cursor.execute('SELECT MIN(date), MAX(date) FROM historical_data')
            date_range = cursor.fetchone()
            
            cursor.execute('SELECT COUNT(*) FROM historical_data WHERE is_delisted = 1')
            delisted_records = cursor.fetchone()[0]
            
            # Get records per ticker
            cursor.execute('''
                SELECT ticker, COUNT(*) as record_count, MIN(date) as first_date, MAX(date) as last_date
                FROM historical_data 
                GROUP BY ticker 
                ORDER BY record_count DESC
            ''')
            ticker_stats = cursor.fetchall()
            
            conn.close()
            
            return {
                'total_records': total_records,
                'unique_tickers': unique_tickers,
                'date_range': date_range,
                'delisted_records': delisted_records,
                'ticker_stats': ticker_stats
            }
            
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {}
    
    def print_progress_summary(self):
        """Print comprehensive progress summary"""
        print("\n" + "="*80)
        print("NZX/ASX HISTORICAL DATA COLLECTION PROGRESS")
        print("="*80)
        
        print(f"Total Tickers: {len(self.stock_universe)}")
        print(f"Completed: {len(self.progress['completed_tickers'])}")
        print(f"Failed: {len(self.progress['failed_tickers'])}")
        print(f"Pending: {len(self.progress['pending_tickers']) - len(self.progress['completed_tickers']) - len(self.progress['failed_tickers'])}")
        
        print(f"\nSession Statistics:")
        print(f"Total Processed: {self.stats['total_processed']}")
        print(f"Successful: {self.stats['successful']}")
        print(f"Failed: {self.stats['failed']}")
        print(f"Total Records: {self.stats['total_records']:,}")
        
        if self.stats['start_time']:
            duration = datetime.now() - self.stats['start_time']
            print(f"Session Duration: {duration}")
        
        # Database stats
        db_stats = self.get_database_stats()
        if db_stats:
            print(f"\nDatabase Statistics:")
            print(f"Total Records: {db_stats['total_records']:,}")
            print(f"Unique Tickers: {db_stats['unique_tickers']}")
            if db_stats['date_range'][0]:
                print(f"Date Range: {db_stats['date_range'][0]} to {db_stats['date_range'][1]}")
            
            if db_stats['ticker_stats']:
                print(f"\nTop 10 Tickers by Record Count:")
                for i, (ticker, count, first_date, last_date) in enumerate(db_stats['ticker_stats'][:10]):
                    print(f"{i+1:2d}. {ticker:<8} - {count:4d} records ({first_date} to {last_date})")
        
        print("="*80)

def main():
    """Main function for NZX/ASX historical data collection"""
    collector = ComprehensiveNZXASXCollector()
    
    print("="*80)
    print("COMPREHENSIVE NZX/ASX HISTORICAL DATA COLLECTION")
    print("="*80)
    print(f"Total Tickers: {len(collector.stock_universe)}")
    print(f"Date Range: {collector.start_date} to {collector.end_date}")
    print(f"Rate Limiting: {collector.min_delay}-{collector.max_delay} seconds between requests")
    print(f"Batch Size: {collector.batch_size} tickers per session")
    print("="*80)
    
    # Run collection session (process 3 tickers at a time to avoid rate limiting)
    records_collected, errors = collector.run_collection_session(max_tickers=3)
    
    # Print summary
    collector.print_progress_summary()
    
    print(f"\nCollection session complete!")
    print(f"Records collected: {records_collected:,}")
    print(f"Errors: {errors}")
    print(f"Database: {collector.db_path}")
    print(f"Progress file: {collector.progress_file}")
    
    print(f"\nTo continue collection, run this script again.")
    print(f"It will automatically resume from where it left off.")
    print(f"Estimated time for all {len(collector.stock_universe)} tickers: {len(collector.stock_universe) * 8 / 3600:.1f} hours")

if __name__ == "__main__":
    main()
