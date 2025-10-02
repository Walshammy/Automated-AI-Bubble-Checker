#!/usr/bin/env python3
"""
Improved Comprehensive Data Collector
Properly separates historical price data from current fundamentals
Addresses data quality, rate limiting, and memory management issues
"""

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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

warnings.filterwarnings('ignore')

@dataclass
class HistoricalPricePoint:
    """Historical price data point - accurate for time series analysis"""
    ticker: str
    date: date
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    adjusted_close: float
    exchange: str
    is_delisted: bool
    delisted_date: Optional[date]
    created_at: datetime

@dataclass
class CurrentFundamentals:
    """Current fundamental metrics - point-in-time snapshot"""
    ticker: str
    snapshot_date: date
    market_cap: float
    pe_ratio: float
    pb_ratio: float
    peg_ratio: float
    ps_ratio: float
    dividend_yield: float
    roe: float
    roa: float
    roic: float
    debt_to_equity: float
    current_ratio: float
    fcf_yield: float
    eps_ttm: float
    eps_growth_5y: float
    revenue_growth_5y: float
    gross_margin: float
    operating_margin: float
    net_margin: float
    beta: float
    volatility_1y: float
    max_drawdown_5y: float
    sector: str
    industry: str
    exchange: str
    is_delisted: bool
    delisted_date: Optional[date]
    created_at: datetime

class ImprovedDataCollector:
    """Improved data collector with proper architecture and error handling"""
    
    def __init__(self, db_path: str = "data_collection/improved_data.db"):
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Database setup
        self.db_path = db_path
        self.init_database()
        
        # Load stock universe
        self.load_stock_universe()
        
        # Improved rate limiting settings
        self.base_delay = 1.0  # Base delay in seconds
        self.max_delay = 10.0  # Maximum delay
        self.backoff_multiplier = 1.5  # Exponential backoff multiplier
        self.max_retries = 5
        self.batch_size = 5  # Smaller batches for better rate limiting
        self.max_workers = 1  # Start conservative, increase gradually
        
        # Rate limiting state
        self.consecutive_failures = 0
        self.last_request_time = 0
        
        # Progress tracking
        self.progress_file = "data_collection/improved_collection_progress.json"
        self.load_progress()
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'rate_limited': 0,
            'total_records': 0,
            'start_time': None,
            'last_update': None
        }
        
        # Thread safety
        self.lock = threading.Lock()
        
        # Setup requests session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
    def init_database(self):
        """Initialize improved database with separate tables"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Historical price data table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS historical_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    date DATE NOT NULL,
                    open_price REAL,
                    high_price REAL,
                    low_price REAL,
                    close_price REAL,
                    volume INTEGER,
                    adjusted_close REAL,
                    exchange TEXT,
                    is_delisted BOOLEAN DEFAULT 0,
                    delisted_date DATE,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, date)
                )
            ''')
            
            # Current fundamentals table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS current_fundamentals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    snapshot_date DATE NOT NULL,
                    market_cap REAL,
                    pe_ratio REAL,
                    pb_ratio REAL,
                    peg_ratio REAL,
                    ps_ratio REAL,
                    dividend_yield REAL,
                    roe REAL,
                    roa REAL,
                    roic REAL,
                    debt_to_equity REAL,
                    current_ratio REAL,
                    fcf_yield REAL,
                    eps_ttm REAL,
                    eps_growth_5y REAL,
                    revenue_growth_5y REAL,
                    gross_margin REAL,
                    operating_margin REAL,
                    net_margin REAL,
                    beta REAL,
                    volatility_1y REAL,
                    max_drawdown_5y REAL,
                    sector TEXT,
                    industry TEXT,
                    exchange TEXT,
                    is_delisted BOOLEAN DEFAULT 0,
                    delisted_date DATE,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, snapshot_date)
                )
            ''')
            
            # Create indexes for performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON historical_prices(ticker, date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_prices_date ON historical_prices(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_fundamentals_ticker ON current_fundamentals(ticker)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_fundamentals_date ON current_fundamentals(snapshot_date)')
            
            # Progress tracking table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS collection_progress (
                    ticker TEXT PRIMARY KEY,
                    status TEXT DEFAULT 'pending',
                    last_attempt DATETIME,
                    attempts INTEGER DEFAULT 0,
                    price_records INTEGER DEFAULT 0,
                    fundamental_records INTEGER DEFAULT 0,
                    last_error TEXT,
                    is_delisted BOOLEAN DEFAULT 0,
                    delisted_date DATE,
                    exchange TEXT,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            self.logger.info(f"Improved database initialized: {self.db_path}")
            
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise
    
    def load_stock_universe(self):
        """Load stock universe from Excel file"""
        try:
            # Load NZX stocks
            nzx_df = pd.read_excel('data_collection/NZX_ASX.xlsx', sheet_name='Sheet1')
            nzx_stocks = {}
            
            for _, row in nzx_df.iterrows():
                code = str(row['Code']).strip()
                company = str(row['Company']).strip()
                if code and company and code != 'nan' and code != 'Code':
                    ticker = f"{code}.NZ"
                    nzx_stocks[ticker] = {
                        'company': company,
                        'exchange': 'NZX',
                        'code': code,
                        'market_cap': row.get('Capitalisation', None)
                    }
            
            # Load ASX stocks
            asx_df = pd.read_excel('data_collection/NZX_ASX.xlsx', sheet_name='Sheet3')
            asx_stocks = {}
            
            for _, row in asx_df.iterrows():
                code = str(row['Code']).strip()
                company = str(row['Company']).strip()
                if code and company and code != 'nan' and code != 'Code':
                    ticker = f"{code}.AX"
                    asx_stocks[ticker] = {
                        'company': company,
                        'exchange': 'ASX',
                        'code': code,
                        'sector': row.get('Sector', None),
                        'market_cap': row.get('Mkt Cap', None)
                    }
            
            # Add major US stocks
            us_stocks = {
                'AAPL': {'company': 'Apple Inc.', 'exchange': 'NASDAQ', 'sector': 'Technology'},
                'MSFT': {'company': 'Microsoft Corporation', 'exchange': 'NASDAQ', 'sector': 'Technology'},
                'GOOGL': {'company': 'Alphabet Inc.', 'exchange': 'NASDAQ', 'sector': 'Technology'},
                'AMZN': {'company': 'Amazon.com Inc.', 'exchange': 'NASDAQ', 'sector': 'Consumer'},
                'TSLA': {'company': 'Tesla Inc.', 'exchange': 'NASDAQ', 'sector': 'Automotive'},
                'NVDA': {'company': 'NVIDIA Corporation', 'exchange': 'NASDAQ', 'sector': 'Technology'},
                'META': {'company': 'Meta Platforms Inc.', 'exchange': 'NASDAQ', 'sector': 'Technology'},
                'BRK-B': {'company': 'Berkshire Hathaway Class B', 'exchange': 'NYSE', 'sector': 'Financial'},
                'JNJ': {'company': 'Johnson & Johnson', 'exchange': 'NYSE', 'sector': 'Healthcare'},
                'PG': {'company': 'Procter & Gamble Company', 'exchange': 'NYSE', 'sector': 'Consumer'},
            }
            
            # Combine all stocks
            self.stock_universe = {**nzx_stocks, **asx_stocks, **us_stocks}
            
            self.logger.info(f"Loaded stock universe: {len(nzx_stocks)} NZX, {len(asx_stocks)} ASX, {len(us_stocks)} US stocks")
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
                    'total_price_records': 0,
                    'total_fundamental_records': 0
                }
        except Exception as e:
            self.logger.error(f"Error loading progress: {e}")
            self.progress = {
                'completed_tickers': [],
                'failed_tickers': [],
                'pending_tickers': list(self.stock_universe.keys()),
                'session_start': None,
                'total_price_records': 0,
                'total_fundamental_records': 0
            }
    
    def save_progress(self):
        """Save collection progress"""
        try:
            self.progress['last_update'] = datetime.now().isoformat()
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving progress: {e}")
    
    def checkpoint_database(self):
        """Checkpoint database for integrity"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA wal_checkpoint(FULL)")
            conn.close()
            self.logger.debug("Database checkpoint completed")
        except Exception as e:
            self.logger.error(f"Error during database checkpoint: {e}")
    
    def adaptive_delay(self):
        """Implement adaptive delay with exponential backoff"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        # Calculate delay based on consecutive failures
        if self.consecutive_failures > 0:
            delay = min(self.base_delay * (self.backoff_multiplier ** self.consecutive_failures), self.max_delay)
        else:
            delay = self.base_delay
        
        # Add randomization to avoid thundering herd
        delay += random.uniform(0, delay * 0.1)
        
        # Ensure minimum time between requests
        if time_since_last < delay:
            time.sleep(delay - time_since_last)
        
        self.last_request_time = time.time()
        self.logger.debug(f"Adaptive delay: {delay:.2f}s (failures: {self.consecutive_failures})")
    
    def safe_get(self, data: dict, key: str, default: float = 0.0) -> float:
        """Safely extract numeric values from data"""
        value = data.get(key, default)
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return default
        return float(value)
    
    def check_delisting_status(self, ticker: str) -> Tuple[bool, Optional[date]]:
        """Check if stock is delisted with improved detection"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Check for explicit delisting date
            if 'delistedDate' in info and info['delistedDate']:
                return True, pd.to_datetime(info['delistedDate']).date()
            
            # Check if stock is currently trading
            try:
                recent_data = stock.history(period="5d")
                if recent_data.empty:
                    return True, None
                
                # Check if last price is reasonable (not $0 or NaN)
                last_price = recent_data['Close'].iloc[-1]
                if pd.isna(last_price) or last_price <= 0:
                    return True, None
                    
            except Exception:
                return True, None
            
            return False, None
            
        except Exception as e:
            self.logger.warning(f"Error checking delisting status for {ticker}: {e}")
            return False, None
    
    def collect_historical_prices(self, ticker: str) -> Tuple[List[HistoricalPricePoint], bool, str]:
        """Collect historical price data only"""
        try:
            self.logger.info(f"Collecting historical prices for {ticker}")
            
            # Check delisting status first
            is_delisted, delisted_date = self.check_delisting_status(ticker)
            
            stock = yf.Ticker(ticker)
            
            # Get historical data with multiple strategies
            historical_data = None
            
            # Strategy 1: Try weekly data first (most efficient)
            try:
                historical_data = stock.history(start=date(2000, 1, 1), end=date.today(), interval="1wk")
                if not historical_data.empty:
                    self.logger.info(f"Got weekly data for {ticker}: {len(historical_data)} records")
            except Exception as e:
                self.logger.warning(f"Weekly data failed for {ticker}: {e}")
            
            # Strategy 2: Try daily data and resample
            if historical_data is None or historical_data.empty:
                try:
                    daily_data = stock.history(start=date(2000, 1, 1), end=date.today())
                    if not daily_data.empty:
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
            
            if historical_data is None or historical_data.empty:
                return [], is_delisted, "No historical data available"
            
            # Convert to price points
            price_points = []
            metadata = self.stock_universe.get(ticker, {})
            exchange = metadata.get('exchange', 'Unknown')
            
            for date_idx, row in historical_data.iterrows():
                try:
                    adj_close = row.get('Adj Close', row.get('Close', 0))
                    if pd.isna(adj_close):
                        adj_close = row.get('Close', 0)
                    
                    price_point = HistoricalPricePoint(
                        ticker=ticker,
                        date=date_idx.date(),
                        open_price=float(row['Open']) if not pd.isna(row['Open']) else 0,
                        high_price=float(row['High']) if not pd.isna(row['High']) else 0,
                        low_price=float(row['Low']) if not pd.isna(row['Low']) else 0,
                        close_price=float(row['Close']) if not pd.isna(row['Close']) else 0,
                        volume=int(row['Volume']) if not pd.isna(row['Volume']) else 0,
                        adjusted_close=float(adj_close) if not pd.isna(adj_close) else 0,
                        exchange=exchange,
                        is_delisted=is_delisted,
                        delisted_date=delisted_date,
                        created_at=datetime.now()
                    )
                    
                    price_points.append(price_point)
                    
                except Exception as e:
                    self.logger.warning(f"Error processing price point for {ticker} on {date_idx.date()}: {e}")
                    continue
            
            self.logger.info(f"Successfully collected {len(price_points)} price points for {ticker}")
            return price_points, is_delisted, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting historical prices for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            return [], True, error_msg
    
    def collect_current_fundamentals(self, ticker: str) -> Tuple[Optional[CurrentFundamentals], bool, str]:
        """Collect current fundamental metrics only"""
        try:
            self.logger.info(f"Collecting current fundamentals for {ticker}")
            
            # Check delisting status
            is_delisted, delisted_date = self.check_delisting_status(ticker)
            
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Get historical data for volatility and drawdown calculations
            hist_data = None
            try:
                hist_data = stock.history(period="5y", interval="1mo")
            except Exception as e:
                self.logger.warning(f"Could not get historical data for {ticker}: {e}")
            
            # Extract fundamental metrics
            metadata = self.stock_universe.get(ticker, {})
            
            fundamentals = CurrentFundamentals(
                ticker=ticker,
                snapshot_date=date.today(),
                market_cap=self.safe_get(info, 'marketCap'),
                pe_ratio=self.safe_get(info, 'trailingPE'),
                pb_ratio=self.safe_get(info, 'priceToBook'),
                peg_ratio=self.safe_get(info, 'pegRatio'),
                ps_ratio=self.safe_get(info, 'priceToSalesTrailing12Months'),
                dividend_yield=self.safe_get(info, 'dividendYield', 0) * 100,
                roe=self.safe_get(info, 'returnOnEquity', 0) * 100,
                roa=self.safe_get(info, 'returnOnAssets', 0) * 100,
                roic=self.safe_get(info, 'returnOnInvestedCapital', 0) * 100,
                debt_to_equity=self.safe_get(info, 'debtToEquity'),
                current_ratio=self.safe_get(info, 'currentRatio'),
                fcf_yield=self.calculate_fcf_yield(info),
                eps_ttm=self.safe_get(info, 'trailingEps'),
                eps_growth_5y=self.safe_get(info, 'earningsGrowth', 0) * 100,
                revenue_growth_5y=self.safe_get(info, 'revenueGrowth', 0) * 100,
                gross_margin=self.safe_get(info, 'grossMargins', 0) * 100,
                operating_margin=self.safe_get(info, 'operatingMargins', 0) * 100,
                net_margin=self.safe_get(info, 'profitMargins', 0) * 100,
                beta=self.safe_get(info, 'beta', 1.0),
                volatility_1y=self.calculate_volatility(hist_data) if hist_data is not None else 0.0,
                max_drawdown_5y=self.calculate_max_drawdown(hist_data) if hist_data is not None else 0.0,
                sector=metadata.get('sector', 'Unknown'),
                industry=info.get('industry', 'Unknown'),
                exchange=metadata.get('exchange', 'Unknown'),
                is_delisted=is_delisted,
                delisted_date=delisted_date,
                created_at=datetime.now()
            )
            
            self.logger.info(f"Successfully collected fundamentals for {ticker}")
            return fundamentals, is_delisted, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting fundamentals for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            return None, True, error_msg
    
    def calculate_fcf_yield(self, stock_info: dict) -> float:
        """Calculate FCF yield"""
        fcf = self.safe_get(stock_info, 'freeCashflow')
        market_cap = self.safe_get(stock_info, 'marketCap')
        
        if market_cap > 0 and fcf > 0:
            return (fcf / market_cap) * 100
        return 0.0
    
    def calculate_volatility(self, hist_data: pd.DataFrame) -> float:
        """Calculate 1-year volatility"""
        try:
            if hist_data is not None and len(hist_data) > 12:
                returns = hist_data['Close'].pct_change().dropna()
                return returns.std() * np.sqrt(12) * 100  # Annualized monthly volatility
            return 0.0
        except:
            return 0.0
    
    def calculate_max_drawdown(self, hist_data: pd.DataFrame) -> float:
        """Calculate maximum drawdown"""
        try:
            if hist_data is not None and not hist_data.empty:
                peak = hist_data['Close'].expanding().max()
                drawdown = (hist_data['Close'] - peak) / peak
                return drawdown.min() * 100
            return 0.0
        except:
            return 0.0
    
    def save_price_points_streaming(self, price_points: List[HistoricalPricePoint], ticker: str) -> int:
        """Stream price points to database in chunks"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            chunk_size = 1000
            total_saved = 0
            
            for i in range(0, len(price_points), chunk_size):
                chunk = price_points[i:i + chunk_size]
                
                # Prepare data for bulk insert
                data_to_insert = []
                for pp in chunk:
                    data_to_insert.append((
                        pp.ticker, pp.date, pp.open_price, pp.high_price, pp.low_price,
                        pp.close_price, pp.volume, pp.adjusted_close, pp.exchange,
                        pp.is_delisted, pp.delisted_date
                    ))
                
                # Bulk insert chunk
                cursor.executemany('''
                    INSERT OR REPLACE INTO historical_prices (
                        ticker, date, open_price, high_price, low_price, close_price,
                        volume, adjusted_close, exchange, is_delisted, delisted_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data_to_insert)
                
                total_saved += len(chunk)
            
            conn.commit()
            conn.close()
            
            return total_saved
            
        except Exception as e:
            self.logger.error(f"Error saving price points for {ticker}: {e}")
            return 0
    
    def save_fundamentals(self, fundamentals: CurrentFundamentals, ticker: str) -> int:
        """Save current fundamentals to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO current_fundamentals (
                    ticker, snapshot_date, market_cap, pe_ratio, pb_ratio, peg_ratio,
                    ps_ratio, dividend_yield, roe, roa, roic, debt_to_equity, current_ratio,
                    fcf_yield, eps_ttm, eps_growth_5y, revenue_growth_5y, gross_margin,
                    operating_margin, net_margin, beta, volatility_1y, max_drawdown_5y,
                    sector, industry, exchange, is_delisted, delisted_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                fundamentals.ticker, fundamentals.snapshot_date, fundamentals.market_cap,
                fundamentals.pe_ratio, fundamentals.pb_ratio, fundamentals.peg_ratio,
                fundamentals.ps_ratio, fundamentals.dividend_yield, fundamentals.roe,
                fundamentals.roa, fundamentals.roic, fundamentals.debt_to_equity,
                fundamentals.current_ratio, fundamentals.fcf_yield, fundamentals.eps_ttm,
                fundamentals.eps_growth_5y, fundamentals.revenue_growth_5y, fundamentals.gross_margin,
                fundamentals.operating_margin, fundamentals.net_margin, fundamentals.beta,
                fundamentals.volatility_1y, fundamentals.max_drawdown_5y, fundamentals.sector,
                fundamentals.industry, fundamentals.exchange, fundamentals.is_delisted,
                fundamentals.delisted_date
            ))
            
            conn.commit()
            conn.close()
            
            return 1
            
        except Exception as e:
            self.logger.error(f"Error saving fundamentals for {ticker}: {e}")
            return 0
    
    def process_ticker(self, ticker: str) -> Tuple[int, int, bool, str]:
        """Process a single ticker with improved error handling"""
        try:
            self.logger.info(f"Processing {ticker} - {self.stock_universe.get(ticker, {}).get('company', ticker)}")
            
            # Apply adaptive delay
            self.adaptive_delay()
            
            # Collect historical prices
            price_points, is_delisted, price_error = self.collect_historical_prices(ticker)
            
            # Collect current fundamentals (only if not delisted)
            fundamentals = None
            fundamental_error = ""
            if not is_delisted:
                fundamentals, _, fundamental_error = self.collect_current_fundamentals(ticker)
            
            # Save data
            price_records = 0
            fundamental_records = 0
            
            if price_points:
                price_records = self.save_price_points_streaming(price_points, ticker)
            
            if fundamentals:
                fundamental_records = self.save_fundamentals(fundamentals, ticker)
            
            # Update progress
            cursor = sqlite3.connect(self.db_path).cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO collection_progress (
                    ticker, status, last_attempt, attempts, price_records,
                    fundamental_records, last_error, is_delisted, delisted_date, exchange
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ticker, "completed", datetime.now(), 1, price_records,
                fundamental_records, f"{price_error}; {fundamental_error}".strip("; "),
                is_delisted, price_points[0].delisted_date if price_points else None,
                price_points[0].exchange if price_points else self.stock_universe.get(ticker, {}).get('exchange', 'Unknown')
            ))
            cursor.connection.commit()
            cursor.connection.close()
            
            # Update statistics
            with self.lock:
                if price_records > 0 or fundamental_records > 0:
                    self.progress['completed_tickers'].append(ticker)
                    self.stats['successful'] += 1
                    self.stats['total_records'] += price_records + fundamental_records
                    self.consecutive_failures = 0  # Reset on success
                    self.logger.info(f"✓ {ticker}: {price_records} price records, {fundamental_records} fundamental records")
                else:
                    self.progress['failed_tickers'].append(ticker)
                    self.stats['failed'] += 1
                    self.consecutive_failures += 1
                    self.logger.warning(f"✗ {ticker}: {price_error}; {fundamental_error}")
                
                self.stats['total_processed'] += 1
                self.stats['last_update'] = datetime.now()
            
            return price_records, fundamental_records, is_delisted, ""
            
        except Exception as e:
            error_msg = f"Unexpected error processing {ticker}: {e}"
            self.logger.error(error_msg)
            
            with self.lock:
                self.progress['failed_tickers'].append(ticker)
                self.stats['failed'] += 1
                self.stats['total_processed'] += 1
                self.consecutive_failures += 1
            
            return 0, 0, True, error_msg
    
    def run_improved_collection(self, max_tickers: int = None, target_percentage: float = None):
        """Run improved data collection session"""
        session_start = datetime.now()
        self.stats['start_time'] = session_start
        
        # Get pending tickers (exclude both completed and failed)
        pending_tickers = [t for t in self.stock_universe.keys() 
                          if t not in self.progress['completed_tickers'] and t not in self.progress['failed_tickers']]
        
        # Calculate target based on percentage if specified
        if target_percentage:
            total_stocks = len(self.stock_universe)
            target_stocks = int(total_stocks * (target_percentage / 100))
            completed_count = len(self.progress['completed_tickers'])
            
            if completed_count >= target_stocks:
                self.logger.info(f"Target of {target_percentage}% already reached! ({completed_count}/{target_stocks} stocks)")
                return 0, 0, 0
            
            remaining_needed = target_stocks - completed_count
            if len(pending_tickers) == 0:
                self.logger.info(f"No more pending stocks available. Target may not be reachable due to delisted stocks.")
                return 0, 0, 0
            
            pending_tickers = pending_tickers[:remaining_needed]
            self.logger.info(f"Target: {target_percentage}% ({target_stocks} stocks) - Need {remaining_needed} more stocks from {len(pending_tickers)} available")
        
        if max_tickers:
            pending_tickers = pending_tickers[:max_tickers]
        
        self.logger.info(f"Starting improved collection session for {len(pending_tickers)} tickers")
        self.logger.info(f"Rate limiting: Adaptive with exponential backoff")
        self.logger.info(f"Batch size: {self.batch_size} tickers per batch")
        self.logger.info(f"Max workers: {self.max_workers}")
        
        session_price_records = 0
        session_fundamental_records = 0
        session_errors = 0
        
        # Process tickers sequentially for better rate limiting
        for i, ticker in enumerate(pending_tickers):
            try:
                price_records, fundamental_records, is_delisted, error = self.process_ticker(ticker)
                
                session_price_records += price_records
                session_fundamental_records += fundamental_records
                
                if error:
                    session_errors += 1
                
                # Checkpoint every 10 tickers
                if (i + 1) % 10 == 0:
                    self.checkpoint_database()
                    self.save_progress()
                    self.logger.info(f"Checkpoint: Processed {i + 1}/{len(pending_tickers)} tickers")
                
            except KeyboardInterrupt:
                self.logger.info("Collection interrupted by user")
                break
            except Exception as e:
                self.logger.error(f"Error in collection loop: {e}")
                session_errors += 1
        
        # Final checkpoint
        self.checkpoint_database()
        self.save_progress()
        
        # Log session results
        session_end = datetime.now()
        session_duration = (session_end - session_start).total_seconds()
        
        self.logger.info(f"Session complete: {session_price_records} price records, {session_fundamental_records} fundamental records, {session_errors} errors, {session_duration:.1f}s")
        
        return session_price_records, session_fundamental_records, session_errors
    
    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get price data stats
            cursor.execute('SELECT COUNT(*) FROM historical_prices')
            price_records = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM historical_prices')
            price_tickers = cursor.fetchone()[0]
            
            cursor.execute('SELECT MIN(date), MAX(date) FROM historical_prices')
            price_date_range = cursor.fetchone()
            
            # Get fundamentals stats
            cursor.execute('SELECT COUNT(*) FROM current_fundamentals')
            fundamental_records = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM current_fundamentals')
            fundamental_tickers = cursor.fetchone()[0]
            
            # Get records per exchange
            cursor.execute('''
                SELECT exchange, COUNT(*) as record_count, COUNT(DISTINCT ticker) as ticker_count
                FROM historical_prices 
                GROUP BY exchange 
                ORDER BY record_count DESC
            ''')
            exchange_stats = cursor.fetchall()
            
            conn.close()
            
            return {
                'price_records': price_records,
                'price_tickers': price_tickers,
                'price_date_range': price_date_range,
                'fundamental_records': fundamental_records,
                'fundamental_tickers': fundamental_tickers,
                'exchange_stats': exchange_stats
            }
            
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {}
    
    def print_progress_summary(self):
        """Print comprehensive progress summary"""
        print("\n" + "="*100)
        print("IMPROVED DATA COLLECTION PROGRESS")
        print("="*100)
        
        print(f"Total Tickers: {len(self.stock_universe)}")
        print(f"Completed: {len(self.progress['completed_tickers'])}")
        print(f"Failed: {len(self.progress['failed_tickers'])}")
        print(f"Pending: {len(self.progress['pending_tickers']) - len(self.progress['completed_tickers']) - len(self.progress['failed_tickers'])}")
        
        print(f"\nSession Statistics:")
        print(f"Total Processed: {self.stats['total_processed']}")
        print(f"Successful: {self.stats['successful']}")
        print(f"Failed: {self.stats['failed']}")
        print(f"Rate Limited: {self.stats['rate_limited']}")
        print(f"Total Records: {self.stats['total_records']:,}")
        
        if self.stats['start_time']:
            duration = datetime.now() - self.stats['start_time']
            print(f"Session Duration: {duration}")
        
        # Database stats
        db_stats = self.get_database_stats()
        if db_stats:
            print(f"\nDatabase Statistics:")
            print(f"Price Records: {db_stats['price_records']:,}")
            print(f"Price Tickers: {db_stats['price_tickers']}")
            print(f"Fundamental Records: {db_stats['fundamental_records']:,}")
            print(f"Fundamental Tickers: {db_stats['fundamental_tickers']}")
            
            if db_stats['price_date_range'][0]:
                print(f"Price Date Range: {db_stats['price_date_range'][0]} to {db_stats['price_date_range'][1]}")
            
            if db_stats['exchange_stats']:
                print(f"\nRecords by Exchange:")
                for exchange, record_count, ticker_count in db_stats['exchange_stats']:
                    print(f"  {exchange}: {record_count:,} records ({ticker_count} tickers)")
        
        print("="*100)

def main():
    """Main function for improved data collection"""
    collector = ImprovedDataCollector()
    
    print("="*100)
    print("IMPROVED COMPREHENSIVE DATA COLLECTOR - CONTINUOUS MODE")
    print("="*100)
    print(f"Stock Universe: {len(collector.stock_universe)} stocks")
    print(f"Exchanges: US, ASX, NZX")
    print(f"Architecture: Separated historical prices from current fundamentals")
    print(f"Rate Limiting: Adaptive with exponential backoff")
    print(f"Memory Management: Streaming data insertion")
    print(f"Target: 5% of database population")
    print("="*100)
    
    # Run continuous collection until 5% completion
    total_price_records = 0
    total_fundamental_records = 0
    total_errors = 0
    session_count = 0
    
    try:
        while True:
            session_count += 1
            print(f"\n--- SESSION {session_count} ---")
            
            # Run collection session targeting 100% completion (progressive)
            price_records, fundamental_records, errors = collector.run_improved_collection(target_percentage=100.0)
            
            total_price_records += price_records
            total_fundamental_records += fundamental_records
            total_errors += errors
            
            # Check if target reached
            completed_count = len(collector.progress['completed_tickers'])
            failed_count = len(collector.progress['failed_tickers'])
            total_stocks = len(collector.stock_universe)
            target_stocks = int(total_stocks * 0.05)  # 5%
            completion_pct = (completed_count / total_stocks) * 100
            
            print(f"\nSession {session_count} Results:")
            print(f"Price records: {price_records:,}")
            print(f"Fundamental records: {fundamental_records:,}")
            print(f"Errors: {errors}")
            print(f"Progress: {completed_count}/{target_stocks} stocks ({completion_pct:.2f}%)")
            print(f"Failed stocks: {failed_count}")
            
            if completed_count >= target_stocks:
                print(f"\nTARGET REACHED! Completed {completion_pct:.2f}% of database population")
                break
            
            # Check if no more stocks available
            if price_records == 0 and fundamental_records == 0 and errors == 0:
                print(f"\nNo more stocks available to process. Target may not be reachable.")
                break
            
            # Brief pause between sessions
            print(f"Continuing to next session in 2 seconds...")
            import time
            time.sleep(2)
    
    except KeyboardInterrupt:
        print(f"\nCollection interrupted by user")
    
    # Final summary
    collector.print_progress_summary()
    
    print(f"\n" + "="*100)
    print(f"CONTINUOUS COLLECTION COMPLETE!")
    print(f"="*100)
    print(f"Total Sessions: {session_count}")
    print(f"Total Price Records: {total_price_records:,}")
    print(f"Total Fundamental Records: {total_fundamental_records:,}")
    print(f"Total Errors: {total_errors}")
    print(f"Final Progress: {len(collector.progress['completed_tickers'])}/{len(collector.stock_universe)} stocks")
    print(f"Database: {collector.db_path}")
    print(f"Progress file: {collector.progress_file}")
    print(f"="*100)

if __name__ == "__main__":
    main()
