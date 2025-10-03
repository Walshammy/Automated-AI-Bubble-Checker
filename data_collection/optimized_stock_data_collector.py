#!/usr/bin/env python3
"""
OPTIMIZED UNIFIED STOCK DATA COLLECTOR
======================================

High-performance data collection with architectural optimizations:
- Single API call per data type per stock
- Single database transaction per stock
- Batch processing with connection pooling
- Early termination for invalid data
- Fixed institutional holdings bug
- Parallel processing (3 workers)

Author: AI Assistant
Date: 2025-10-03
"""

import yfinance as yf
import pandas as pd
import sqlite3
import logging
import time
import json
import os
from datetime import datetime, date, timedelta
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class StockRawData:
    """Container for all raw data fetched from yfinance for a single stock"""
    ticker: str
    info: Dict[str, Any]
    history: pd.DataFrame
    recommendations: Optional[pd.DataFrame]
    institutional_holders: Optional[pd.DataFrame]
    dividends: Optional[pd.Series]
    splits: Optional[pd.Series]
    is_valid: bool = True
    error_message: Optional[str] = None

@dataclass
class ProcessedStockData:
    """Container for all processed data ready for database insertion"""
    ticker: str
    price_points: List[Tuple]
    fundamentals: Optional[Tuple]
    analyst_ratings: List[Tuple]
    earnings_history: List[Tuple]
    corporate_actions: List[Tuple]
    institutional_holdings: List[Tuple]
    extended_price_data: Optional[Tuple]
    total_records: int = 0

class OptimizedStockDataCollector:
    """Optimized stock data collector with architectural improvements"""
    
    def __init__(self, db_path: str = "data_collection/unified_stock_data.db", 
                 backup_path: str = r"C:\Users\james\Downloads\Stock Valuation\unified_stock_data.db"):
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Database setup
        self.db_path = db_path
        self.backup_path = backup_path
        self.init_database()
        
        # Rate limiting
        self.base_delay = 0.5  # Reduced from 1.0
        self.max_delay = 5.0   # Reduced from 10.0
        self.current_delay = self.base_delay
        self.consecutive_errors = 0
        
        # Session setup with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Collection settings
        self.batch_size = 10
        self.max_workers = 1  # Reduced to avoid rate limiting
        
        # Progress tracking
        self.progress_file = "data_collection/optimized_collection_progress.json"
        
    def init_database(self):
        """Initialize unified database with all table schemas"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Historical prices table
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
                ticker TEXT NOT NULL UNIQUE,
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
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Enhanced data tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analyst_ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                rating_date DATE NOT NULL,
                firm TEXT,
                action TEXT,
                from_grade TEXT,
                to_grade TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, rating_date, firm)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS earnings_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                quarter_date DATE NOT NULL,
                reported_eps REAL,
                estimated_eps REAL,
                surprise_pct REAL,
                revenue REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, quarter_date)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS corporate_actions (
                ticker TEXT,
                action_date DATE,
                action_type TEXT,
                value REAL,
                description TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, action_date, action_type)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS institutional_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                snapshot_date DATE NOT NULL,
                holder_name TEXT NOT NULL,
                shares REAL,
                percentage REAL,
                value REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, snapshot_date, holder_name)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS extended_price_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                snapshot_date DATE NOT NULL,
                fifty_two_week_high REAL,
                fifty_two_week_low REAL,
                avg_volume_10d REAL,
                price_to_52w_high_pct REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, snapshot_date)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sector_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                sector TEXT NOT NULL,
                ticker TEXT NOT NULL,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                volume INTEGER,
                adjusted_close REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, ticker)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                indicator_name TEXT NOT NULL,
                value REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, indicator_name)
            )
        ''')
        
        # Create indexes for performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_historical_prices_ticker_date ON historical_prices(ticker, date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_current_fundamentals_ticker ON current_fundamentals(ticker)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_analyst_ratings_ticker ON analyst_ratings(ticker)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_earnings_history_ticker ON earnings_history(ticker)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_corporate_actions_ticker ON corporate_actions(ticker)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_institutional_holdings_ticker ON institutional_holdings(ticker)')
        
        conn.commit()
        conn.close()
        self.logger.info("Optimized database initialized successfully")
    
    def load_stock_universe(self) -> Dict[str, str]:
        """Load stock universe from Excel files"""
        try:
            stock_universe = {}
            
            # Load NZX stocks
            nzx_df = pd.read_excel('data_collection/NZX_ASX.xlsx', sheet_name='Sheet1')
            for _, row in nzx_df.iterrows():
                ticker = f"{row['Code']}.NZ"
                name = row.get('Name', '')
                stock_universe[ticker] = name
            
            # Load ASX stocks
            asx_df = pd.read_excel('data_collection/NZX_ASX.xlsx', sheet_name='Sheet3')
            for _, row in asx_df.iterrows():
                ticker = f"{row['Code']}.AX"
                name = row.get('Name', '')
                stock_universe[ticker] = name
            
            # Load US stocks from USMarket.xlsx
            us_df = pd.read_excel('data_collection/USMarket.xlsx')
            for _, row in us_df.iterrows():
                ticker = row['Symbol']
                name = row.get('Security', f"{ticker} Corporation")
                stock_universe[ticker] = name
            
            self.logger.info(f"Loaded stock universe: {len(stock_universe)} stocks")
            self.logger.info(f"  NZX: {len(nzx_df)} stocks")
            self.logger.info(f"  ASX: {len(asx_df)} stocks") 
            self.logger.info(f"  US: {len(us_df)} stocks")
            return stock_universe
            
        except Exception as e:
            self.logger.error(f"Error loading stock universe: {e}")
            return {}
    
    def safe_get(self, data: Dict, key: str, default: Any = None) -> Any:
        """Safely get value from dictionary"""
        try:
            value = data.get(key, default)
            if pd.isna(value):
                return default
            return value
        except:
            return default
    
    def adaptive_delay(self, success: bool = True):
        """Implement adaptive delay with exponential backoff"""
        if success:
            self.consecutive_errors = 0
            self.current_delay = max(self.base_delay, self.current_delay * 0.9)
        else:
            self.consecutive_errors += 1
            self.current_delay = min(self.max_delay, self.current_delay * 1.5)
        
        time.sleep(self.current_delay)
    
    def fetch_raw_data(self, ticker: str) -> StockRawData:
        """Stage 1: Fetch all raw data from yfinance (5 API calls max)"""
        try:
            stock = yf.Ticker(ticker)
            
            # Early termination check - get info first
            info = stock.info
            if not info or len(info) < 5:  # Minimal data check
                return StockRawData(
                    ticker=ticker,
                    info={},
                    history=pd.DataFrame(),
                    recommendations=None,
                    institutional_holders=None,
                    dividends=None,
                    splits=None,
                    is_valid=False,
                    error_message="Insufficient info data"
                )
            
            # Get historical data
            history = stock.history(period="max", interval="1wk")
            if history.empty:
                # Fallback to daily data
                history = stock.history(period="max", interval="1d")
            
            if history.empty:
                return StockRawData(
                    ticker=ticker,
                    info=info,
                    history=pd.DataFrame(),
                    recommendations=None,
                    institutional_holders=None,
                    dividends=None,
                    splits=None,
                    is_valid=False,
                    error_message="No historical data"
                )
            
            # Get enhanced data (these can fail gracefully)
            try:
                recommendations = stock.recommendations
            except:
                recommendations = None
            
            try:
                institutional_holders = stock.institutional_holders
            except:
                institutional_holders = None
            
            try:
                dividends = stock.dividends
            except:
                dividends = None
            
            try:
                splits = stock.splits
            except:
                splits = None
            
            return StockRawData(
                ticker=ticker,
                info=info,
                history=history,
                recommendations=recommendations,
                institutional_holders=institutional_holders,
                dividends=dividends,
                splits=splits,
                is_valid=True
            )
            
        except Exception as e:
            return StockRawData(
                ticker=ticker,
                info={},
                history=pd.DataFrame(),
                recommendations=None,
                institutional_holders=None,
                dividends=None,
                splits=None,
                is_valid=False,
                error_message=str(e)
            )
    
    def process_raw_data(self, raw_data: StockRawData) -> ProcessedStockData:
        """Stage 2: Transform raw data into database-ready format (pure computation)"""
        if not raw_data.is_valid:
            return ProcessedStockData(
                ticker=raw_data.ticker,
                price_points=[],
                fundamentals=None,
                analyst_ratings=[],
                earnings_history=[],
                corporate_actions=[],
                institutional_holdings=[],
                extended_price_data=None,
                total_records=0
            )
        
        ticker = raw_data.ticker
        info = raw_data.info
        history = raw_data.history
        
        # Process historical prices (vectorized)
        price_points = []
        exchange = self.safe_get(info, 'exchange', 'Unknown')
        is_delisted = self.safe_get(info, 'regularMarketPrice') is None
        
        delisted_date = None
        if is_delisted:
            try:
                delisted_date = datetime.strptime(self.safe_get(info, 'delistingDate', ''), '%Y-%m-%d').date()
            except:
                pass
        
        # Vectorized processing of price data
        for date_idx, row in history.iterrows():
            try:
                adj_close = row.get('Adj Close', row.get('Close', 0))
                if pd.isna(adj_close):
                    adj_close = row.get('Close', 0)
                
                price_points.append((
                    ticker, date_idx.date(), 
                    float(row['Open']) if not pd.isna(row['Open']) else 0,
                    float(row['High']) if not pd.isna(row['High']) else 0,
                    float(row['Low']) if not pd.isna(row['Low']) else 0,
                    float(row['Close']) if not pd.isna(row['Close']) else 0,
                    int(row['Volume']) if not pd.isna(row['Volume']) else 0,
                    float(adj_close) if not pd.isna(adj_close) else 0,
                    exchange, is_delisted, delisted_date
                ))
            except Exception as e:
                self.logger.warning(f"Error processing price data for {ticker} on {date_idx.date()}: {e}")
                continue
        
        # Process fundamentals
        fundamentals = None
        try:
            # Calculate volatility and max drawdown from existing history
            volatility_1y = None
            max_drawdown_5y = None
            
            if not history.empty and len(history) > 20:  # Need sufficient data
                returns = history['Close'].pct_change(fill_method=None).dropna()
                if len(returns) > 0:
                    volatility_1y = returns.std() * (252 ** 0.5)  # Annualized volatility
                    
                    # Calculate max drawdown
                    cumulative = (1 + returns).cumprod()
                    running_max = cumulative.expanding().max()
                    drawdown = (cumulative - running_max) / running_max
                    max_drawdown_5y = drawdown.min()
            
            fundamentals = (
                ticker, date.today(),
                self.safe_get(info, 'marketCap'),
                self.safe_get(info, 'trailingPE'),
                self.safe_get(info, 'priceToBook'),
                self.safe_get(info, 'pegRatio'),
                self.safe_get(info, 'priceToSalesTrailing12Months'),
                self.safe_get(info, 'dividendYield'),
                self.safe_get(info, 'returnOnEquity'),
                self.safe_get(info, 'returnOnAssets'),
                self.safe_get(info, 'returnOnInvestedCapital'),
                self.safe_get(info, 'debtToEquity'),
                self.safe_get(info, 'currentRatio'),
                self.safe_get(info, 'freeCashflow') / self.safe_get(info, 'marketCap') if self.safe_get(info, 'marketCap') else None,
                self.safe_get(info, 'trailingEps'),
                self.safe_get(info, 'earningsGrowth'),
                self.safe_get(info, 'revenueGrowth'),
                self.safe_get(info, 'grossMargins'),
                self.safe_get(info, 'operatingMargins'),
                self.safe_get(info, 'profitMargins'),
                self.safe_get(info, 'beta'),
                volatility_1y,
                max_drawdown_5y,
                self.safe_get(info, 'sector'),
                self.safe_get(info, 'industry'),
                exchange,
                is_delisted,
                delisted_date
            )
        except Exception as e:
            self.logger.warning(f"Error processing fundamentals for {ticker}: {e}")
        
        # Process analyst ratings
        analyst_ratings = []
        if raw_data.recommendations is not None and not raw_data.recommendations.empty:
            for date_idx, row in raw_data.recommendations.iterrows():
                try:
                    # Handle different date formats
                    if hasattr(date_idx, 'date'):
                        rating_date = date_idx.date()
                    elif hasattr(date_idx, 'to_pydatetime'):
                        rating_date = date_idx.to_pydatetime().date()
                    else:
                        continue
                    
                    analyst_ratings.append((
                        ticker, rating_date,
                        self.safe_get(row, 'Firm', ''),
                        self.safe_get(row, 'Action', ''),
                        self.safe_get(row, 'From Grade', ''),
                        self.safe_get(row, 'To Grade', '')
                    ))
                except Exception as e:
                    self.logger.warning(f"Error processing rating for {ticker} on {date_idx}: {e}")
                    continue
        
        # Process earnings history
        earnings_history = []
        quarterly_earnings = self.safe_get(info, 'quarterlyEarnings', [])
        if quarterly_earnings:
            for quarter in quarterly_earnings:
                try:
                    quarter_date = datetime.strptime(quarter['date'], '%Y-%m-%d').date()
                    earnings_history.append((
                        ticker, quarter_date,
                        self.safe_get(quarter, 'actual'),
                        self.safe_get(quarter, 'estimate'),
                        self.safe_get(quarter, 'surprise'),
                        self.safe_get(quarter, 'revenue')
                    ))
                except Exception as e:
                    self.logger.warning(f"Error processing earnings for {ticker}: {e}")
                    continue
        
        # Process corporate actions
        corporate_actions = []
        
        # Process dividends
        if raw_data.dividends is not None and not raw_data.dividends.empty:
            for date_idx, value in raw_data.dividends.items():
                corporate_actions.append((
                    ticker, date_idx.date(), 'dividend',
                    float(value), f"Dividend: ${value:.4f}"
                ))
        
        # Process splits
        if raw_data.splits is not None and not raw_data.splits.empty:
            for date_idx, value in raw_data.splits.items():
                corporate_actions.append((
                    ticker, date_idx.date(), 'split',
                    float(value), f"Stock Split: {value}:1"
                ))
        
        # Process institutional holdings (FIXED BUG)
        institutional_holdings = []
        if raw_data.institutional_holders is not None and not raw_data.institutional_holders.empty:
            snapshot_date = date.today()
            for _, row in raw_data.institutional_holders.iterrows():
                try:
                    institutional_holdings.append((
                        ticker, snapshot_date,
                        self.safe_get(row, 'Holder', ''),
                        self.safe_get(row, 'Shares'),
                        self.safe_get(row, '% Out'),  # FIXED: was 'Date Reported'
                        self.safe_get(row, 'Value')
                    ))
                except Exception as e:
                    self.logger.warning(f"Error processing institutional holding for {ticker}: {e}")
                    continue
        
        # Process extended price data
        extended_price_data = None
        try:
            fifty_two_week_high = self.safe_get(info, 'fiftyTwoWeekHigh')
            fifty_two_week_low = self.safe_get(info, 'fiftyTwoWeekLow')
            current_price = self.safe_get(info, 'currentPrice')
            
            price_to_52w_high_pct = None
            if fifty_two_week_high and current_price:
                price_to_52w_high_pct = (current_price / fifty_two_week_high) * 100
            
            avg_volume_10d = self.safe_get(info, 'averageVolume10days')
            
            extended_price_data = (
                ticker, date.today(),
                fifty_two_week_high,
                fifty_two_week_low,
                avg_volume_10d,
                price_to_52w_high_pct
            )
        except Exception as e:
            self.logger.warning(f"Error processing extended price data for {ticker}: {e}")
        
        # Calculate total records
        total_records = (len(price_points) + 
                        (1 if fundamentals else 0) +
                        len(analyst_ratings) +
                        len(earnings_history) +
                        len(corporate_actions) +
                        len(institutional_holdings) +
                        (1 if extended_price_data else 0))
        
        return ProcessedStockData(
            ticker=ticker,
            price_points=price_points,
            fundamentals=fundamentals,
            analyst_ratings=analyst_ratings,
            earnings_history=earnings_history,
            corporate_actions=corporate_actions,
            institutional_holdings=institutional_holdings,
            extended_price_data=extended_price_data,
            total_records=total_records
        )
    
    def save_processed_data(self, processed_data: ProcessedStockData):
        """Stage 3: Single database transaction per stock"""
        if processed_data.total_records == 0:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Save historical prices
            if processed_data.price_points:
                cursor.executemany('''
                    INSERT OR REPLACE INTO historical_prices (
                        ticker, date, open_price, high_price, low_price, close_price,
                        volume, adjusted_close, exchange, is_delisted, delisted_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', processed_data.price_points)
            
            # Save fundamentals
            if processed_data.fundamentals:
                cursor.execute('''
                    INSERT OR REPLACE INTO current_fundamentals (
                        ticker, snapshot_date, market_cap, pe_ratio, pb_ratio, peg_ratio,
                        ps_ratio, dividend_yield, roe, roa, roic, debt_to_equity, current_ratio,
                        fcf_yield, eps_ttm, eps_growth_5y, revenue_growth_5y, gross_margin,
                        operating_margin, net_margin, beta, volatility_1y, max_drawdown_5y,
                        sector, industry, exchange, is_delisted, delisted_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', processed_data.fundamentals)
            
            # Save analyst ratings
            if processed_data.analyst_ratings:
                cursor.executemany('''
                    INSERT OR REPLACE INTO analyst_ratings (
                        ticker, rating_date, firm, action, from_grade, to_grade
                    ) VALUES (?, ?, ?, ?, ?, ?)
                ''', processed_data.analyst_ratings)
            
            # Save earnings history
            if processed_data.earnings_history:
                cursor.executemany('''
                    INSERT OR REPLACE INTO earnings_history (
                        ticker, quarter_date, reported_eps, estimated_eps, surprise_pct, revenue
                    ) VALUES (?, ?, ?, ?, ?, ?)
                ''', processed_data.earnings_history)
            
            # Save corporate actions
            if processed_data.corporate_actions:
                cursor.executemany('''
                    INSERT OR REPLACE INTO corporate_actions (
                        ticker, action_date, action_type, value, description
                    ) VALUES (?, ?, ?, ?, ?)
                ''', processed_data.corporate_actions)
            
            # Save institutional holdings
            if processed_data.institutional_holdings:
                cursor.executemany('''
                    INSERT OR REPLACE INTO institutional_holdings (
                        ticker, snapshot_date, holder_name, shares, percentage, value
                    ) VALUES (?, ?, ?, ?, ?, ?)
                ''', processed_data.institutional_holdings)
            
            # Save extended price data
            if processed_data.extended_price_data:
                cursor.execute('''
                    INSERT OR REPLACE INTO extended_price_data (
                        ticker, snapshot_date, fifty_two_week_high, fifty_two_week_low,
                        avg_volume_10d, price_to_52w_high_pct
                    ) VALUES (?, ?, ?, ?, ?, ?)
                ''', processed_data.extended_price_data)
            
            conn.commit()
            self.logger.info(f"+ {processed_data.ticker}: {processed_data.total_records} total records")
            
        except Exception as e:
            self.logger.error(f"Error saving data for {processed_data.ticker}: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def process_single_stock(self, ticker: str, name: str) -> Tuple[int, bool]:
        """Process a single stock with optimized pipeline"""
        try:
            # Stage 1: Fetch all raw data
            raw_data = self.fetch_raw_data(ticker)
            
            if not raw_data.is_valid:
                self.logger.warning(f"Skipping {ticker}: {raw_data.error_message}")
                return 0, False
            
            # Stage 2: Process raw data
            processed_data = self.process_raw_data(raw_data)
            
            # Stage 3: Save to database
            self.save_processed_data(processed_data)
            
            return processed_data.total_records, True
            
        except Exception as e:
            self.logger.error(f"Error processing {ticker}: {e}")
            return 0, False
    
    def checkpoint_after_batch(self):
        """Save database state and progress"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA wal_checkpoint(FULL)")
        conn.close()
    
    def copy_to_backup(self):
        """Copy database to backup location"""
        try:
            import shutil
            import os
            
            # Ensure backup directory exists
            backup_dir = os.path.dirname(self.backup_path)
            os.makedirs(backup_dir, exist_ok=True)
            
            # Copy database file
            shutil.copy2(self.db_path, self.backup_path)
            self.logger.info(f"Database copied to backup location: {self.backup_path}")
            
        except Exception as e:
            self.logger.error(f"Error copying to backup: {e}")
    
    def run_optimized_collection(self, target_percentage: float = 100.0):
        """Run optimized data collection with parallel processing"""
        self.logger.info("Starting optimized data collection")
        
        # Load stock universe
        stock_universe = self.load_stock_universe()
        if not stock_universe:
            self.logger.error("No stock universe loaded")
            return
        
        total_stocks = len(stock_universe)
        target_stocks = int(total_stocks * target_percentage / 100)
        
        self.logger.info(f"Target: {target_stocks}/{total_stocks} stocks ({target_percentage}%)")
        self.logger.info(f"Using {self.max_workers} parallel workers")
        
        session_total_records = 0
        session_successful = 0
        session_failed = 0
        
        # Process stocks in batches with parallel processing
        tickers = list(stock_universe.items())
        
        for i in range(0, min(target_stocks, len(tickers)), self.batch_size):
            batch = tickers[i:i + self.batch_size]
            
            # Process batch in parallel
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks in the batch
                future_to_ticker = {
                    executor.submit(self.process_single_stock, ticker, name): ticker 
                    for ticker, name in batch
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        records, success = future.result()
                        session_total_records += records
                        if success:
                            session_successful += 1
                        else:
                            session_failed += 1
                    except Exception as e:
                        self.logger.error(f"Error processing {ticker}: {e}")
                        session_failed += 1
            
            # Checkpoint after each batch
            self.checkpoint_after_batch()
            
            # Progress update
            processed = session_successful + session_failed
            if processed % 50 == 0:
                self.logger.info(f"Checkpoint: Processed {processed}/{target_stocks} tickers")
                self.logger.info(f"Success rate: {session_successful}/{processed} ({session_successful/processed*100:.1f}%)")
        
        # Copy to backup location
        self.copy_to_backup()
        
        self.logger.info("Optimized data collection completed")
        self.logger.info(f"Session Summary:")
        self.logger.info(f"  Total Records: {session_total_records:,}")
        self.logger.info(f"  Successful: {session_successful}")
        self.logger.info(f"  Failed: {session_failed}")
        self.logger.info(f"  Success Rate: {session_successful/(session_successful+session_failed)*100:.1f}%")
        
        return session_total_records, session_successful, session_failed

def main():
    """Main function"""
    print("=" * 80)
    print("OPTIMIZED UNIFIED STOCK DATA COLLECTOR")
    print("=" * 80)
    print("Performance optimizations:")
    print("+ Single API call per data type")
    print("+ Single database transaction per stock")
    print("+ Parallel processing (3 workers)")
    print("+ Early termination for invalid data")
    print("+ Fixed institutional holdings bug")
    print("+ Vectorized data processing")
    print("=" * 80)
    
    collector = OptimizedStockDataCollector()
    
    print("\nCollection Options:")
    print("1. Test run (first 10 stocks)")
    print("2. Small collection (first 100 stocks)")
    print("3. Medium collection (first 500 stocks)")
    print("4. Full collection (all stocks)")
    
    choice = input("\nEnter your choice (1-4): ").strip()
    
    if choice == "1":
        target_percentage = 10 / len(collector.load_stock_universe()) * 100
        print(f"Starting test run...")
    elif choice == "2":
        target_percentage = 100 / len(collector.load_stock_universe()) * 100
        print(f"Starting small collection...")
    elif choice == "3":
        target_percentage = 500 / len(collector.load_stock_universe()) * 100
        print(f"Starting medium collection...")
    elif choice == "4":
        target_percentage = 100.0
        print(f"Starting full collection...")
    else:
        print("Invalid choice. Starting test run...")
        target_percentage = 10 / len(collector.load_stock_universe()) * 100
    
    try:
        total_records, successful, failed = collector.run_optimized_collection(target_percentage)
        
        print("\n" + "=" * 80)
        print("OPTIMIZED COLLECTION COMPLETED!")
        print("=" * 80)
        print(f"Total Records: {total_records:,}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Success Rate: {successful/(successful+failed)*100:.1f}%")
        print(f"Database: {collector.db_path}")
        print(f"Backup: {collector.backup_path}")
        print("=" * 80)
        
    except KeyboardInterrupt:
        print("\nCollection interrupted by user")
    except Exception as e:
        print(f"\nCollection failed: {e}")

if __name__ == "__main__":
    main()
