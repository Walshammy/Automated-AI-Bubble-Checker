#!/usr/bin/env python3
"""
Comprehensive Data Collector - Fixed Version
Unified system for collecting maximum historical and valuation data across US, ASX, and NZX markets
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

warnings.filterwarnings('ignore')

@dataclass
class ComprehensiveDataPoint:
    """Comprehensive data point combining historical and valuation data"""
    ticker: str
    date: date
    # Price data
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    adjusted_close: float
    # Valuation metrics
    market_cap: float
    pe_ratio: float
    pb_ratio: float
    peg_ratio: float
    ps_ratio: float
    dividend_yield: float
    # Quality metrics
    roe: float
    roa: float
    roic: float
    debt_to_equity: float
    current_ratio: float
    fcf_yield: float
    # Growth metrics
    eps_ttm: float
    eps_growth_5y: float
    revenue_growth_5y: float
    # Margin metrics
    gross_margin: float
    operating_margin: float
    net_margin: float
    # Risk metrics
    beta: float
    volatility_1y: float
    max_drawdown_5y: float
    # Metadata
    sector: str
    industry: str
    exchange: str
    is_delisted: bool
    delisted_date: Optional[date]
    created_at: datetime

class ComprehensiveDataCollector:
    """Comprehensive data collector for maximum data gathering"""
    
    def __init__(self, db_path: str = "comprehensive_data.db"):
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Database setup
        self.db_path = db_path
        self.init_database()
        
        # Load comprehensive stock universe
        self.load_stock_universe()
        
        # Collection settings optimized for maximum data collection
        self.start_date = date(2000, 1, 1)
        self.end_date = date.today()
        self.min_delay = 2  # Faster collection
        self.max_delay = 4
        self.max_retries = 3
        self.batch_size = 10  # Larger batches
        self.max_workers = 3  # More concurrent workers
        
        # Progress tracking
        self.progress_file = "comprehensive_collection_progress.json"
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
        
        # Thread safety
        self.lock = threading.Lock()
        
    def init_database(self):
        """Initialize comprehensive SQLite database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create comprehensive data table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS comprehensive_data (
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
                    UNIQUE(ticker, date)
                )
            ''')
            
            # Create indexes for performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker_date ON comprehensive_data(ticker, date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON comprehensive_data(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON comprehensive_data(ticker)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_exchange ON comprehensive_data(exchange)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sector ON comprehensive_data(sector)')
            
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
                    exchange TEXT,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            self.logger.info(f"Database initialized: {self.db_path}")
            
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise
    
    def load_stock_universe(self):
        """Load comprehensive stock universe"""
        try:
            # Load NZX stocks
            nzx_df = pd.read_excel('NZX_ASX.xlsx', sheet_name='Sheet1')
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
            asx_df = pd.read_excel('NZX_ASX.xlsx', sheet_name='Sheet3')
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
            
            # Add comprehensive US stock universe
            us_stocks = {
                # Major US Stocks
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
                'KO': {'company': 'Coca-Cola Company', 'exchange': 'NYSE', 'sector': 'Consumer'},
                'PEP': {'company': 'PepsiCo Inc.', 'exchange': 'NASDAQ', 'sector': 'Consumer'},
                'WMT': {'company': 'Walmart Inc.', 'exchange': 'NYSE', 'sector': 'Consumer'},
                'HD': {'company': 'Home Depot Inc.', 'exchange': 'NYSE', 'sector': 'Consumer'},
                'JPM': {'company': 'JPMorgan Chase & Co.', 'exchange': 'NYSE', 'sector': 'Financial'},
                'BAC': {'company': 'Bank of America Corporation', 'exchange': 'NYSE', 'sector': 'Financial'},
                'WFC': {'company': 'Wells Fargo & Company', 'exchange': 'NYSE', 'sector': 'Financial'},
                'CVX': {'company': 'Chevron Corporation', 'exchange': 'NYSE', 'sector': 'Energy'},
                'XOM': {'company': 'Exxon Mobil Corporation', 'exchange': 'NYSE', 'sector': 'Energy'},
                'IBM': {'company': 'International Business Machines Corporation', 'exchange': 'NYSE', 'sector': 'Technology'},
                'INTC': {'company': 'Intel Corporation', 'exchange': 'NASDAQ', 'sector': 'Technology'},
                'CSCO': {'company': 'Cisco Systems Inc.', 'exchange': 'NASDAQ', 'sector': 'Technology'},
                'ORCL': {'company': 'Oracle Corporation', 'exchange': 'NYSE', 'sector': 'Technology'},
                'ADBE': {'company': 'Adobe Inc.', 'exchange': 'NASDAQ', 'sector': 'Technology'},
                'NFLX': {'company': 'Netflix Inc.', 'exchange': 'NASDAQ', 'sector': 'Technology'},
                'CRM': {'company': 'Salesforce Inc.', 'exchange': 'NYSE', 'sector': 'Technology'},
                'DIS': {'company': 'The Walt Disney Company', 'exchange': 'NYSE', 'sector': 'Consumer'},
                'MCD': {'company': 'McDonald\'s Corporation', 'exchange': 'NYSE', 'sector': 'Consumer'},
                'NKE': {'company': 'Nike Inc.', 'exchange': 'NYSE', 'sector': 'Consumer'},
                'BA': {'company': 'The Boeing Company', 'exchange': 'NYSE', 'sector': 'Industrial'},
                'CAT': {'company': 'Caterpillar Inc.', 'exchange': 'NYSE', 'sector': 'Industrial'},
                'GE': {'company': 'General Electric Company', 'exchange': 'NYSE', 'sector': 'Industrial'},
                'F': {'company': 'Ford Motor Company', 'exchange': 'NYSE', 'sector': 'Automotive'},
                'T': {'company': 'AT&T Inc.', 'exchange': 'NYSE', 'sector': 'Communication'},
                'VZ': {'company': 'Verizon Communications Inc.', 'exchange': 'NYSE', 'sector': 'Communication'},
                'MMM': {'company': '3M Company', 'exchange': 'NYSE', 'sector': 'Industrial'},
                'RTX': {'company': 'Raytheon Technologies Corporation', 'exchange': 'NYSE', 'sector': 'Aerospace'},
                'ABBV': {'company': 'AbbVie Inc.', 'exchange': 'NYSE', 'sector': 'Healthcare'},
                'ACN': {'company': 'Accenture plc', 'exchange': 'NYSE', 'sector': 'Technology'},
                'AMGN': {'company': 'Amgen Inc.', 'exchange': 'NASDAQ', 'sector': 'Healthcare'},
                'AVGO': {'company': 'Broadcom Inc.', 'exchange': 'NASDAQ', 'sector': 'Technology'},
                'BABA': {'company': 'Alibaba Group Holding Limited', 'exchange': 'NYSE', 'sector': 'Technology'},
                'BKNG': {'company': 'Booking Holdings Inc.', 'exchange': 'NASDAQ', 'sector': 'Consumer'},
                'COST': {'company': 'Costco Wholesale Corporation', 'exchange': 'NASDAQ', 'sector': 'Consumer'},
                'DHR': {'company': 'Danaher Corporation', 'exchange': 'NYSE', 'sector': 'Healthcare'},
                'GOOG': {'company': 'Alphabet Inc. Class C', 'exchange': 'NASDAQ', 'sector': 'Technology'},
                'HON': {'company': 'Honeywell International Inc.', 'exchange': 'NASDAQ', 'sector': 'Industrial'},
                'ISRG': {'company': 'Intuitive Surgical Inc.', 'exchange': 'NASDAQ', 'sector': 'Healthcare'},
                'LIN': {'company': 'Linde plc', 'exchange': 'NYSE', 'sector': 'Materials'},
                'LLY': {'company': 'Eli Lilly and Company', 'exchange': 'NYSE', 'sector': 'Healthcare'},
                'MA': {'company': 'Mastercard Incorporated', 'exchange': 'NYSE', 'sector': 'Financial'},
                'MRNA': {'company': 'Moderna Inc.', 'exchange': 'NASDAQ', 'sector': 'Healthcare'},
                'MRK': {'company': 'Merck & Co. Inc.', 'exchange': 'NYSE', 'sector': 'Healthcare'},
                'NEE': {'company': 'NextEra Energy Inc.', 'exchange': 'NYSE', 'sector': 'Utilities'},
                'NVO': {'company': 'Novo Nordisk A/S', 'exchange': 'NYSE', 'sector': 'Healthcare'},
                'NVS': {'company': 'Novartis AG', 'exchange': 'NYSE', 'sector': 'Healthcare'},
                'PFE': {'company': 'Pfizer Inc.', 'exchange': 'NYSE', 'sector': 'Healthcare'},
                'PYPL': {'company': 'PayPal Holdings Inc.', 'exchange': 'NASDAQ', 'sector': 'Financial'},
                'QCOM': {'company': 'QUALCOMM Incorporated', 'exchange': 'NASDAQ', 'sector': 'Technology'},
                'SAP': {'company': 'SAP SE', 'exchange': 'NYSE', 'sector': 'Technology'},
                'SHOP': {'company': 'Shopify Inc.', 'exchange': 'NYSE', 'sector': 'Technology'},
                'SNY': {'company': 'Sanofi', 'exchange': 'NYSE', 'sector': 'Healthcare'},
                'TM': {'company': 'Toyota Motor Corporation', 'exchange': 'NYSE', 'sector': 'Automotive'},
                'TSM': {'company': 'Taiwan Semiconductor Manufacturing Company Limited', 'exchange': 'NYSE', 'sector': 'Technology'},
                'UL': {'company': 'Unilever PLC', 'exchange': 'NYSE', 'sector': 'Consumer'},
                'UNH': {'company': 'UnitedHealth Group Incorporated', 'exchange': 'NYSE', 'sector': 'Healthcare'},
                'V': {'company': 'Visa Inc.', 'exchange': 'NYSE', 'sector': 'Financial'},
                'ZM': {'company': 'Zoom Video Communications Inc.', 'exchange': 'NASDAQ', 'sector': 'Technology'},
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
    
    def safe_get(self, data: dict, key: str, default: float = 0.0) -> float:
        """Safely extract numeric values from data"""
        value = data.get(key, default)
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return default
        return float(value)
    
    def collect_comprehensive_data(self, ticker: str) -> Tuple[List[ComprehensiveDataPoint], bool, str]:
        """Collect comprehensive data for a single ticker"""
        try:
            self.logger.info(f"Collecting comprehensive data for {ticker}")
            
            # Get ticker info
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Check if delisted
            is_delisted = False
            delisted_date = None
            
            if 'delistedDate' in info and info['delistedDate']:
                is_delisted = True
                delisted_date = pd.to_datetime(info['delistedDate']).date()
            elif 'regularMarketPrice' not in info or info.get('regularMarketPrice') is None:
                try:
                    recent_data = stock.history(period="1mo")
                    if recent_data.empty:
                        is_delisted = True
                except:
                    is_delisted = True
            
            # Get historical data with multiple strategies
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
            
            # Convert to comprehensive data points
            data_points = []
            
            for date_idx, row in historical_data.iterrows():
                try:
                    adj_close = row.get('Adj Close', row.get('Close', 0))
                    if pd.isna(adj_close):
                        adj_close = row.get('Close', 0)
                    
                    data_point = ComprehensiveDataPoint(
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
                        volatility_1y=self.calculate_volatility(historical_data),
                        max_drawdown_5y=self.calculate_max_drawdown(historical_data),
                        sector=self.get_sector(ticker),
                        industry=info.get('industry', 'Unknown'),
                        exchange=self.get_exchange(ticker),
                        is_delisted=is_delisted,
                        delisted_date=delisted_date,
                        created_at=datetime.now()
                    )
                    
                    data_points.append(data_point)
                    
                except Exception as e:
                    self.logger.warning(f"Error processing data point for {ticker} on {date_idx.date()}: {e}")
                    continue
            
            self.logger.info(f"Successfully collected {len(data_points)} comprehensive data points for {ticker}")
            return data_points, is_delisted, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting comprehensive data for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            return [], True, error_msg
    
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
            if len(hist_data) > 12:
                returns = hist_data['Close'].pct_change().dropna()
                return returns.std() * np.sqrt(52) * 100  # Annualized weekly volatility
            return 0.0
        except:
            return 0.0
    
    def calculate_max_drawdown(self, hist_data: pd.DataFrame) -> float:
        """Calculate maximum drawdown"""
        try:
            peak = hist_data['Close'].expanding().max()
            drawdown = (hist_data['Close'] - peak) / peak
            return drawdown.min() * 100
        except:
            return 0.0
    
    def get_sector(self, ticker: str) -> str:
        """Get sector for ticker"""
        metadata = self.stock_universe.get(ticker, {})
        return metadata.get('sector', 'Unknown')
    
    def get_exchange(self, ticker: str) -> str:
        """Get exchange for ticker"""
        metadata = self.stock_universe.get(ticker, {})
        return metadata.get('exchange', 'Unknown')
    
    def save_data_points(self, data_points: List[ComprehensiveDataPoint], ticker: str, status: str, error_msg: str = ""):
        """Save comprehensive data points to database"""
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
                        dp.pe_ratio, dp.pb_ratio, dp.peg_ratio, dp.ps_ratio, dp.dividend_yield,
                        dp.roe, dp.roa, dp.roic, dp.debt_to_equity, dp.current_ratio, dp.fcf_yield,
                        dp.eps_ttm, dp.eps_growth_5y, dp.revenue_growth_5y, dp.gross_margin,
                        dp.operating_margin, dp.net_margin, dp.beta, dp.volatility_1y, dp.max_drawdown_5y,
                        dp.sector, dp.industry, dp.exchange, dp.is_delisted, dp.delisted_date
                    ))
                
                # Bulk insert - Fixed column count
                cursor.executemany('''
                    INSERT OR REPLACE INTO comprehensive_data (
                        ticker, date, open_price, high_price, low_price, close_price,
                        volume, adjusted_close, market_cap, pe_ratio, pb_ratio, peg_ratio,
                        ps_ratio, dividend_yield, roe, roa, roic, debt_to_equity, current_ratio,
                        fcf_yield, eps_ttm, eps_growth_5y, revenue_growth_5y, gross_margin,
                        operating_margin, net_margin, beta, volatility_1y, max_drawdown_5y,
                        sector, industry, exchange, is_delisted, delisted_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data_to_insert)
            
            # Update progress
            cursor.execute('''
                INSERT OR REPLACE INTO collection_progress (
                    ticker, status, last_attempt, attempts, records_collected,
                    last_error, is_delisted, delisted_date, exchange
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ticker, status, datetime.now(), 1, len(data_points),
                error_msg, data_points[0].is_delisted if data_points else False,
                data_points[0].delisted_date if data_points else None,
                data_points[0].exchange if data_points else self.get_exchange(ticker)
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
    
    def process_ticker_batch(self, ticker_batch: List[str]) -> Tuple[int, int]:
        """Process a batch of tickers"""
        batch_records = 0
        batch_errors = 0
        
        for ticker in ticker_batch:
            try:
                self.logger.info(f"Processing {ticker} - {self.stock_universe.get(ticker, {}).get('company', ticker)}")
                
                # Collect comprehensive data
                data_points, is_delisted, error_msg = self.collect_comprehensive_data(ticker)
                
                # Save data
                records_saved = self.save_data_points(data_points, ticker, 
                                                    "completed" if data_points else "failed", 
                                                    error_msg)
                
                # Update statistics
                with self.lock:
                    if data_points:
                        self.progress['completed_tickers'].append(ticker)
                        batch_records += records_saved
                        self.stats['successful'] += 1
                        self.stats['total_records'] += records_saved
                        self.logger.info(f"✓ {ticker}: {records_saved} records saved")
                    else:
                        self.progress['failed_tickers'].append(ticker)
                        batch_errors += 1
                        self.stats['failed'] += 1
                        self.logger.warning(f"✗ {ticker}: {error_msg}")
                    
                    self.stats['total_processed'] += 1
                    self.stats['last_update'] = datetime.now()
                
                # Smart delay between tickers
                self.smart_delay()
                
            except Exception as e:
                self.logger.error(f"Unexpected error processing {ticker}: {e}")
                with self.lock:
                    self.progress['failed_tickers'].append(ticker)
                    batch_errors += 1
                    self.stats['failed'] += 1
                    self.stats['total_processed'] += 1
        
        return batch_records, batch_errors
    
    def run_comprehensive_collection(self, max_tickers: int = None):
        """Run comprehensive data collection session"""
        session_start = datetime.now()
        self.stats['start_time'] = session_start
        
        # Get pending tickers
        pending_tickers = [t for t in self.stock_universe.keys() 
                          if t not in self.progress['completed_tickers']]
        
        if max_tickers:
            pending_tickers = pending_tickers[:max_tickers]
        
        self.logger.info(f"Starting comprehensive collection session for {len(pending_tickers)} tickers")
        self.logger.info(f"Rate limiting: {self.min_delay}-{self.max_delay} seconds between requests")
        self.logger.info(f"Batch size: {self.batch_size} tickers per batch")
        self.logger.info(f"Max workers: {self.max_workers}")
        
        session_records = 0
        session_errors = 0
        
        # Process in batches with limited concurrency
        for i in range(0, len(pending_tickers), self.batch_size):
            batch = pending_tickers[i:i + self.batch_size]
            
            if len(batch) == 1:
                # Single ticker - process directly
                batch_records, batch_errors = self.process_ticker_batch(batch)
            else:
                # Multiple tickers - use limited concurrency
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Split batch into smaller chunks for concurrent processing
                    chunk_size = max(1, len(batch) // self.max_workers)
                    chunks = [batch[j:j + chunk_size] for j in range(0, len(batch), chunk_size)]
                    
                    futures = [executor.submit(self.process_ticker_batch, chunk) for chunk in chunks]
                    
                    for future in as_completed(futures):
                        try:
                            chunk_records, chunk_errors = future.result()
                            session_records += chunk_records
                            session_errors += chunk_errors
                        except Exception as e:
                            self.logger.error(f"Error in concurrent processing: {e}")
                            session_errors += len(batch)
            
            # Save progress after each batch
            self.save_progress()
            
            # Longer delay between batches
            if i + self.batch_size < len(pending_tickers):
                batch_delay = random.uniform(5, 10)  # 5-10 seconds between batches
                self.logger.info(f"Batch complete. Waiting {batch_delay:.1f} seconds before next batch...")
                time.sleep(batch_delay)
        
        # Log session results
        session_end = datetime.now()
        session_duration = (session_end - session_start).total_seconds()
        
        self.logger.info(f"Session complete: {session_records} records, {session_errors} errors, {session_duration:.1f}s")
        
        return session_records, session_errors
    
    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get basic stats
            cursor.execute('SELECT COUNT(*) FROM comprehensive_data')
            total_records = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM comprehensive_data')
            unique_tickers = cursor.fetchone()[0]
            
            cursor.execute('SELECT MIN(date), MAX(date) FROM comprehensive_data')
            date_range = cursor.fetchone()
            
            cursor.execute('SELECT COUNT(*) FROM comprehensive_data WHERE is_delisted = 1')
            delisted_records = cursor.fetchone()[0]
            
            # Get records per exchange
            cursor.execute('''
                SELECT exchange, COUNT(*) as record_count, COUNT(DISTINCT ticker) as ticker_count
                FROM comprehensive_data 
                GROUP BY exchange 
                ORDER BY record_count DESC
            ''')
            exchange_stats = cursor.fetchall()
            
            conn.close()
            
            return {
                'total_records': total_records,
                'unique_tickers': unique_tickers,
                'date_range': date_range,
                'delisted_records': delisted_records,
                'exchange_stats': exchange_stats
            }
            
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {}
    
    def print_progress_summary(self):
        """Print comprehensive progress summary"""
        print("\n" + "="*100)
        print("COMPREHENSIVE DATA COLLECTION PROGRESS")
        print("="*100)
        
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
            
            if db_stats['exchange_stats']:
                print(f"\nRecords by Exchange:")
                for exchange, record_count, ticker_count in db_stats['exchange_stats']:
                    print(f"  {exchange}: {record_count:,} records ({ticker_count} tickers)")
        
        print("="*100)

def main():
    """Main function for comprehensive data collection"""
    collector = ComprehensiveDataCollector()
    
    print("="*100)
    print("COMPREHENSIVE DATA COLLECTOR")
    print("="*100)
    print(f"Stock Universe: {len(collector.stock_universe)} stocks")
    print(f"Exchanges: US, ASX, NZX")
    print(f"Date Range: {collector.start_date} to {collector.end_date}")
    print(f"Rate Limiting: {collector.min_delay}-{collector.max_delay} seconds between requests")
    print(f"Batch Size: {collector.batch_size} tickers per batch")
    print(f"Max Workers: {collector.max_workers}")
    print("="*100)
    
    # Run collection session (limit to 5 stocks for testing)
    records_collected, errors = collector.run_comprehensive_collection(max_tickers=5)
    
    # Print summary
    collector.print_progress_summary()
    
    print(f"\nCollection session complete!")
    print(f"Records collected: {records_collected:,}")
    print(f"Errors: {errors}")
    print(f"Database: {collector.db_path}")
    print(f"Progress file: {collector.progress_file}")
    
    print(f"\nTo continue collection, run this script again.")
    print(f"It will automatically resume from where it left off.")

if __name__ == "__main__":
    main()
