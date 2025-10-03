#!/usr/bin/env python3
"""
UNIFIED STOCK DATA COLLECTOR
============================

A comprehensive data collection system that merges all previous functionality:
- Historical price data (OHLCV)
- Current fundamental metrics
- Enhanced data (analyst ratings, earnings, corporate actions, etc.)
- All data types in a single unified database

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
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class HistoricalPricePoint:
    """Historical price data point"""
    ticker: str
    date: date
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    adjusted_close: float
    exchange: str
    is_delisted: bool = False
    delisted_date: Optional[date] = None
    created_at: datetime = None

@dataclass
class CurrentFundamentals:
    """Current fundamental metrics snapshot"""
    ticker: str
    snapshot_date: date
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    peg_ratio: Optional[float] = None
    ps_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    roic: Optional[float] = None
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    fcf_yield: Optional[float] = None
    eps_ttm: Optional[float] = None
    eps_growth_5y: Optional[float] = None
    revenue_growth_5y: Optional[float] = None
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    net_margin: Optional[float] = None
    beta: Optional[float] = None
    volatility_1y: Optional[float] = None
    max_drawdown_5y: Optional[float] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    exchange: str = ""
    is_delisted: bool = False
    delisted_date: Optional[date] = None
    created_at: datetime = None

@dataclass
class AnalystRating:
    """Analyst rating data"""
    ticker: str
    rating_date: date
    firm: str
    action: str
    from_grade: str
    to_grade: str
    created_at: datetime = None

@dataclass
class EarningsHistory:
    """Earnings history data"""
    ticker: str
    quarter_date: date
    reported_eps: Optional[float] = None
    estimated_eps: Optional[float] = None
    surprise_pct: Optional[float] = None
    revenue: Optional[float] = None
    created_at: datetime = None

@dataclass
class CorporateAction:
    """Corporate action data (dividends, splits)"""
    ticker: str
    action_date: date
    action_type: str
    value: Optional[float] = None
    description: Optional[str] = None
    created_at: datetime = None

@dataclass
class InstitutionalHolding:
    """Institutional holding data"""
    ticker: str
    snapshot_date: date
    holder_name: str
    shares: Optional[float] = None
    percentage: Optional[float] = None
    value: Optional[float] = None
    created_at: datetime = None

@dataclass
class ExtendedPriceData:
    """Extended price metrics"""
    ticker: str
    snapshot_date: date
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None
    avg_volume_10d: Optional[float] = None
    price_to_52w_high_pct: Optional[float] = None
    created_at: datetime = None

@dataclass
class SectorPerformance:
    """Sector performance data"""
    date: date
    sector: str
    ticker: str
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: Optional[float] = None
    volume: Optional[int] = None
    adjusted_close: Optional[float] = None
    created_at: datetime = None

@dataclass
class MarketIndicator:
    """Market indicator data"""
    date: date
    indicator_name: str
    value: Optional[float] = None
    created_at: datetime = None

class UnifiedStockDataCollector:
    """Unified stock data collector combining all previous functionality"""
    
    def __init__(self, db_path: str = "data_collection/unified_stock_data.db", 
                 backup_path: str = r"C:\Users\james\Downloads\Stock Valuation\unified_stock_data.db"):
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Database setup
        self.db_path = db_path
        self.backup_path = backup_path
        self.init_database()
        
        # Rate limiting
        self.base_delay = 1.0
        self.max_delay = 10.0
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
        self.max_workers = 1  # Conservative approach
        
        # Progress tracking
        self.progress_file = "data_collection/unified_collection_progress.json"
        
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
        self.logger.info("Unified database initialized successfully")
    
    def load_stock_universe(self) -> Dict[str, str]:
        """Load stock universe from Excel file"""
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
            
            # Add US stocks
            us_stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK-B', 'UNH', 'JNJ']
            for ticker in us_stocks:
                stock_universe[ticker] = f"{ticker} Corporation"
            
            self.logger.info(f"Loaded stock universe: {len(stock_universe)} stocks")
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
    
    def collect_historical_prices(self, ticker: str) -> List[HistoricalPricePoint]:
        """Collect historical price data for a ticker"""
        try:
            stock = yf.Ticker(ticker)
            
            # Try weekly data first (more reliable)
            historical_data = stock.history(period="max", interval="1wk")
            
            if historical_data.empty:
                # Fallback to daily data
                historical_data = stock.history(period="max", interval="1d")
            
            if historical_data.empty:
                self.logger.warning(f"No price data found for {ticker}")
                return []
            
            # Get stock info for exchange and delisting status
            info = stock.info
            exchange = self.safe_get(info, 'exchange', 'Unknown')
            is_delisted = self.safe_get(info, 'regularMarketPrice') is None
            
            delisted_date = None
            if is_delisted:
                try:
                    delisted_date = datetime.strptime(self.safe_get(info, 'delistingDate', ''), '%Y-%m-%d').date()
                except:
                    pass
            
            # Convert to data points
            data_points = []
            for date_idx, row in historical_data.iterrows():
                try:
                    adj_close = row.get('Adj Close', row.get('Close', 0))
                    if pd.isna(adj_close):
                        adj_close = row.get('Close', 0)
                    
                    data_point = HistoricalPricePoint(
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
                    data_points.append(data_point)
                except Exception as e:
                    self.logger.warning(f"Error processing price data for {ticker} on {date_idx.date()}: {e}")
                    continue
            
            self.logger.info(f"Collected {len(data_points)} price points for {ticker}")
            return data_points
            
        except Exception as e:
            self.logger.error(f"Error collecting historical prices for {ticker}: {e}")
            return []
    
    def collect_current_fundamentals(self, ticker: str) -> Optional[CurrentFundamentals]:
        """Collect current fundamental metrics for a ticker"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Calculate volatility and max drawdown from historical data
            historical_data = stock.history(period="1y")
            volatility_1y = None
            max_drawdown_5y = None
            
            if not historical_data.empty:
                returns = historical_data['Close'].pct_change().dropna()
                volatility_1y = returns.std() * (252 ** 0.5)  # Annualized volatility
                
                # Calculate max drawdown
                cumulative = (1 + returns).cumprod()
                running_max = cumulative.expanding().max()
                drawdown = (cumulative - running_max) / running_max
                max_drawdown_5y = drawdown.min()
            
            fundamentals = CurrentFundamentals(
                ticker=ticker,
                snapshot_date=date.today(),
                market_cap=self.safe_get(info, 'marketCap'),
                pe_ratio=self.safe_get(info, 'trailingPE'),
                pb_ratio=self.safe_get(info, 'priceToBook'),
                peg_ratio=self.safe_get(info, 'pegRatio'),
                ps_ratio=self.safe_get(info, 'priceToSalesTrailing12Months'),
                dividend_yield=self.safe_get(info, 'dividendYield'),
                roe=self.safe_get(info, 'returnOnEquity'),
                roa=self.safe_get(info, 'returnOnAssets'),
                roic=self.safe_get(info, 'returnOnInvestedCapital'),
                debt_to_equity=self.safe_get(info, 'debtToEquity'),
                current_ratio=self.safe_get(info, 'currentRatio'),
                fcf_yield=self.safe_get(info, 'freeCashflow') / self.safe_get(info, 'marketCap') if self.safe_get(info, 'marketCap') else None,
                eps_ttm=self.safe_get(info, 'trailingEps'),
                eps_growth_5y=self.safe_get(info, 'earningsGrowth'),
                revenue_growth_5y=self.safe_get(info, 'revenueGrowth'),
                gross_margin=self.safe_get(info, 'grossMargins'),
                operating_margin=self.safe_get(info, 'operatingMargins'),
                net_margin=self.safe_get(info, 'profitMargins'),
                beta=self.safe_get(info, 'beta'),
                volatility_1y=volatility_1y,
                max_drawdown_5y=max_drawdown_5y,
                sector=self.safe_get(info, 'sector'),
                industry=self.safe_get(info, 'industry'),
                exchange=self.safe_get(info, 'exchange', 'Unknown'),
                is_delisted=self.safe_get(info, 'regularMarketPrice') is None,
                delisted_date=None,
                created_at=datetime.now()
            )
            
            return fundamentals
            
        except Exception as e:
            self.logger.error(f"Error collecting fundamentals for {ticker}: {e}")
            return None
    
    def collect_analyst_ratings(self, ticker: str) -> List[AnalystRating]:
        """Collect analyst ratings for a ticker"""
        try:
            stock = yf.Ticker(ticker)
            recommendations = stock.recommendations
            
            if recommendations is None or recommendations.empty:
                return []
            
            ratings = []
            for date_idx, row in recommendations.iterrows():
                try:
                    # Handle different date formats
                    if hasattr(date_idx, 'date'):
                        rating_date = date_idx.date()
                    elif hasattr(date_idx, 'to_pydatetime'):
                        rating_date = date_idx.to_pydatetime().date()
                    else:
                        continue
                    
                    rating = AnalystRating(
                        ticker=ticker,
                        rating_date=rating_date,
                        firm=self.safe_get(row, 'Firm', ''),
                        action=self.safe_get(row, 'Action', ''),
                        from_grade=self.safe_get(row, 'From Grade', ''),
                        to_grade=self.safe_get(row, 'To Grade', ''),
                        created_at=datetime.now()
                    )
                    ratings.append(rating)
                except Exception as e:
                    self.logger.warning(f"Error processing rating for {ticker} on {date_idx}: {e}")
                    continue
            
            return ratings
            
        except Exception as e:
            self.logger.error(f"Error collecting analyst ratings for {ticker}: {e}")
            return []
    
    def collect_earnings_history(self, ticker: str) -> List[EarningsHistory]:
        """Collect earnings history for a ticker"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            earnings_history = []
            
            # Get quarterly earnings from info
            quarterly_earnings = self.safe_get(info, 'quarterlyEarnings', [])
            if quarterly_earnings:
                for quarter in quarterly_earnings:
                    try:
                        quarter_date = datetime.strptime(quarter['date'], '%Y-%m-%d').date()
                        earnings = EarningsHistory(
                            ticker=ticker,
                            quarter_date=quarter_date,
                            reported_eps=self.safe_get(quarter, 'actual'),
                            estimated_eps=self.safe_get(quarter, 'estimate'),
                            surprise_pct=self.safe_get(quarter, 'surprise'),
                            revenue=self.safe_get(quarter, 'revenue'),
                            created_at=datetime.now()
                        )
                        earnings_history.append(earnings)
                    except Exception as e:
                        self.logger.warning(f"Error processing earnings for {ticker}: {e}")
                        continue
            
            return earnings_history
            
        except Exception as e:
            self.logger.error(f"Error collecting earnings history for {ticker}: {e}")
            return []
    
    def collect_corporate_actions(self, ticker: str) -> List[CorporateAction]:
        """Collect corporate actions (dividends, splits) for a ticker"""
        try:
            stock = yf.Ticker(ticker)
            
            actions = []
            
            # Get dividends
            dividends = stock.dividends
            if not dividends.empty:
                for date_idx, value in dividends.items():
                    action = CorporateAction(
                        ticker=ticker,
                        action_date=date_idx.date(),
                        action_type='dividend',
                        value=float(value),
                        description=f"Dividend: ${value:.4f}",
                        created_at=datetime.now()
                    )
                    actions.append(action)
            
            # Get stock splits
            splits = stock.splits
            if not splits.empty:
                for date_idx, value in splits.items():
                    action = CorporateAction(
                        ticker=ticker,
                        action_date=date_idx.date(),
                        action_type='split',
                        value=float(value),
                        description=f"Stock Split: {value}:1",
                        created_at=datetime.now()
                    )
                    actions.append(action)
            
            return actions
            
        except Exception as e:
            self.logger.error(f"Error collecting corporate actions for {ticker}: {e}")
            return []
    
    def collect_institutional_holdings(self, ticker: str) -> List[InstitutionalHolding]:
        """Collect institutional holdings for a ticker"""
        try:
            stock = yf.Ticker(ticker)
            institutional_holders = stock.institutional_holders
            
            if institutional_holders is None or institutional_holders.empty:
                return []
            
            holdings = []
            snapshot_date = date.today()
            
            for _, row in institutional_holders.iterrows():
                holding = InstitutionalHolding(
                    ticker=ticker,
                    snapshot_date=snapshot_date,
                    holder_name=self.safe_get(row, 'Holder', ''),
                    shares=self.safe_get(row, 'Shares'),
                    percentage=self.safe_get(row, 'Date Reported'),
                    value=self.safe_get(row, 'Value'),
                    created_at=datetime.now()
                )
                holdings.append(holding)
            
            return holdings
            
        except Exception as e:
            self.logger.error(f"Error collecting institutional holdings for {ticker}: {e}")
            return []
    
    def collect_extended_price_data(self, ticker: str) -> Optional[ExtendedPriceData]:
        """Collect extended price metrics for a ticker"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Get 52-week high/low
            fifty_two_week_high = self.safe_get(info, 'fiftyTwoWeekHigh')
            fifty_two_week_low = self.safe_get(info, 'fiftyTwoWeekLow')
            current_price = self.safe_get(info, 'currentPrice')
            
            # Calculate price to 52-week high percentage
            price_to_52w_high_pct = None
            if fifty_two_week_high and current_price:
                price_to_52w_high_pct = (current_price / fifty_two_week_high) * 100
            
            # Get average volume (10-day)
            avg_volume_10d = self.safe_get(info, 'averageVolume10days')
            
            extended_data = ExtendedPriceData(
                ticker=ticker,
                snapshot_date=date.today(),
                fifty_two_week_high=fifty_two_week_high,
                fifty_two_week_low=fifty_two_week_low,
                avg_volume_10d=avg_volume_10d,
                price_to_52w_high_pct=price_to_52w_high_pct,
                created_at=datetime.now()
            )
            
            return extended_data
            
        except Exception as e:
            self.logger.error(f"Error collecting extended price data for {ticker}: {e}")
            return None
    
    def save_price_points(self, data_points: List[HistoricalPricePoint]):
        """Save historical price points to database"""
        if not data_points:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Prepare data for insertion
            data_to_insert = []
            for point in data_points:
                data_to_insert.append((
                    point.ticker, point.date, point.open_price, point.high_price,
                    point.low_price, point.close_price, point.volume, point.adjusted_close,
                    point.exchange, point.is_delisted, point.delisted_date
                ))
            
            # Insert in batches
            cursor.executemany('''
                INSERT OR REPLACE INTO historical_prices (
                    ticker, date, open_price, high_price, low_price, close_price,
                    volume, adjusted_close, exchange, is_delisted, delisted_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            conn.commit()
            self.logger.info(f"Saved {len(data_points)} price points to database")
            
        except Exception as e:
            self.logger.error(f"Error saving price points: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def save_fundamentals(self, fundamentals: CurrentFundamentals):
        """Save current fundamentals to database"""
        if not fundamentals:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
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
                fundamentals.industry, fundamentals.exchange, fundamentals.is_delisted, fundamentals.delisted_date
            ))
            
            conn.commit()
            self.logger.info(f"Saved fundamentals for {fundamentals.ticker}")
            
        except Exception as e:
            self.logger.error(f"Error saving fundamentals: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def save_analyst_ratings(self, ratings: List[AnalystRating]):
        """Save analyst ratings to database"""
        if not ratings:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            data_to_insert = []
            for rating in ratings:
                data_to_insert.append((
                    rating.ticker, rating.rating_date, rating.firm, rating.action,
                    rating.from_grade, rating.to_grade
                ))
            
            cursor.executemany('''
                INSERT OR REPLACE INTO analyst_ratings (
                    ticker, rating_date, firm, action, from_grade, to_grade
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            conn.commit()
            self.logger.info(f"Saved {len(ratings)} analyst ratings")
            
        except Exception as e:
            self.logger.error(f"Error saving analyst ratings: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def save_earnings_history(self, earnings: List[EarningsHistory]):
        """Save earnings history to database"""
        if not earnings:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            data_to_insert = []
            for earning in earnings:
                data_to_insert.append((
                    earning.ticker, earning.quarter_date, earning.reported_eps,
                    earning.estimated_eps, earning.surprise_pct, earning.revenue
                ))
            
            cursor.executemany('''
                INSERT OR REPLACE INTO earnings_history (
                    ticker, quarter_date, reported_eps, estimated_eps, surprise_pct, revenue
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            conn.commit()
            self.logger.info(f"Saved {len(earnings)} earnings records")
            
        except Exception as e:
            self.logger.error(f"Error saving earnings history: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def save_corporate_actions(self, actions: List[CorporateAction]):
        """Save corporate actions to database"""
        if not actions:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            data_to_insert = []
            for action in actions:
                data_to_insert.append((
                    action.ticker, action.action_date, action.action_type,
                    action.value, action.description
                ))
            
            cursor.executemany('''
                INSERT OR REPLACE INTO corporate_actions (
                    ticker, action_date, action_type, value, description
                ) VALUES (?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            conn.commit()
            self.logger.info(f"Saved {len(actions)} corporate actions")
            
        except Exception as e:
            self.logger.error(f"Error saving corporate actions: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def save_institutional_holdings(self, holdings: List[InstitutionalHolding]):
        """Save institutional holdings to database"""
        if not holdings:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            data_to_insert = []
            for holding in holdings:
                data_to_insert.append((
                    holding.ticker, holding.snapshot_date, holding.holder_name,
                    holding.shares, holding.percentage, holding.value
                ))
            
            cursor.executemany('''
                INSERT OR REPLACE INTO institutional_holdings (
                    ticker, snapshot_date, holder_name, shares, percentage, value
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            conn.commit()
            self.logger.info(f"Saved {len(holdings)} institutional holdings")
            
        except Exception as e:
            self.logger.error(f"Error saving institutional holdings: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def save_extended_price_data(self, extended_data: ExtendedPriceData):
        """Save extended price data to database"""
        if not extended_data:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO extended_price_data (
                    ticker, snapshot_date, fifty_two_week_high, fifty_two_week_low,
                    avg_volume_10d, price_to_52w_high_pct
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                extended_data.ticker, extended_data.snapshot_date, extended_data.fifty_two_week_high,
                extended_data.fifty_two_week_low, extended_data.avg_volume_10d, extended_data.price_to_52w_high_pct
            ))
            
            conn.commit()
            self.logger.info(f"Saved extended price data for {extended_data.ticker}")
            
        except Exception as e:
            self.logger.error(f"Error saving extended price data: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def collect_all_data_for_ticker(self, ticker: str, name: str) -> Dict[str, int]:
        """Collect all data types for a single ticker"""
        self.logger.info(f"Processing {ticker} - {name}")
        
        records_collected = {
            'price_records': 0,
            'fundamental_records': 0,
            'analyst_ratings': 0,
            'earnings_history': 0,
            'corporate_actions': 0,
            'institutional_holdings': 0,
            'extended_price_data': 0
        }
        
        try:
            # Collect historical prices
            self.logger.info(f"Collecting historical prices for {ticker}")
            price_points = self.collect_historical_prices(ticker)
            if price_points:
                self.save_price_points(price_points)
                records_collected['price_records'] = len(price_points)
            
            # Collect current fundamentals
            self.logger.info(f"Collecting current fundamentals for {ticker}")
            fundamentals = self.collect_current_fundamentals(ticker)
            if fundamentals:
                self.save_fundamentals(fundamentals)
                records_collected['fundamental_records'] = 1
            
            # Collect enhanced data
            self.logger.info(f"Collecting analyst ratings for {ticker}")
            ratings = self.collect_analyst_ratings(ticker)
            if ratings:
                self.save_analyst_ratings(ratings)
                records_collected['analyst_ratings'] = len(ratings)
            
            self.logger.info(f"Collecting earnings history for {ticker}")
            earnings = self.collect_earnings_history(ticker)
            if earnings:
                self.save_earnings_history(earnings)
                records_collected['earnings_history'] = len(earnings)
            
            self.logger.info(f"Collecting corporate actions for {ticker}")
            actions = self.collect_corporate_actions(ticker)
            if actions:
                self.save_corporate_actions(actions)
                records_collected['corporate_actions'] = len(actions)
            
            self.logger.info(f"Collecting institutional holdings for {ticker}")
            holdings = self.collect_institutional_holdings(ticker)
            if holdings:
                self.save_institutional_holdings(holdings)
                records_collected['institutional_holdings'] = len(holdings)
            
            self.logger.info(f"Collecting extended price data for {ticker}")
            extended_data = self.collect_extended_price_data(ticker)
            if extended_data:
                self.save_extended_price_data(extended_data)
                records_collected['extended_price_data'] = 1
            
            total_records = sum(records_collected.values())
            self.logger.info(f"+ {ticker}: {total_records} total records")
            
            return records_collected
            
        except Exception as e:
            self.logger.error(f"Error processing {ticker}: {e}")
            return records_collected
    
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
    
    def run_unified_collection(self, target_percentage: float = 100.0):
        """Run unified data collection for all stocks"""
        self.logger.info("Starting unified data collection")
        
        # Load stock universe
        stock_universe = self.load_stock_universe()
        if not stock_universe:
            self.logger.error("No stock universe loaded")
            return
        
        total_stocks = len(stock_universe)
        target_stocks = int(total_stocks * target_percentage / 100)
        
        self.logger.info(f"Target: {target_stocks}/{total_stocks} stocks ({target_percentage}%)")
        
        session_price_records = 0
        session_fundamental_records = 0
        session_enhanced_records = 0
        session_errors = 0
        
        processed_count = 0
        
        # Process stocks in batches
        tickers = list(stock_universe.items())
        
        for i in range(0, min(target_stocks, len(tickers)), self.batch_size):
            batch = tickers[i:i + self.batch_size]
            
            # Process batch
            for ticker, name in batch:
                try:
                    records = self.collect_all_data_for_ticker(ticker, name)
                    
                    session_price_records += records['price_records']
                    session_fundamental_records += records['fundamental_records']
                    session_enhanced_records += sum([
                        records['analyst_ratings'],
                        records['earnings_history'],
                        records['corporate_actions'],
                        records['institutional_holdings'],
                        records['extended_price_data']
                    ])
                    
                    processed_count += 1
                    
                    # Adaptive delay
                    self.adaptive_delay(success=True)
                    
                except Exception as e:
                    self.logger.error(f"Error processing {ticker}: {e}")
                    session_errors += 1
                    self.adaptive_delay(success=False)
            
            # Checkpoint after each batch
            self.checkpoint_after_batch()
            
            # Progress update
            if processed_count % 50 == 0:
                self.logger.info(f"Checkpoint: Processed {processed_count}/{target_stocks} tickers")
        
        # Copy to backup location
        self.copy_to_backup()
        
        self.logger.info("Unified data collection completed")
        self.logger.info(f"Session Summary:")
        self.logger.info(f"  Price Records: {session_price_records:,}")
        self.logger.info(f"  Fundamental Records: {session_fundamental_records:,}")
        self.logger.info(f"  Enhanced Records: {session_enhanced_records:,}")
        self.logger.info(f"  Errors: {session_errors}")
        
        return session_price_records, session_fundamental_records, session_enhanced_records, session_errors

def main():
    """Main function"""
    print("=" * 80)
    print("UNIFIED STOCK DATA COLLECTOR")
    print("=" * 80)
    print("This collector combines all previous functionality:")
    print("+ Historical price data (OHLCV)")
    print("+ Current fundamental metrics")
    print("+ Enhanced data (analyst ratings, earnings, corporate actions, etc.)")
    print("+ All data in a single unified database")
    print("=" * 80)
    
    collector = UnifiedStockDataCollector()
    
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
        price_records, fundamental_records, enhanced_records, errors = collector.run_unified_collection(target_percentage)
        
        print("\n" + "=" * 80)
        print("COLLECTION COMPLETED!")
        print("=" * 80)
        print(f"Price Records: {price_records:,}")
        print(f"Fundamental Records: {fundamental_records:,}")
        print(f"Enhanced Records: {enhanced_records:,}")
        print(f"Errors: {errors}")
        print(f"Database: {collector.db_path}")
        print(f"Backup: {collector.backup_path}")
        print("=" * 80)
        
    except KeyboardInterrupt:
        print("\nCollection interrupted by user")
    except Exception as e:
        print(f"\nCollection failed: {e}")

if __name__ == "__main__":
    main()
