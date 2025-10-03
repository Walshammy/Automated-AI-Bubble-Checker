#!/usr/bin/env python3
"""
Enhanced Data Collection Script
Tests and collects additional free data from yfinance for serious analysis
"""

import pandas as pd
import numpy as np
import yfinance as yf
import sqlite3
import time
import os
from datetime import datetime, date
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import warnings

warnings.filterwarnings('ignore')

@dataclass
class AnalystRating:
    """Analyst rating data"""
    ticker: str
    rating_date: date
    firm: str
    action: str  # upgrade, downgrade, maintain
    from_grade: str
    to_grade: str
    created_at: datetime

@dataclass
class EarningsPoint:
    """Earnings history data"""
    ticker: str
    quarter_date: date
    reported_eps: float
    estimated_eps: float
    surprise_pct: float
    revenue: float
    created_at: datetime

@dataclass
class InstitutionalHolding:
    """Institutional holding data"""
    ticker: str
    snapshot_date: date
    holder_name: str
    shares: float
    percentage: float
    value: float
    created_at: datetime

@dataclass
class SectorPerformance:
    """Sector performance data"""
    date: date
    sector: str
    price: float
    created_at: datetime

@dataclass
class MarketIndicator:
    """Market indicator data"""
    date: date
    indicator_name: str
    value: float
    created_at: datetime

@dataclass
class CorporateAction:
    """Corporate action data"""
    ticker: str
    action_date: date
    action_type: str  # split, dividend, buyback
    value: float
    description: str
    created_at: datetime

@dataclass
class ExtendedPriceData:
    """Extended price metrics"""
    ticker: str
    date: date
    fifty_two_week_high: float
    fifty_two_week_low: float
    avg_volume_10d: float
    price_to_52w_high_pct: float
    bid: float
    ask: float
    day_range_low: float
    day_range_high: float
    previous_close: float
    created_at: datetime

class EnhancedDataCollector:
    """Collects additional free data for serious analysis"""
    
    def __init__(self, db_path: str = "db_additions_test/enhanced_data.db", backup_path: str = r"C:\Users\james\Downloads\Stock Valuation\enhanced_data.db"):
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Database setup
        self.db_path = db_path
        self.backup_path = backup_path
        self.init_database()
        
        # Load full stock universe
        self.load_stock_universe()
        
        # Test tickers (mix of US, ASX, NZX) - for validation only
        self.test_tickers = [
            'AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN',  # US tech
            'BHP.AX', 'CBA.AX', 'WBC.AX', 'ANZ.AX', 'NAB.AX',  # ASX major
            'AIR.NZ', 'ANZ.NZ', 'FPH.NZ', 'SKC.NZ', 'RYM.NZ'  # NZX major
        ]
        
        # Sector ETFs for benchmarking
        self.sector_etfs = {
            'XLK': 'Technology',
            'XLF': 'Financials', 
            'XLE': 'Energy',
            'XLV': 'Healthcare',
            'XLY': 'Consumer Discretionary',
            'XLP': 'Consumer Staples',
            'XLI': 'Industrials',
            'XLB': 'Materials',
            'XLU': 'Utilities',
            'XLRE': 'Real Estate'
        }
        
        # Market indicators
        self.market_indicators = {
            '^VIX': 'Volatility Index',
            '^TNX': '10-Year Treasury',
            '^TYX': '30-Year Treasury',
            'GC=F': 'Gold Futures',
            'CL=F': 'Oil Futures',
            'AUDUSD=X': 'AUD/USD',
            'NZDUSD=X': 'NZD/USD'
        }
        
    def init_database(self):
        """Initialize enhanced database with additional tables"""
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Analyst ratings table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS analyst_ratings (
                    ticker TEXT,
                    rating_date DATE,
                    firm TEXT,
                    action TEXT,
                    from_grade TEXT,
                    to_grade TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (ticker, rating_date, firm)
                )
            ''')
            
            # Earnings history table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS earnings_history (
                    ticker TEXT,
                    quarter_date DATE,
                    reported_eps REAL,
                    estimated_eps REAL,
                    surprise_pct REAL,
                    revenue REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (ticker, quarter_date)
                )
            ''')
            
            # Institutional holdings table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS institutional_holdings (
                    ticker TEXT,
                    snapshot_date DATE,
                    holder_name TEXT,
                    shares REAL,
                    percentage REAL,
                    value REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (ticker, snapshot_date, holder_name)
                )
            ''')
            
            # Sector performance table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sector_performance (
                    date DATE,
                    sector TEXT,
                    price REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (date, sector)
                )
            ''')
            
            # Market indicators table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS market_indicators (
                    date DATE,
                    indicator_name TEXT,
                    value REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (date, indicator_name)
                )
            ''')
            
            # Corporate actions table
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
            
            # Extended price data table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS extended_price_data (
                    ticker TEXT,
                    date DATE,
                    fifty_two_week_high REAL,
                    fifty_two_week_low REAL,
                    avg_volume_10d REAL,
                    price_to_52w_high_pct REAL,
                    bid REAL,
                    ask REAL,
                    day_range_low REAL,
                    day_range_high REAL,
                    previous_close REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (ticker, date)
                )
            ''')
            
            # Test results table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS test_results (
                    ticker TEXT,
                    test_type TEXT,
                    success BOOLEAN,
                    records_collected INTEGER,
                    error_message TEXT,
                    test_date DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            self.logger.info(f"Enhanced database initialized: {self.db_path}")
            
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise
    
    def load_stock_universe(self):
        """Load the full stock universe from the Excel file (same as main collector)"""
        try:
            import pandas as pd
            
            # Load NZX stocks from Excel file (Sheet1)
            nzx_df = pd.read_excel('data_collection/NZX_ASX.xlsx', sheet_name='Sheet1')
            nzx_stocks = {}
            for _, row in nzx_df.iterrows():
                ticker = row['Code']
                if pd.notna(ticker):
                    nzx_stocks[f"{ticker}.NZ"] = {
                        'company': str(row.get('Company', 'Unknown')),
                        'exchange': 'NZX',
                        'sector': 'Unknown'  # NZX sheet doesn't have sector info
                    }
            
            # Load ASX stocks from Excel file (Sheet3)
            asx_df = pd.read_excel('data_collection/NZX_ASX.xlsx', sheet_name='Sheet3')
            asx_stocks = {}
            for _, row in asx_df.iterrows():
                ticker = row['Code']
                if pd.notna(ticker):
                    asx_stocks[f"{ticker}.AX"] = {
                        'company': str(row.get('Company', 'Unknown')),
                        'exchange': 'ASX',
                        'sector': str(row.get('Sector', 'Unknown'))
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
            # Use minimal fallback
            self.stock_universe = {'AAPL': {'company': 'Apple Inc.', 'exchange': 'NASDAQ', 'sector': 'Technology'}}
    
    def test_analyst_ratings(self, ticker: str) -> Tuple[bool, int, str]:
        """Test collection of analyst ratings"""
        try:
            self.logger.info(f"Testing analyst ratings for {ticker}")
            
            stock = yf.Ticker(ticker)
            recommendations = stock.recommendations
            
            records_collected = 0
            
            if recommendations is not None and not recommendations.empty:
                for date_idx, row in recommendations.iterrows():
                    try:
                        # Handle different date formats
                        if hasattr(date_idx, 'date'):
                            rating_date = date_idx.date()
                        elif hasattr(date_idx, 'to_pydatetime'):
                            rating_date = date_idx.to_pydatetime().date()
                        else:
                            rating_date = datetime.now().date()
                        
                        ar = AnalystRating(
                            ticker=ticker,
                            rating_date=rating_date,
                            firm=str(row.get('Firm', 'Unknown')),
                            action=str(row.get('Action', 'Unknown')),
                            from_grade=str(row.get('From Grade', 'Unknown')),
                            to_grade=str(row.get('To Grade', 'Unknown')),
                            created_at=datetime.now()
                        )
                        
                        self.save_analyst_rating(ar)
                        records_collected += 1
                        
                    except Exception as e:
                        self.logger.warning(f"Error processing rating for {ticker} on {date_idx}: {e}")
                        continue
            
            return True, records_collected, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting analyst ratings for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            return False, 0, error_msg
    
    def test_earnings_history(self, ticker: str) -> Tuple[bool, int, str]:
        """Test collection of earnings history using simplified approach"""
        try:
            self.logger.info(f"Testing earnings history for {ticker}")
            
            stock = yf.Ticker(ticker)
            
            records_collected = 0
            
            # Try to get earnings data from info (simpler approach)
            info = stock.info
            
            if info:
                # Get annual earnings data from info
                try:
                    # Extract earnings data from info
                    if 'earningsGrowth' in info and info['earningsGrowth']:
                        ep = EarningsPoint(
                            ticker=ticker,
                            quarter_date=datetime.now().date(),
                            reported_eps=float(info.get('trailingEps', 0)) if info.get('trailingEps') else 0,
                            estimated_eps=float(info.get('forwardEps', 0)) if info.get('forwardEps') else 0,
                            surprise_pct=0,  # Not available in info
                            revenue=float(info.get('totalRevenue', 0)) if info.get('totalRevenue') else 0,
                            created_at=datetime.now()
                        )
                        
                        self.save_earnings_point(ep)
                        records_collected += 1
                        
                except Exception as e:
                    self.logger.warning(f"Error processing earnings info for {ticker}: {e}")
            
            # Try to get quarterly earnings dates (simpler method)
            try:
                # Use a different approach to get earnings data
                hist_data = stock.history(period="2y", interval="1d")
                if hist_data is not None and not hist_data.empty:
                    # Calculate quarterly earnings proxy from price data
                    quarterly_dates = hist_data.resample('Q').last().index
                    
                    for q_date in quarterly_dates[-4:]:  # Last 4 quarters
                        try:
                            ep = EarningsPoint(
                                ticker=ticker,
                                quarter_date=q_date.date(),
                                reported_eps=0,  # Not available without lxml
                                estimated_eps=0,
                                surprise_pct=0,
                                revenue=0,
                                created_at=datetime.now()
                            )
                            
                            self.save_earnings_point(ep)
                            records_collected += 1
                            
                        except Exception as e:
                            self.logger.warning(f"Error processing quarterly data for {ticker}: {e}")
                            continue
                            
            except Exception as e:
                self.logger.warning(f"Error getting quarterly data for {ticker}: {e}")
            
            return True, records_collected, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting earnings history for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            return False, 0, error_msg
    
    def test_corporate_actions(self, ticker: str) -> Tuple[bool, int, str]:
        """Test collection of corporate actions (dividends, splits)"""
        try:
            self.logger.info(f"Testing corporate actions for {ticker}")
            
            stock = yf.Ticker(ticker)
            
            records_collected = 0
            
            # Test dividend data
            try:
                dividend_data = stock.dividends
                if dividend_data is not None and not dividend_data.empty:
                    for date_idx, dividend in dividend_data.items():
                        try:
                            # Handle different date formats
                            if hasattr(date_idx, 'date'):
                                action_date = date_idx.date()
                            elif hasattr(date_idx, 'to_pydatetime'):
                                action_date = date_idx.to_pydatetime().date()
                            else:
                                action_date = datetime.now().date()
                            
                            ca = CorporateAction(
                                ticker=ticker,
                                action_date=action_date,
                                action_type='dividend',
                                value=float(dividend),
                                description=f"Dividend payment: ${dividend:.4f}",
                                created_at=datetime.now()
                            )
                            
                            self.save_corporate_action(ca)
                            records_collected += 1
                            
                        except Exception as e:
                            self.logger.warning(f"Error processing dividend for {ticker}: {e}")
                            continue
                            
            except Exception as e:
                self.logger.warning(f"Error getting dividend data for {ticker}: {e}")
            
            # Test stock splits
            try:
                splits_data = stock.splits
                if splits_data is not None and not splits_data.empty:
                    for date_idx, split_ratio in splits_data.items():
                        try:
                            # Handle different date formats
                            if hasattr(date_idx, 'date'):
                                action_date = date_idx.date()
                            elif hasattr(date_idx, 'to_pydatetime'):
                                action_date = date_idx.to_pydatetime().date()
                            else:
                                action_date = datetime.now().date()
                            
                            ca = CorporateAction(
                                ticker=ticker,
                                action_date=action_date,
                                action_type='split',
                                value=float(split_ratio),
                                description=f"Stock split: {split_ratio}:1",
                                created_at=datetime.now()
                            )
                            
                            self.save_corporate_action(ca)
                            records_collected += 1
                            
                        except Exception as e:
                            self.logger.warning(f"Error processing split for {ticker}: {e}")
                            continue
                            
            except Exception as e:
                self.logger.warning(f"Error getting splits data for {ticker}: {e}")
            
            return True, records_collected, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting corporate actions for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            return False, 0, error_msg
    
    def test_institutional_holdings(self, ticker: str) -> Tuple[bool, int, str]:
        """Test collection of institutional holdings"""
        try:
            self.logger.info(f"Testing institutional holdings for {ticker}")
            
            stock = yf.Ticker(ticker)
            institutional_holders = stock.institutional_holders
            mutual_fund_holders = stock.mutualfund_holders
            
            records_collected = 0
            
            # Process institutional holders
            if institutional_holders is not None and not institutional_holders.empty:
                for _, row in institutional_holders.iterrows():
                    try:
                        ih = InstitutionalHolding(
                            ticker=ticker,
                            snapshot_date=datetime.now().date(),  # yfinance doesn't provide dates
                            holder_name=str(row.get('Holder', 'Unknown')),
                            shares=float(row.get('Shares', 0)) if pd.notna(row.get('Shares')) else 0,
                            percentage=float(row.get('% Out', 0)) if pd.notna(row.get('% Out')) else 0,
                            value=float(row.get('Value', 0)) if pd.notna(row.get('Value')) else 0,
                            created_at=datetime.now()
                        )
                        
                        self.save_institutional_holding(ih)
                        records_collected += 1
                        
                    except Exception as e:
                        self.logger.warning(f"Error processing institutional holder for {ticker}: {e}")
                        continue
            
            # Process mutual fund holders
            if mutual_fund_holders is not None and not mutual_fund_holders.empty:
                for _, row in mutual_fund_holders.iterrows():
                    try:
                        ih = InstitutionalHolding(
                            ticker=ticker,
                            snapshot_date=datetime.now().date(),
                            holder_name=str(row.get('Holder', 'Unknown')),
                            shares=float(row.get('Shares', 0)) if pd.notna(row.get('Shares')) else 0,
                            percentage=float(row.get('% Out', 0)) if pd.notna(row.get('% Out')) else 0,
                            value=float(row.get('Value', 0)) if pd.notna(row.get('Value')) else 0,
                            created_at=datetime.now()
                        )
                        
                        self.save_institutional_holding(ih)
                        records_collected += 1
                        
                    except Exception as e:
                        self.logger.warning(f"Error processing mutual fund holder for {ticker}: {e}")
                        continue
            
            return True, records_collected, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting institutional holdings for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            return False, 0, error_msg
    
    def test_extended_price_data(self, ticker: str) -> Tuple[bool, int, str]:
        """Test collection of extended price metrics"""
        try:
            self.logger.info(f"Testing extended price data for {ticker}")
            
            stock = yf.Ticker(ticker)
            info = stock.info
            
            records_collected = 0
            
            # Get historical data for 52-week calculations
            hist_data = stock.history(period="1y", interval="1d")
            
            if hist_data is not None and not hist_data.empty:
                # Calculate 52-week high/low
                fifty_two_week_high = float(hist_data['High'].max())
                fifty_two_week_low = float(hist_data['Low'].min())
                
                # Calculate 10-day average volume
                avg_volume_10d = float(hist_data['Volume'].tail(10).mean()) if len(hist_data) >= 10 else 0
                
                # Get current price for 52-week high percentage
                current_price = float(info.get('currentPrice', 0)) if info.get('currentPrice') else 0
                price_to_52w_high_pct = (current_price / fifty_two_week_high * 100) if fifty_two_week_high > 0 else 0
                
                epd = ExtendedPriceData(
                    ticker=ticker,
                    date=datetime.now().date(),
                    fifty_two_week_high=fifty_two_week_high,
                    fifty_two_week_low=fifty_two_week_low,
                    avg_volume_10d=avg_volume_10d,
                    price_to_52w_high_pct=price_to_52w_high_pct,
                    bid=float(info.get('bid', 0)) if info.get('bid') else 0,
                    ask=float(info.get('ask', 0)) if info.get('ask') else 0,
                    day_range_low=float(info.get('dayLow', 0)) if info.get('dayLow') else 0,
                    day_range_high=float(info.get('dayHigh', 0)) if info.get('dayHigh') else 0,
                    previous_close=float(info.get('previousClose', 0)) if info.get('previousClose') else 0,
                    created_at=datetime.now()
                )
                
                self.save_extended_price_data(epd)
                records_collected += 1
            
            return True, records_collected, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting extended price data for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            return False, 0, error_msg
    
    def test_sector_performance(self) -> Tuple[bool, int, str]:
        """Test collection of sector performance data"""
        try:
            self.logger.info("Testing sector performance collection")
            
            records_collected = 0
            
            for etf_ticker, sector_name in self.sector_etfs.items():
                try:
                    etf = yf.Ticker(etf_ticker)
                    hist_data = etf.history(period="1mo", interval="1d")
                    
                    if hist_data is not None and not hist_data.empty:
                        for date_idx, row in hist_data.iterrows():
                            sp = SectorPerformance(
                                date=date_idx.date(),
                                sector=sector_name,
                                price=float(row['Close']),
                                created_at=datetime.now()
                            )
                            
                            self.save_sector_performance(sp)
                            records_collected += 1
                            
                except Exception as e:
                    self.logger.warning(f"Error getting {sector_name} data: {e}")
                    continue
            
            return True, records_collected, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting sector performance: {str(e)}"
            self.logger.error(error_msg)
            return False, 0, error_msg
    
    def test_market_indicators(self) -> Tuple[bool, int, str]:
        """Test collection of market indicators"""
        try:
            self.logger.info("Testing market indicators collection")
            
            records_collected = 0
            
            for indicator_ticker, indicator_name in self.market_indicators.items():
                try:
                    indicator = yf.Ticker(indicator_ticker)
                    hist_data = indicator.history(period="1mo", interval="1d")
                    
                    if hist_data is not None and not hist_data.empty:
                        for date_idx, row in hist_data.iterrows():
                            mi = MarketIndicator(
                                date=date_idx.date(),
                                indicator_name=indicator_name,
                                value=float(row['Close']),
                                created_at=datetime.now()
                            )
                            
                            self.save_market_indicator(mi)
                            records_collected += 1
                            
                except Exception as e:
                    self.logger.warning(f"Error getting {indicator_name} data: {e}")
                    continue
            
            return True, records_collected, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting market indicators: {str(e)}"
            self.logger.error(error_msg)
            return False, 0, error_msg
    
    def save_analyst_rating(self, ar: AnalystRating):
        """Save analyst rating to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO analyst_ratings (
                ticker, rating_date, firm, action, from_grade, to_grade
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (ar.ticker, ar.rating_date, ar.firm, ar.action, ar.from_grade, ar.to_grade))
        
        conn.commit()
        conn.close()
    
    def save_earnings_point(self, ep: EarningsPoint):
        """Save earnings point to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO earnings_history (
                ticker, quarter_date, reported_eps, estimated_eps, surprise_pct, revenue
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (ep.ticker, ep.quarter_date, ep.reported_eps, ep.estimated_eps, ep.surprise_pct, ep.revenue))
        
        conn.commit()
        conn.close()
    
    def save_corporate_action(self, ca: CorporateAction):
        """Save corporate action to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO corporate_actions (
                ticker, action_date, action_type, value, description
            ) VALUES (?, ?, ?, ?, ?)
        ''', (ca.ticker, ca.action_date, ca.action_type, ca.value, ca.description))
        
        conn.commit()
        conn.close()
    
    def save_institutional_holding(self, ih: InstitutionalHolding):
        """Save institutional holding to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO institutional_holdings (
                ticker, snapshot_date, holder_name, shares, percentage, value
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (ih.ticker, ih.snapshot_date, ih.holder_name, ih.shares, ih.percentage, ih.value))
        
        conn.commit()
        conn.close()
    
    def save_sector_performance(self, sp: SectorPerformance):
        """Save sector performance to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO sector_performance (
                date, sector, price
            ) VALUES (?, ?, ?)
        ''', (sp.date, sp.sector, sp.price))
        
        conn.commit()
        conn.close()
    
    def save_market_indicator(self, mi: MarketIndicator):
        """Save market indicator to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO market_indicators (
                date, indicator_name, value
            ) VALUES (?, ?, ?)
        ''', (mi.date, mi.indicator_name, mi.value))
        
        conn.commit()
        conn.close()
    
    def save_extended_price_data(self, epd: ExtendedPriceData):
        """Save extended price data to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO extended_price_data (
                ticker, date, fifty_two_week_high, fifty_two_week_low,
                avg_volume_10d, price_to_52w_high_pct, bid, ask,
                day_range_low, day_range_high, previous_close
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            epd.ticker, epd.date, epd.fifty_two_week_high, epd.fifty_two_week_low,
            epd.avg_volume_10d, epd.price_to_52w_high_pct, epd.bid, epd.ask,
            epd.day_range_low, epd.day_range_high, epd.previous_close
        ))
        
        conn.commit()
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
            self.logger.info(f"Enhanced database copied to backup location: {self.backup_path}")
            
        except Exception as e:
            self.logger.error(f"Error copying enhanced database to backup: {e}")
    
    def save_test_result(self, ticker: str, test_type: str, success: bool, records: int, error: str):
        """Save test result to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO test_results (
                ticker, test_type, success, records_collected, error_message
            ) VALUES (?, ?, ?, ?, ?)
        ''', (ticker, test_type, success, records, error))
        
        conn.commit()
        conn.close()
    
    def run_comprehensive_test(self):
        """Run comprehensive test of all enhanced data types"""
        print("=" * 80)
        print("ENHANCED DATA COLLECTION COMPREHENSIVE TEST")
        print("=" * 80)
        
        total_tests = 0
        successful_tests = 0
        total_records = 0
        
        # Test analyst ratings
        print(f"\n1. TESTING ANALYST RATINGS")
        print("-" * 50)
        for ticker in self.test_tickers:
            success, records, error = self.test_analyst_ratings(ticker)
            self.save_test_result(ticker, 'analyst_ratings', success, records, error)
            
            total_tests += 1
            if success:
                successful_tests += 1
                total_records += records
                print(f"+ {ticker}: {records} analyst ratings")
            else:
                print(f"- {ticker}: {error}")
            
            time.sleep(1)  # Rate limiting
        
        # Test earnings history
        print(f"\n2. TESTING EARNINGS HISTORY")
        print("-" * 50)
        for ticker in self.test_tickers:
            success, records, error = self.test_earnings_history(ticker)
            self.save_test_result(ticker, 'earnings_history', success, records, error)
            
            total_tests += 1
            if success:
                successful_tests += 1
                total_records += records
                print(f"+ {ticker}: {records} earnings records")
            else:
                print(f"- {ticker}: {error}")
            
            time.sleep(1)  # Rate limiting
        
        # Test corporate actions
        print(f"\n3. TESTING CORPORATE ACTIONS")
        print("-" * 50)
        for ticker in self.test_tickers:
            success, records, error = self.test_corporate_actions(ticker)
            self.save_test_result(ticker, 'corporate_actions', success, records, error)
            
            total_tests += 1
            if success:
                successful_tests += 1
                total_records += records
                print(f"+ {ticker}: {records} corporate actions")
            else:
                print(f"- {ticker}: {error}")
            
            time.sleep(1)  # Rate limiting
        
        # Test institutional holdings
        print(f"\n4. TESTING INSTITUTIONAL HOLDINGS")
        print("-" * 50)
        for ticker in self.test_tickers:
            success, records, error = self.test_institutional_holdings(ticker)
            self.save_test_result(ticker, 'institutional_holdings', success, records, error)
            
            total_tests += 1
            if success:
                successful_tests += 1
                total_records += records
                print(f"+ {ticker}: {records} institutional holdings")
            else:
                print(f"- {ticker}: {error}")
            
            time.sleep(1)  # Rate limiting
        
        # Test extended price data
        print(f"\n5. TESTING EXTENDED PRICE DATA")
        print("-" * 50)
        for ticker in self.test_tickers:
            success, records, error = self.test_extended_price_data(ticker)
            self.save_test_result(ticker, 'extended_price_data', success, records, error)
            
            total_tests += 1
            if success:
                successful_tests += 1
                total_records += records
                print(f"+ {ticker}: {records} extended price records")
            else:
                print(f"- {ticker}: {error}")
            
            time.sleep(1)  # Rate limiting
        
        # Test sector performance
        print(f"\n6. TESTING SECTOR PERFORMANCE")
        print("-" * 50)
        success, records, error = self.test_sector_performance()
        self.save_test_result('SECTOR', 'sector_performance', success, records, error)
        
        total_tests += 1
        if success:
            successful_tests += 1
            total_records += records
            print(f"+ Sector performance: {records} records")
        else:
            print(f"- Sector performance: {error}")
        
        # Test market indicators
        print(f"\n7. TESTING MARKET INDICATORS")
        print("-" * 50)
        success, records, error = self.test_market_indicators()
        self.save_test_result('MARKET', 'market_indicators', success, records, error)
        
        total_tests += 1
        if success:
            successful_tests += 1
            total_records += records
            print(f"+ Market indicators: {records} records")
        else:
            print(f"- Market indicators: {error}")
        
        # Summary
        print(f"\n" + "=" * 80)
        print(f"ENHANCED TEST SUMMARY")
        print(f"=" * 80)
        print(f"Total tests: {total_tests}")
        print(f"Successful: {successful_tests}")
        print(f"Success rate: {(successful_tests/total_tests)*100:.1f}%")
        print(f"Total records collected: {total_records:,}")
        
        # Database analysis
        self.analyze_enhanced_database()
    
    def analyze_enhanced_database(self):
        """Analyze the enhanced database results"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print(f"\nENHANCED DATABASE ANALYSIS")
            print("-" * 50)
            
            # Analyst ratings
            cursor.execute('SELECT COUNT(*) FROM analyst_ratings')
            ar_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM analyst_ratings')
            ar_tickers = cursor.fetchone()[0]
            
            # Earnings history
            cursor.execute('SELECT COUNT(*) FROM earnings_history')
            eh_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM earnings_history')
            eh_tickers = cursor.fetchone()[0]
            
            # Corporate actions
            cursor.execute('SELECT COUNT(*) FROM corporate_actions')
            ca_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM corporate_actions')
            ca_tickers = cursor.fetchone()[0]
            
            # Institutional holdings
            cursor.execute('SELECT COUNT(*) FROM institutional_holdings')
            ih_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM institutional_holdings')
            ih_tickers = cursor.fetchone()[0]
            
            # Extended price data
            cursor.execute('SELECT COUNT(*) FROM extended_price_data')
            epd_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM extended_price_data')
            epd_tickers = cursor.fetchone()[0]
            
            # Sector performance
            cursor.execute('SELECT COUNT(*) FROM sector_performance')
            sp_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(DISTINCT sector) FROM sector_performance')
            sp_sectors = cursor.fetchone()[0]
            
            # Market indicators
            cursor.execute('SELECT COUNT(*) FROM market_indicators')
            mi_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(DISTINCT indicator_name) FROM market_indicators')
            mi_indicators = cursor.fetchone()[0]
            
            print(f"Analyst Ratings: {ar_count:,} records ({ar_tickers} tickers)")
            print(f"Earnings History: {eh_count:,} records ({eh_tickers} tickers)")
            print(f"Corporate Actions: {ca_count:,} records ({ca_tickers} tickers)")
            print(f"Institutional Holdings: {ih_count:,} records ({ih_tickers} tickers)")
            print(f"Extended Price Data: {epd_count:,} records ({epd_tickers} tickers)")
            print(f"Sector Performance: {sp_count:,} records ({sp_sectors} sectors)")
            print(f"Market Indicators: {mi_count:,} records ({mi_indicators} indicators)")
            
            # Coverage analysis by exchange
            print(f"\nCOVERAGE ANALYSIS BY EXCHANGE")
            print("-" * 50)
            
            # US stocks
            us_tickers = [t for t in self.test_tickers if '.' not in t]
            cursor.execute('''
                SELECT COUNT(DISTINCT ticker) FROM analyst_ratings 
                WHERE ticker IN ({})
            '''.format(','.join('?' * len(us_tickers))), us_tickers)
            us_ar_coverage = cursor.fetchone()[0]
            
            cursor.execute('''
                SELECT COUNT(DISTINCT ticker) FROM earnings_history 
                WHERE ticker IN ({})
            '''.format(','.join('?' * len(us_tickers))), us_tickers)
            us_eh_coverage = cursor.fetchone()[0]
            
            print(f"US Stocks ({len(us_tickers)} tested):")
            print(f"  Analyst Ratings: {us_ar_coverage}/{len(us_tickers)} ({us_ar_coverage/len(us_tickers)*100:.1f}%)")
            print(f"  Earnings History: {us_eh_coverage}/{len(us_tickers)} ({us_eh_coverage/len(us_tickers)*100:.1f}%)")
            
            # ASX stocks
            asx_tickers = [t for t in self.test_tickers if '.AX' in t]
            cursor.execute('''
                SELECT COUNT(DISTINCT ticker) FROM analyst_ratings 
                WHERE ticker IN ({})
            '''.format(','.join('?' * len(asx_tickers))), asx_tickers)
            asx_ar_coverage = cursor.fetchone()[0]
            
            cursor.execute('''
                SELECT COUNT(DISTINCT ticker) FROM earnings_history 
                WHERE ticker IN ({})
            '''.format(','.join('?' * len(asx_tickers))), asx_tickers)
            asx_eh_coverage = cursor.fetchone()[0]
            
            print(f"ASX Stocks ({len(asx_tickers)} tested):")
            print(f"  Analyst Ratings: {asx_ar_coverage}/{len(asx_tickers)} ({asx_ar_coverage/len(asx_tickers)*100:.1f}%)")
            print(f"  Earnings History: {asx_eh_coverage}/{len(asx_tickers)} ({asx_eh_coverage/len(asx_tickers)*100:.1f}%)")
            
            # NZX stocks
            nzx_tickers = [t for t in self.test_tickers if '.NZ' in t]
            cursor.execute('''
                SELECT COUNT(DISTINCT ticker) FROM analyst_ratings 
                WHERE ticker IN ({})
            '''.format(','.join('?' * len(nzx_tickers))), nzx_tickers)
            nzx_ar_coverage = cursor.fetchone()[0]
            
            cursor.execute('''
                SELECT COUNT(DISTINCT ticker) FROM earnings_history 
                WHERE ticker IN ({})
            '''.format(','.join('?' * len(nzx_tickers))), nzx_tickers)
            nzx_eh_coverage = cursor.fetchone()[0]
            
            print(f"NZX Stocks ({len(nzx_tickers)} tested):")
            print(f"  Analyst Ratings: {nzx_ar_coverage}/{len(nzx_tickers)} ({nzx_ar_coverage/len(nzx_tickers)*100:.1f}%)")
            print(f"  Earnings History: {nzx_eh_coverage}/{len(nzx_tickers)} ({nzx_eh_coverage/len(nzx_tickers)*100:.1f}%)")
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error analyzing enhanced database: {e}")
    
    def collect_enhanced_data_for_all_stocks(self, max_stocks: int = None, data_types: list = None):
        """Collect enhanced data for all stocks in the universe"""
        if data_types is None:
            data_types = ['analyst_ratings', 'earnings_history', 'corporate_actions', 'institutional_holdings', 'extended_price_data']
        
        if max_stocks is None:
            max_stocks = len(self.stock_universe)
        
        # Get list of all tickers
        all_tickers = list(self.stock_universe.keys())[:max_stocks]
        
        print("=" * 80)
        print("ENHANCED DATA COLLECTION FOR ALL STOCKS")
        print("=" * 80)
        print(f"Total stocks to process: {len(all_tickers)}")
        print(f"Data types: {', '.join(data_types)}")
        print(f"Rate limiting: 1 second between requests")
        print("=" * 80)
        
        total_records = 0
        successful_stocks = 0
        failed_stocks = 0
        
        for i, ticker in enumerate(all_tickers, 1):
            print(f"\n[{i}/{len(all_tickers)}] Processing {ticker}...")
            
            stock_success = True
            stock_records = 0
            
            for data_type in data_types:
                try:
                    if data_type == 'analyst_ratings':
                        success, records, error = self.test_analyst_ratings(ticker)
                    elif data_type == 'earnings_history':
                        success, records, error = self.test_earnings_history(ticker)
                    elif data_type == 'corporate_actions':
                        success, records, error = self.test_corporate_actions(ticker)
                    elif data_type == 'institutional_holdings':
                        success, records, error = self.test_institutional_holdings(ticker)
                    elif data_type == 'extended_price_data':
                        success, records, error = self.test_extended_price_data(ticker)
                    else:
                        continue
                    
                    if success:
                        stock_records += records
                        print(f"  + {data_type}: {records} records")
                    else:
                        print(f"  - {data_type}: {error}")
                        stock_success = False
                    
                except Exception as e:
                    print(f"  - {data_type}: Error - {e}")
                    stock_success = False
                
                # Rate limiting
                time.sleep(1)
            
            if stock_success:
                successful_stocks += 1
                total_records += stock_records
                print(f"  + {ticker}: {stock_records} total records")
            else:
                failed_stocks += 1
                print(f"  - {ticker}: Failed")
            
            # Progress update every 50 stocks
            if i % 50 == 0:
                print(f"\n--- PROGRESS UPDATE ---")
                print(f"Processed: {i}/{len(all_tickers)} stocks")
                print(f"Successful: {successful_stocks}")
                print(f"Failed: {failed_stocks}")
                print(f"Total records: {total_records:,}")
                print(f"Success rate: {(successful_stocks/i)*100:.1f}%")
        
        # Final summary
        print(f"\n" + "=" * 80)
        print(f"ENHANCED DATA COLLECTION COMPLETE!")
        print(f"=" * 80)
        print(f"Total stocks processed: {len(all_tickers)}")
        print(f"Successful: {successful_stocks}")
        print(f"Failed: {failed_stocks}")
        print(f"Success rate: {(successful_stocks/len(all_tickers))*100:.1f}%")
        print(f"Total records collected: {total_records:,}")
        print(f"Database: {self.db_path}")
        print(f"=" * 80)
        
        # Final database analysis
        self.analyze_enhanced_database()
        
        # Copy to backup location
        self.copy_to_backup()

def main():
    """Main function for enhanced data collection"""
    collector = EnhancedDataCollector()
    
    # Ask user what to do
    print("Enhanced Data Collector Options:")
    print("1. Run test with sample stocks (15 stocks)")
    print("2. Collect data for ALL stocks in universe (2,487 stocks)")
    print("3. Collect data for first 100 stocks")
    print("4. Collect data for first 500 stocks")
    
    choice = input("\nEnter your choice (1-4): ").strip()
    
    if choice == "1":
        collector.run_comprehensive_test()
    elif choice == "2":
        collector.collect_enhanced_data_for_all_stocks()
    elif choice == "3":
        collector.collect_enhanced_data_for_all_stocks(max_stocks=100)
    elif choice == "4":
        collector.collect_enhanced_data_for_all_stocks(max_stocks=500)
    else:
        print("Invalid choice. Running test with sample stocks...")
        collector.run_comprehensive_test()

if __name__ == "__main__":
    main()
