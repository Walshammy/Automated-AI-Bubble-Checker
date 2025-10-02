#!/usr/bin/env python3
"""
Consolidated Stock Valuation Scraper
Comprehensive valuation analysis for US, ASX, and NZX stocks with bubble detection
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
from enum import Enum
import json
import warnings

warnings.filterwarnings('ignore')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Sector(Enum):
    TECHNOLOGY = "Technology"
    FINANCIAL = "Financial"
    HEALTHCARE = "Healthcare"
    INDUSTRIAL = "Industrial"
    CONSUMER = "Consumer"
    ENERGY = "Energy"
    UTILITIES = "Utilities"
    REAL_ESTATE = "Real Estate"
    MATERIALS = "Materials"
    COMMUNICATION = "Communication"
    UNKNOWN = "Unknown"

@dataclass
class ValuationSummary:
    """Comprehensive valuation summary with bubble indicators"""
    ticker: str
    company_name: str
    current_price: float
    sector: str
    exchange: str
    
    # Key valuation metrics
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
    
    # Bubble indicators
    is_bubble_candidate: bool
    bubble_score: float
    bubble_warnings: List[str]
    
    # Valuation assessment
    is_cheap: bool
    is_quality: bool
    margin_of_safety: float
    confidence: float
    
    # Overall warnings
    warnings: List[str]
    timestamp: str

class ComprehensiveStockAnalyzer:
    """Comprehensive stock analyzer for US, ASX, and NZX markets"""
    
    def __init__(self, db_path: str = "stock_valuation_data.db"):
        self.db_path = db_path
        self.init_database()
        
        # Load stock universe from Excel file
        self.load_stock_universe()
        
        # Rate limiting settings
        self.min_delay = 2
        self.max_delay = 5
        self.max_retries = 3
        
        # Output directories
        self.output_dir = os.path.join(os.getcwd(), "valuation_results")
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Primary dataset path
        self.primary_dataset_path = os.path.join(self.output_dir, "comprehensive_valuation_dataset.xlsx")
    
    def init_database(self):
        """Initialize SQLite database for storing valuation data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS valuation_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    company_name TEXT,
                    current_price REAL,
                    sector TEXT,
                    exchange TEXT,
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
                    eps_growth_5y REAL,
                    revenue_growth_5y REAL,
                    gross_margin REAL,
                    operating_margin REAL,
                    net_margin REAL,
                    beta REAL,
                    volatility_1y REAL,
                    max_drawdown_5y REAL,
                    is_bubble_candidate BOOLEAN,
                    bubble_score REAL,
                    bubble_warnings TEXT,
                    is_cheap BOOLEAN,
                    is_quality BOOLEAN,
                    margin_of_safety REAL,
                    confidence REAL,
                    warnings TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, timestamp)
                )
            ''')
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker_timestamp ON valuation_data(ticker, timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bubble_score ON valuation_data(bubble_score)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sector ON valuation_data(sector)')
            
            conn.commit()
            conn.close()
            logger.info(f"Database initialized: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    def load_stock_universe(self):
        """Load comprehensive stock universe from Excel file"""
        try:
            # Load NZX stocks (Sheet1)
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
            
            # Load ASX stocks (Sheet3)
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
            }
            
            # Combine all stocks
            self.stock_universe = {**nzx_stocks, **asx_stocks, **us_stocks}
            
            logger.info(f"Loaded stock universe: {len(nzx_stocks)} NZX, {len(asx_stocks)} ASX, {len(us_stocks)} US stocks")
            logger.info(f"Total stocks: {len(self.stock_universe)}")
            
        except Exception as e:
            logger.error(f"Error loading stock universe: {e}")
            raise
    
    def get_sector(self, ticker: str) -> Sector:
        """Determine sector based on ticker and metadata"""
        metadata = self.stock_universe.get(ticker, {})
        
        # Use sector from metadata if available
        if metadata.get('sector'):
            sector_map = {
                'Technology': Sector.TECHNOLOGY,
                'Financial': Sector.FINANCIAL,
                'Financials': Sector.FINANCIAL,
                'Healthcare': Sector.HEALTHCARE,
                'Health Care': Sector.HEALTHCARE,
                'Industrial': Sector.INDUSTRIAL,
                'Industrials': Sector.INDUSTRIAL,
                'Consumer': Sector.CONSUMER,
                'Consumer Discretionary': Sector.CONSUMER,
                'Consumer Staples': Sector.CONSUMER,
                'Energy': Sector.ENERGY,
                'Utilities': Sector.UTILITIES,
                'Real Estate': Sector.REAL_ESTATE,
                'Materials': Sector.MATERIALS,
                'Communication': Sector.COMMUNICATION,
                'Information Technology': Sector.TECHNOLOGY,
                'Automotive': Sector.INDUSTRIAL,
                'Aerospace': Sector.INDUSTRIAL,
            }
            return sector_map.get(metadata['sector'], Sector.UNKNOWN)
        
        # Fallback to exchange-based classification
        if '.NZ' in ticker:
            return Sector.UNKNOWN  # NZX
        elif '.AX' in ticker:
            return Sector.UNKNOWN  # ASX
        else:
            return Sector.UNKNOWN  # US
    
    def safe_get(self, data: dict, key: str, default: float = 0.0) -> float:
        """Safely extract numeric values from data"""
        value = data.get(key, default)
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return default
        return float(value)
    
    def get_stock_data(self, ticker: str) -> Dict:
        """Get comprehensive stock data with historical context"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Get historical data for additional metrics
            hist_data = None
            try:
                hist_data = stock.history(period="5y", interval="1mo")
            except Exception as e:
                logger.warning(f"Could not get historical data for {ticker}: {e}")
            
            # Extract comprehensive metrics
            data = {
                'ticker': ticker,
                'company_name': self.stock_universe.get(ticker, {}).get('company', ticker),
                'current_price': self.safe_get(info, 'currentPrice'),
                'market_cap': self.safe_get(info, 'marketCap'),
                'pe_ratio': self.safe_get(info, 'trailingPE'),
                'pb_ratio': self.safe_get(info, 'priceToBook'),
                'ps_ratio': self.safe_get(info, 'priceToSalesTrailing12Months'),
                'peg_ratio': self.safe_get(info, 'pegRatio'),
                'dividend_yield': self.safe_get(info, 'dividendYield', 0) * 100,
                'eps_ttm': self.safe_get(info, 'trailingEps'),
                'eps_growth_5y': self.safe_get(info, 'earningsGrowth', 0) * 100,
                'revenue_growth_5y': self.safe_get(info, 'revenueGrowth', 0) * 100,
                'roe': self.safe_get(info, 'returnOnEquity', 0) * 100,
                'roa': self.safe_get(info, 'returnOnAssets', 0) * 100,
                'roic': self.safe_get(info, 'returnOnInvestedCapital', 0) * 100,
                'debt_to_equity': self.safe_get(info, 'debtToEquity'),
                'current_ratio': self.safe_get(info, 'currentRatio'),
                'quick_ratio': self.safe_get(info, 'quickRatio'),
                'fcf_ttm': self.safe_get(info, 'freeCashflow'),
                'net_income': self.safe_get(info, 'netIncomeToCommon'),
                'revenue_ttm': self.safe_get(info, 'totalRevenue'),
                'gross_margin': self.safe_get(info, 'grossMargins', 0) * 100,
                'operating_margin': self.safe_get(info, 'operatingMargins', 0) * 100,
                'net_margin': self.safe_get(info, 'profitMargins', 0) * 100,
                'beta': self.safe_get(info, 'beta', 1.0),
                'shares_outstanding': self.safe_get(info, 'sharesOutstanding'),
                'book_value_per_share': self.safe_get(info, 'bookValue'),
                'cash_per_share': self.safe_get(info, 'totalCashPerShare'),
                'debt_per_share': self.safe_get(info, 'totalDebtPerShare'),
                'sector': self.get_sector(ticker).value,
                'industry': info.get('industry', 'Unknown'),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            
            # Add historical metrics if available
            if hist_data is not None and not hist_data.empty:
                if len(hist_data) > 12:
                    returns = hist_data['Close'].pct_change().dropna()
                    data['volatility_1y'] = returns.std() * np.sqrt(12) * 100
                    data['max_drawdown_5y'] = self.calculate_max_drawdown(hist_data['Close'])
                
                if len(hist_data) >= 12:
                    data['price_change_1y'] = ((hist_data['Close'].iloc[-1] / hist_data['Close'].iloc[-12]) - 1) * 100
                if len(hist_data) >= 3:
                    data['price_change_3m'] = ((hist_data['Close'].iloc[-1] / hist_data['Close'].iloc[-3]) - 1) * 100
            
            return data
            
        except Exception as e:
            logger.error(f"Error getting data for {ticker}: {e}")
            return {}
    
    def calculate_max_drawdown(self, prices: pd.Series) -> float:
        """Calculate maximum drawdown from peak"""
        try:
            peak = prices.expanding().max()
            drawdown = (prices - peak) / peak
            return drawdown.min() * 100
        except:
            return 0.0
    
    def calculate_fcf_yield(self, data: Dict) -> float:
        """Calculate FCF yield"""
        fcf = data.get('fcf_ttm', 0)
        market_cap = data.get('market_cap', 0)
        
        if market_cap > 0 and fcf > 0:
            return (fcf / market_cap) * 100
        return 0.0
    
    def detect_bubble_indicators(self, data: Dict) -> Tuple[bool, float, List[str]]:
        """Detect potential bubble indicators"""
        warnings = []
        bubble_score = 0.0
        
        # High P/E ratio (bubble indicator)
        pe_ratio = data.get('pe_ratio', 0)
        if pe_ratio > 50:
            bubble_score += 0.3
            warnings.append(f"Extremely high P/E: {pe_ratio:.1f}")
        elif pe_ratio > 30:
            bubble_score += 0.2
            warnings.append(f"High P/E: {pe_ratio:.1f}")
        
        # High P/B ratio
        pb_ratio = data.get('pb_ratio', 0)
        if pb_ratio > 10:
            bubble_score += 0.2
            warnings.append(f"Extremely high P/B: {pb_ratio:.1f}")
        elif pb_ratio > 5:
            bubble_score += 0.1
            warnings.append(f"High P/B: {pb_ratio:.1f}")
        
        # High P/S ratio
        ps_ratio = data.get('ps_ratio', 0)
        if ps_ratio > 20:
            bubble_score += 0.2
            warnings.append(f"Extremely high P/S: {ps_ratio:.1f}")
        elif ps_ratio > 10:
            bubble_score += 0.1
            warnings.append(f"High P/S: {ps_ratio:.1f}")
        
        # High volatility
        volatility = data.get('volatility_1y', 0)
        if volatility > 50:
            bubble_score += 0.1
            warnings.append(f"High volatility: {volatility:.1f}%")
        
        # Negative fundamentals
        roe = data.get('roe', 0)
        if roe < 0:
            bubble_score += 0.2
            warnings.append(f"Negative ROE: {roe:.1f}%")
        
        # High debt
        debt_to_equity = data.get('debt_to_equity', 0)
        if debt_to_equity > 2:
            bubble_score += 0.1
            warnings.append(f"High debt-to-equity: {debt_to_equity:.2f}")
        
        # Negative FCF
        fcf_yield = self.calculate_fcf_yield(data)
        if fcf_yield < 0:
            bubble_score += 0.1
            warnings.append(f"Negative FCF yield: {fcf_yield:.1f}%")
        
        is_bubble_candidate = bubble_score >= 0.5
        return is_bubble_candidate, bubble_score, warnings
    
    def assess_quality(self, data: Dict) -> Tuple[bool, float, List[str]]:
        """Assess business quality"""
        warnings = []
        quality_score = 0.0
        
        # ROE assessment
        roe = data.get('roe', 0)
        if roe > 15:
            quality_score += 0.3
        elif roe > 10:
            quality_score += 0.2
        elif roe < 5:
            warnings.append(f"Low ROE: {roe:.1f}%")
        
        # Debt assessment
        debt_to_equity = data.get('debt_to_equity', 0)
        if debt_to_equity < 0.3:
            quality_score += 0.2
        elif debt_to_equity < 0.5:
            quality_score += 0.1
        elif debt_to_equity > 1.0:
            warnings.append(f"High debt-to-equity: {debt_to_equity:.2f}")
        
        # Current ratio assessment
        current_ratio = data.get('current_ratio', 0)
        if current_ratio > 1.5:
            quality_score += 0.1
        elif current_ratio < 1.0:
            warnings.append(f"Low current ratio: {current_ratio:.2f}")
        
        # FCF yield assessment
        fcf_yield = self.calculate_fcf_yield(data)
        if fcf_yield > 5:
            quality_score += 0.2
        elif fcf_yield > 3:
            quality_score += 0.1
        elif fcf_yield < 1:
            warnings.append(f"Low FCF yield: {fcf_yield:.1f}%")
        
        # Growth consistency
        eps_growth = data.get('eps_growth_5y', 0)
        revenue_growth = data.get('revenue_growth_5y', 0)
        
        if eps_growth > 0 and revenue_growth > 0:
            if abs(eps_growth - revenue_growth) < 5:
                quality_score += 0.1
        
        # Margin stability
        gross_margin = data.get('gross_margin', 0)
        if gross_margin > 30:
            quality_score += 0.1
        
        is_quality = quality_score >= 0.6
        return is_quality, quality_score, warnings
    
    def assess_value(self, data: Dict, sector: Sector) -> Tuple[bool, float, List[str]]:
        """Assess valuation attractiveness"""
        warnings = []
        value_score = 0.0
        
        # P/E ratio assessment
        pe_ratio = data.get('pe_ratio', 0)
        if pe_ratio > 0:
            if pe_ratio < 15:
                value_score += 0.4
            elif pe_ratio < 20:
                value_score += 0.2
            elif pe_ratio > 30:
                warnings.append(f"High P/E ratio: {pe_ratio:.1f}")
        
        # PEG ratio assessment
        peg_ratio = data.get('peg_ratio', 0)
        if peg_ratio > 0:
            if peg_ratio < 1.0:
                value_score += 0.3
            elif peg_ratio < 1.5:
                value_score += 0.1
            elif peg_ratio > 2.0:
                warnings.append(f"High PEG ratio: {peg_ratio:.2f}")
        
        # P/B ratio assessment
        pb_ratio = data.get('pb_ratio', 0)
        if pb_ratio > 0:
            if sector == Sector.FINANCIAL:
                if pb_ratio < 1.2:
                    value_score += 0.3
                elif pb_ratio < 1.5:
                    value_score += 0.1
                elif pb_ratio > 2.0:
                    warnings.append(f"High P/B for financial: {pb_ratio:.2f}")
            else:
                if pb_ratio < 1.5:
                    value_score += 0.3
                elif pb_ratio < 2.0:
                    value_score += 0.1
                elif pb_ratio > 3.0:
                    warnings.append(f"High P/B ratio: {pb_ratio:.2f}")
        
        # Dividend yield assessment
        dividend_yield = data.get('dividend_yield', 0)
        if dividend_yield > 4:
            value_score += 0.2
        elif dividend_yield > 2:
            value_score += 0.1
        
        # FCF yield assessment
        fcf_yield = self.calculate_fcf_yield(data)
        if fcf_yield > 5:
            value_score += 0.3
        elif fcf_yield > 3:
            value_score += 0.2
        elif fcf_yield < 1:
            warnings.append(f"Low FCF yield: {fcf_yield:.1f}%")
        
        is_cheap = value_score >= 0.6
        return is_cheap, value_score, warnings
    
    def calculate_margin_of_safety(self, data: Dict) -> float:
        """Calculate margin of safety using multiple methods"""
        current_price = data.get('current_price', 0)
        if current_price <= 0:
            return 0.0
        
        # Method 1: Graham's formula
        eps = data.get('eps_ttm', 0)
        growth_rate = data.get('eps_growth_5y', 0) / 100
        
        if eps > 0 and growth_rate > 0:
            graham_value = eps * (8.5 + 2 * growth_rate)
            graham_mos = ((graham_value - current_price) / current_price) * 100
        else:
            graham_mos = 0
        
        # Method 2: P/E reversion
        pe_ratio = data.get('pe_ratio', 0)
        if pe_ratio > 0 and eps > 0:
            fair_pe = 15
            if pe_ratio > fair_pe:
                pe_mos = ((fair_pe - pe_ratio) / pe_ratio) * 100
            else:
                pe_mos = ((pe_ratio - fair_pe) / fair_pe) * 100
        else:
            pe_mos = 0
        
        # Average the methods
        if graham_mos != 0 and pe_mos != 0:
            return (graham_mos + pe_mos) / 2
        elif graham_mos != 0:
            return graham_mos
        elif pe_mos != 0:
            return pe_mos
        else:
            return 0.0
    
    def analyze_stock(self, ticker: str) -> Optional[ValuationSummary]:
        """Comprehensive stock analysis with bubble detection"""
        logger.info(f"Analyzing {ticker}")
        
        # Get data
        data = self.get_stock_data(ticker)
        if not data or data.get('current_price', 0) <= 0:
            logger.warning(f"No valid data for {ticker}")
            return None
        
        # Determine sector and exchange
        sector = self.get_sector(ticker)
        metadata = self.stock_universe.get(ticker, {})
        exchange = metadata.get('exchange', 'Unknown')
        
        # Detect bubble indicators
        is_bubble_candidate, bubble_score, bubble_warnings = self.detect_bubble_indicators(data)
        
        # Assess quality and value
        is_quality, quality_score, quality_warnings = self.assess_quality(data)
        is_cheap, value_score, value_warnings = self.assess_value(data, sector)
        
        # Calculate margin of safety
        margin_of_safety = self.calculate_margin_of_safety(data)
        
        # Calculate confidence
        confidence = (quality_score + value_score) / 2
        
        # Combine warnings
        all_warnings = quality_warnings + value_warnings + bubble_warnings
        
        # Add data quality warnings
        if data.get('eps_ttm', 0) <= 0:
            all_warnings.append("Negative or zero EPS")
        if data.get('fcf_ttm', 0) <= 0:
            all_warnings.append("Negative FCF")
        if data.get('revenue_ttm', 0) <= 0:
            all_warnings.append("Negative revenue")
        
        return ValuationSummary(
            ticker=ticker,
            company_name=data.get('company_name', ticker),
            current_price=data.get('current_price', 0),
            sector=sector.value,
            exchange=exchange,
            pe_ratio=data.get('pe_ratio', 0),
            pb_ratio=data.get('pb_ratio', 0),
            peg_ratio=data.get('peg_ratio', 0),
            ps_ratio=data.get('ps_ratio', 0),
            dividend_yield=data.get('dividend_yield', 0),
            roe=data.get('roe', 0),
            roa=data.get('roa', 0),
            roic=data.get('roic', 0),
            debt_to_equity=data.get('debt_to_equity', 0),
            current_ratio=data.get('current_ratio', 0),
            fcf_yield=self.calculate_fcf_yield(data),
            eps_growth_5y=data.get('eps_growth_5y', 0),
            revenue_growth_5y=data.get('revenue_growth_5y', 0),
            gross_margin=data.get('gross_margin', 0),
            operating_margin=data.get('operating_margin', 0),
            net_margin=data.get('net_margin', 0),
            beta=data.get('beta', 1.0),
            volatility_1y=data.get('volatility_1y', 0),
            max_drawdown_5y=data.get('max_drawdown_5y', 0),
            is_bubble_candidate=is_bubble_candidate,
            bubble_score=bubble_score,
            bubble_warnings=bubble_warnings,
            is_cheap=is_cheap,
            is_quality=is_quality,
            margin_of_safety=margin_of_safety,
            confidence=confidence,
            warnings=all_warnings,
            timestamp=data.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        )
    
    def save_to_database(self, result: ValuationSummary):
        """Save valuation result to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO valuation_data (
                    ticker, company_name, current_price, sector, exchange,
                    pe_ratio, pb_ratio, peg_ratio, ps_ratio, dividend_yield,
                    roe, roa, roic, debt_to_equity, current_ratio, fcf_yield,
                    eps_growth_5y, revenue_growth_5y, gross_margin, operating_margin, net_margin,
                    beta, volatility_1y, max_drawdown_5y, is_bubble_candidate, bubble_score,
                    bubble_warnings, is_cheap, is_quality, margin_of_safety, confidence, warnings
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                result.ticker, result.company_name, result.current_price, result.sector, result.exchange,
                result.pe_ratio, result.pb_ratio, result.peg_ratio, result.ps_ratio, result.dividend_yield,
                result.roe, result.roa, result.roic, result.debt_to_equity, result.current_ratio, result.fcf_yield,
                result.eps_growth_5y, result.revenue_growth_5y, result.gross_margin, result.operating_margin, result.net_margin,
                result.beta, result.volatility_1y, result.max_drawdown_5y, result.is_bubble_candidate, result.bubble_score,
                '; '.join(result.bubble_warnings), result.is_cheap, result.is_quality, result.margin_of_safety, result.confidence,
                '; '.join(result.warnings)
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
    
    def save_to_excel(self, results: List[ValuationSummary]):
        """Save results to Excel file"""
        if not results:
            logger.warning("No results to save")
            return
        
        # Convert to DataFrame
        data = []
        for result in results:
            data.append({
                'Ticker': result.ticker,
                'Company': result.company_name,
                'Exchange': result.exchange,
                'Current Price': result.current_price,
                'Sector': result.sector,
                'P/E Ratio': result.pe_ratio,
                'P/B Ratio': result.pb_ratio,
                'PEG Ratio': result.peg_ratio,
                'P/S Ratio': result.ps_ratio,
                'Dividend Yield %': result.dividend_yield,
                'ROE %': result.roe,
                'ROA %': result.roa,
                'ROIC %': result.roic,
                'Debt/Equity': result.debt_to_equity,
                'Current Ratio': result.current_ratio,
                'FCF Yield %': result.fcf_yield,
                'EPS Growth 5Y %': result.eps_growth_5y,
                'Revenue Growth 5Y %': result.revenue_growth_5y,
                'Gross Margin %': result.gross_margin,
                'Operating Margin %': result.operating_margin,
                'Net Margin %': result.net_margin,
                'Beta': result.beta,
                'Volatility 1Y %': result.volatility_1y,
                'Max Drawdown 5Y %': result.max_drawdown_5y,
                'Is Bubble Candidate': result.is_bubble_candidate,
                'Bubble Score': result.bubble_score,
                'Bubble Warnings': '; '.join(result.bubble_warnings),
                'Is Cheap': result.is_cheap,
                'Is Quality': result.is_quality,
                'Margin of Safety %': result.margin_of_safety,
                'Confidence': result.confidence,
                'Warnings': '; '.join(result.warnings),
                'Timestamp': result.timestamp
            })
        
        df = pd.DataFrame(data)
        
        # Save to Excel
        df.to_excel(self.primary_dataset_path, index=False)
        logger.info(f"Results saved to {self.primary_dataset_path}")
        
        # Also save timestamped version
        timestamped_file = os.path.join(self.output_dir, f"valuation_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        df.to_excel(timestamped_file, index=False)
        logger.info(f"Timestamped results saved to {timestamped_file}")
    
    def smart_delay(self):
        """Implement smart delay with randomization"""
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)
    
    def run_analysis(self, max_stocks: int = None):
        """Run comprehensive analysis with bubble detection"""
        logger.info("Starting comprehensive stock valuation analysis with bubble detection")
        
        # Get stocks to analyze
        stocks_to_analyze = list(self.stock_universe.keys())
        if max_stocks:
            stocks_to_analyze = stocks_to_analyze[:max_stocks]
        
        results = []
        bubble_candidates = []
        value_candidates = []
        
        for i, ticker in enumerate(stocks_to_analyze):
            try:
                logger.info(f"Processing {ticker} ({i+1}/{len(stocks_to_analyze)})")
                
                result = self.analyze_stock(ticker)
                if result:
                    results.append(result)
                    self.save_to_database(result)
                    
                    # Check for bubble candidates
                    if result.is_bubble_candidate:
                        bubble_candidates.append(result)
                        logger.warning(f"BUBBLE CANDIDATE: {ticker} - Score: {result.bubble_score:.2f}")
                    
                    # Check for value candidates
                    if result.is_cheap and result.is_quality:
                        value_candidates.append(result)
                        logger.info(f"VALUE CANDIDATE: {ticker} - P/E: {result.pe_ratio:.1f}, ROE: {result.roe:.1f}%")
                
                # Smart delay
                if i < len(stocks_to_analyze) - 1:
                    self.smart_delay()
                    
            except Exception as e:
                logger.error(f"Error analyzing {ticker}: {e}")
        
        # Save results
        self.save_to_excel(results)
        
        # Display results
        self.print_summary(results, bubble_candidates, value_candidates)
        
        return results, bubble_candidates, value_candidates
    
    def print_summary(self, results: List[ValuationSummary], bubble_candidates: List[ValuationSummary], value_candidates: List[ValuationSummary]):
        """Print comprehensive analysis summary"""
        print("\n" + "="*120)
        print("COMPREHENSIVE STOCK VALUATION ANALYSIS WITH BUBBLE DETECTION")
        print("="*120)
        
        print(f"\nAnalysis Summary:")
        print(f"Total Stocks Analyzed: {len(results)}")
        print(f"Bubble Candidates: {len(bubble_candidates)}")
        print(f"Value Candidates: {len(value_candidates)}")
        
        # Bubble candidates
        if bubble_candidates:
            print(f"\nBUBBLE CANDIDATES (Score >= 0.5):")
            print("-" * 80)
            bubble_candidates.sort(key=lambda x: x.bubble_score, reverse=True)
            for candidate in bubble_candidates[:10]:
                print(f"• {candidate.ticker} ({candidate.company_name}) - Score: {candidate.bubble_score:.2f}")
                print(f"  P/E: {candidate.pe_ratio:.1f} | P/B: {candidate.pb_ratio:.1f} | P/S: {candidate.ps_ratio:.1f}")
                print(f"  Warnings: {'; '.join(candidate.bubble_warnings[:3])}")
                print()
        
        # Value candidates
        if value_candidates:
            print(f"\nVALUE CANDIDATES (Cheap + Quality):")
            print("-" * 80)
            value_candidates.sort(key=lambda x: x.margin_of_safety, reverse=True)
            for candidate in value_candidates[:10]:
                print(f"• {candidate.ticker} ({candidate.company_name}) - MoS: {candidate.margin_of_safety:.1f}%")
                print(f"  P/E: {candidate.pe_ratio:.1f} | ROE: {candidate.roe:.1f}% | FCF Yield: {candidate.fcf_yield:.1f}%")
                print(f"  Confidence: {candidate.confidence:.2f}")
                print()
        
        # Top opportunities
        opportunities = [r for r in results if r.is_cheap and r.is_quality and r.margin_of_safety > 10]
        if opportunities:
            print(f"\nTOP OPPORTUNITIES (Cheap + Quality + MoS > 10%):")
            print("-" * 80)
            opportunities.sort(key=lambda x: x.margin_of_safety, reverse=True)
            for opp in opportunities[:5]:
                print(f"• {opp.ticker} ({opp.company_name}) - {opp.margin_of_safety:.1f}% MoS")
                print(f"  P/E: {opp.pe_ratio:.1f} | ROE: {opp.roe:.1f}% | Confidence: {opp.confidence:.2f}")
                print()
        
        print(f"\nResults saved to: {self.primary_dataset_path}")
        print(f"Database: {self.db_path}")

def main():
    """Main function"""
    analyzer = ComprehensiveStockAnalyzer()
    
    print("="*120)
    print("COMPREHENSIVE STOCK VALUATION SCRAPER")
    print("="*120)
    print(f"Stock Universe: {len(analyzer.stock_universe)} stocks")
    print(f"Exchanges: US, ASX, NZX")
    print(f"Features: Valuation analysis, bubble detection, quality assessment")
    print("="*120)
    
    # Run analysis (limit to 50 stocks for demo, remove limit for full analysis)
    results, bubble_candidates, value_candidates = analyzer.run_analysis(max_stocks=50)
    
    print(f"\nAnalysis complete!")
    print(f"Check the Excel file and database for detailed results.")

if __name__ == "__main__":
    main()