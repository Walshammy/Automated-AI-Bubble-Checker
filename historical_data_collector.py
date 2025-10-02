import pandas as pd
import numpy as np
import yfinance as yf
import sqlite3
import time
import os
import schedule
import threading
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import json
import warnings

warnings.filterwarnings('ignore')

@dataclass
class HistoricalDataPoint:
    """Single data point for historical tracking"""
    ticker: str
    timestamp: datetime
    current_price: float
    pe_ratio: float
    pb_ratio: float
    peg_ratio: float
    dividend_yield: float
    roe: float
    debt_to_equity: float
    current_ratio: float
    fcf_yield: float
    market_cap: float
    eps_ttm: float
    eps_growth_5y: float
    revenue_growth_5y: float
    roa: float
    roic: float
    gross_margin: float
    operating_margin: float
    net_margin: float
    beta: float
    volatility_1y: float
    max_drawdown_5y: float
    price_change_1y: float
    price_change_3m: float
    is_quality: bool
    is_cheap: bool
    margin_of_safety: float
    confidence: float
    sector: str
    industry: str
    warnings: str

class HistoricalDataCollector:
    """Comprehensive historical data collection system"""
    
    def __init__(self, db_path: str = "historical_valuation_data.db"):
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Database setup
        self.db_path = db_path
        self.init_database()
        
        # Stock universe (same as main script)
        self.focus_stocks = {
            # NZX Stocks
            'WBC.NZ': 'Westpac Banking Corporation',
            'ANZ.NZ': 'ANZ Group Holdings Limited',
            'FPH.NZ': 'Fisher & Paykel Healthcare Corporation Limited',
            'MEL.NZ': 'Meridian Energy Limited',
            'AIA.NZ': 'Auckland International Airport Limited',
            'IFT.NZ': 'Infratil Limited',
            'AFI.NZ': 'Australian Foundation Investment Company Limited',
            'MCY.NZ': 'Mercury NZ Limited',
            'EBO.NZ': 'EBOS Group Limited',
            'FCG.NZ': 'Fonterra Co-operative Group Limited',
            'CEN.NZ': 'Contact Energy Limited',
            'MFT.NZ': 'Mainfreight Limited',
            'ATM.NZ': 'The a2 Milk Company Limited',
            'POT.NZ': 'Port of Tauranga Limited',
            'SPK.NZ': 'Spark New Zealand Limited',
            'VNT.NZ': 'Ventia Services Group Limited',
            'VCT.NZ': 'Vector Limited',
            'CNU.NZ': 'Chorus Limited',
            'FBU.NZ': 'Fletcher Building Limited',
            'GMT.NZ': 'Goodman Property Trust',
            'SUM.NZ': 'Summerset Group Holdings Limited',
            'GNE.NZ': 'Genesis Energy Limited',
            'RYM.NZ': 'Ryman Healthcare Limited',
            'FRW.NZ': 'Freightways Group Limited',
            'PCT.NZ': 'Precinct Properties NZ Ltd',
            'AIR.NZ': 'Air New Zealand Limited',
            'KPG.NZ': 'Kiwi Property Group Limited',
            'GTK.NZ': 'Gentrack Group Limited',
            
            # International Stocks
            'BRK-B': 'Berkshire Hathaway Class B',
            'MSFT': 'Microsoft Corporation',
            'AAPL': 'Apple Inc.',
            'GOOGL': 'Alphabet Inc.',
            'JNJ': 'Johnson & Johnson',
            'PG': 'Procter & Gamble Company',
            'KO': 'Coca-Cola Company',
            'PEP': 'PepsiCo Inc.',
            'WMT': 'Walmart Inc.',
            'HD': 'Home Depot Inc.',
            'JPM': 'JPMorgan Chase & Co.',
            'BAC': 'Bank of America Corporation',
            'WFC': 'Wells Fargo & Company',
            'CVX': 'Chevron Corporation',
            'XOM': 'Exxon Mobil Corporation',
            'IBM': 'International Business Machines Corporation',
            'INTC': 'Intel Corporation',
            'CSCO': 'Cisco Systems Inc.',
            'ORCL': 'Oracle Corporation',
            'ADBE': 'Adobe Inc.',
        }
        
        # Collection settings
        self.collection_interval_hours = 6  # Collect every 6 hours
        self.max_retries = 3
        self.rate_limit_delay = 1  # seconds between API calls
        
    def init_database(self):
        """Initialize SQLite database with proper schema"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create main historical data table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS historical_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    current_price REAL,
                    pe_ratio REAL,
                    pb_ratio REAL,
                    peg_ratio REAL,
                    dividend_yield REAL,
                    roe REAL,
                    debt_to_equity REAL,
                    current_ratio REAL,
                    fcf_yield REAL,
                    market_cap REAL,
                    eps_ttm REAL,
                    eps_growth_5y REAL,
                    revenue_growth_5y REAL,
                    roa REAL,
                    roic REAL,
                    gross_margin REAL,
                    operating_margin REAL,
                    net_margin REAL,
                    beta REAL,
                    volatility_1y REAL,
                    max_drawdown_5y REAL,
                    price_change_1y REAL,
                    price_change_3m REAL,
                    is_quality BOOLEAN,
                    is_cheap BOOLEAN,
                    margin_of_safety REAL,
                    confidence REAL,
                    sector TEXT,
                    industry TEXT,
                    warnings TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, timestamp)
                )
            ''')
            
            # Create index for faster queries
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker_timestamp ON historical_data(ticker, timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON historical_data(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON historical_data(ticker)')
            
            # Create collection log table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS collection_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collection_date DATETIME NOT NULL,
                    stocks_collected INTEGER,
                    stocks_failed INTEGER,
                    total_time_seconds REAL,
                    errors TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            self.logger.info(f"Database initialized: {self.db_path}")
            
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise
    
    def get_sector(self, ticker: str) -> str:
        """Determine sector based on ticker"""
        sector_map = {
            'FPH.NZ': 'Healthcare',
            'MEL.NZ': 'Utilities',
            'AIA.NZ': 'Industrial',
            'IFT.NZ': 'Industrial',
            'MFT.NZ': 'Industrial',
            'ATM.NZ': 'Consumer',
            'POT.NZ': 'Industrial',
            'SPK.NZ': 'Communication',
            'VCT.NZ': 'Utilities',
            'CNU.NZ': 'Communication',
            'WBC.NZ': 'Financial',
            'ANZ.NZ': 'Financial',
            'AFI.NZ': 'Financial',
            'BRK-B': 'Financial',
            'MSFT': 'Technology',
            'AAPL': 'Technology',
            'GOOGL': 'Technology',
            'JNJ': 'Healthcare',
            'PG': 'Consumer',
            'KO': 'Consumer',
            'PEP': 'Consumer',
            'WMT': 'Consumer',
            'HD': 'Consumer',
            'JPM': 'Financial',
            'BAC': 'Financial',
            'WFC': 'Financial',
            'CVX': 'Energy',
            'XOM': 'Energy',
            'IBM': 'Technology',
            'INTC': 'Technology',
            'CSCO': 'Technology',
            'ORCL': 'Technology',
            'ADBE': 'Technology',
        }
        return sector_map.get(ticker, 'Unknown')
    
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
                self.logger.warning(f"Could not get historical data for {ticker}: {e}")
            
            # Extract comprehensive metrics
            data = {
                'ticker': ticker,
                'company_name': self.focus_stocks.get(ticker, ticker),
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
                'sector': self.get_sector(ticker),
                'industry': info.get('industry', 'Unknown'),
                'timestamp': datetime.now(),
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
            self.logger.error(f"Error getting data for {ticker}: {e}")
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
    
    def assess_quality(self, data: Dict) -> Tuple[bool, float, List[str]]:
        """Assess business quality with strict criteria"""
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
    
    def assess_value(self, data: Dict) -> Tuple[bool, float, List[str]]:
        """Assess valuation attractiveness with strict value criteria"""
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
    
    def collect_stock_data(self, ticker: str) -> Optional[HistoricalDataPoint]:
        """Collect comprehensive data for a single stock"""
        try:
            # Get raw data
            data = self.get_stock_data(ticker)
            if not data or data.get('current_price', 0) <= 0:
                return None
            
            # Assess quality and value
            is_quality, quality_score, quality_warnings = self.assess_quality(data)
            is_cheap, value_score, value_warnings = self.assess_value(data)
            
            # Calculate margin of safety
            margin_of_safety = self.calculate_margin_of_safety(data)
            
            # Calculate confidence
            confidence = (quality_score + value_score) / 2
            
            # Combine warnings
            all_warnings = quality_warnings + value_warnings
            
            # Add data quality warnings
            if data.get('eps_ttm', 0) <= 0:
                all_warnings.append("Negative or zero EPS")
            if data.get('fcf_ttm', 0) <= 0:
                all_warnings.append("Negative FCF")
            if data.get('revenue_ttm', 0) <= 0:
                all_warnings.append("Negative revenue")
            
            return HistoricalDataPoint(
                ticker=ticker,
                timestamp=data['timestamp'],
                current_price=data.get('current_price', 0),
                pe_ratio=data.get('pe_ratio', 0),
                pb_ratio=data.get('pb_ratio', 0),
                peg_ratio=data.get('peg_ratio', 0),
                dividend_yield=data.get('dividend_yield', 0),
                roe=data.get('roe', 0),
                debt_to_equity=data.get('debt_to_equity', 0),
                current_ratio=data.get('current_ratio', 0),
                fcf_yield=self.calculate_fcf_yield(data),
                market_cap=data.get('market_cap', 0),
                eps_ttm=data.get('eps_ttm', 0),
                eps_growth_5y=data.get('eps_growth_5y', 0),
                revenue_growth_5y=data.get('revenue_growth_5y', 0),
                roa=data.get('roa', 0),
                roic=data.get('roic', 0),
                gross_margin=data.get('gross_margin', 0),
                operating_margin=data.get('operating_margin', 0),
                net_margin=data.get('net_margin', 0),
                beta=data.get('beta', 1.0),
                volatility_1y=data.get('volatility_1y', 0),
                max_drawdown_5y=data.get('max_drawdown_5y', 0),
                price_change_1y=data.get('price_change_1y', 0),
                price_change_3m=data.get('price_change_3m', 0),
                is_quality=is_quality,
                is_cheap=is_cheap,
                margin_of_safety=margin_of_safety,
                confidence=confidence,
                sector=data.get('sector', 'Unknown'),
                industry=data.get('industry', 'Unknown'),
                warnings='; '.join(all_warnings) if all_warnings else ''
            )
            
        except Exception as e:
            self.logger.error(f"Error collecting data for {ticker}: {e}")
            return None
    
    def save_data_point(self, data_point: HistoricalDataPoint):
        """Save a single data point to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO historical_data (
                    ticker, timestamp, current_price, pe_ratio, pb_ratio, peg_ratio,
                    dividend_yield, roe, debt_to_equity, current_ratio, fcf_yield,
                    market_cap, eps_ttm, eps_growth_5y, revenue_growth_5y, roa, roic,
                    gross_margin, operating_margin, net_margin, beta, volatility_1y,
                    max_drawdown_5y, price_change_1y, price_change_3m, is_quality,
                    is_cheap, margin_of_safety, confidence, sector, industry, warnings
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data_point.ticker, data_point.timestamp, data_point.current_price,
                data_point.pe_ratio, data_point.pb_ratio, data_point.peg_ratio,
                data_point.dividend_yield, data_point.roe, data_point.debt_to_equity,
                data_point.current_ratio, data_point.fcf_yield, data_point.market_cap,
                data_point.eps_ttm, data_point.eps_growth_5y, data_point.revenue_growth_5y,
                data_point.roa, data_point.roic, data_point.gross_margin,
                data_point.operating_margin, data_point.net_margin, data_point.beta,
                data_point.volatility_1y, data_point.max_drawdown_5y,
                data_point.price_change_1y, data_point.price_change_3m,
                data_point.is_quality, data_point.is_cheap, data_point.margin_of_safety,
                data_point.confidence, data_point.sector, data_point.industry,
                data_point.warnings
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error saving data point for {data_point.ticker}: {e}")
    
    def log_collection(self, stocks_collected: int, stocks_failed: int, 
                      total_time: float, errors: List[str]):
        """Log collection statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO collection_log (
                    collection_date, stocks_collected, stocks_failed,
                    total_time_seconds, errors
                ) VALUES (?, ?, ?, ?, ?)
            ''', (
                datetime.now(), stocks_collected, stocks_failed,
                total_time, '; '.join(errors) if errors else ''
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error logging collection: {e}")
    
    def collect_all_data(self):
        """Collect data for all stocks in universe"""
        start_time = time.time()
        stocks_collected = 0
        stocks_failed = 0
        errors = []
        
        self.logger.info(f"Starting historical data collection for {len(self.focus_stocks)} stocks")
        
        for ticker in self.focus_stocks.keys():
            try:
                data_point = self.collect_stock_data(ticker)
                if data_point:
                    self.save_data_point(data_point)
                    stocks_collected += 1
                    self.logger.info(f"Collected data for {ticker}")
                else:
                    stocks_failed += 1
                    errors.append(f"Failed to collect data for {ticker}")
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
                
            except Exception as e:
                stocks_failed += 1
                error_msg = f"Error collecting {ticker}: {str(e)}"
                errors.append(error_msg)
                self.logger.error(error_msg)
        
        total_time = time.time() - start_time
        
        # Log collection statistics
        self.log_collection(stocks_collected, stocks_failed, total_time, errors)
        
        self.logger.info(f"Collection complete: {stocks_collected} collected, {stocks_failed} failed, {total_time:.1f}s")
        
        return stocks_collected, stocks_failed, total_time, errors
    
    def get_historical_data(self, ticker: str = None, days: int = 30) -> pd.DataFrame:
        """Retrieve historical data from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            if ticker:
                query = '''
                    SELECT * FROM historical_data 
                    WHERE ticker = ? AND timestamp >= datetime('now', '-{} days')
                    ORDER BY timestamp DESC
                '''.format(days)
                df = pd.read_sql_query(query, conn, params=(ticker,))
            else:
                query = '''
                    SELECT * FROM historical_data 
                    WHERE timestamp >= datetime('now', '-{} days')
                    ORDER BY timestamp DESC
                '''.format(days)
                df = pd.read_sql_query(query, conn)
            
            conn.close()
            return df
            
        except Exception as e:
            self.logger.error(f"Error retrieving historical data: {e}")
            return pd.DataFrame()
    
    def start_scheduled_collection(self):
        """Start automated scheduled data collection"""
        self.logger.info("Starting scheduled data collection")
        
        # Schedule collection every 6 hours
        schedule.every(self.collection_interval_hours).hours.do(self.collect_all_data)
        
        # Run initial collection
        self.collect_all_data()
        
        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def run_single_collection(self):
        """Run a single data collection cycle"""
        self.logger.info("Running single data collection cycle")
        return self.collect_all_data()

def main():
    """Main function for testing"""
    collector = HistoricalDataCollector()
    
    # Run single collection
    stocks_collected, stocks_failed, total_time, errors = collector.run_single_collection()
    
    print(f"\nCollection Results:")
    print(f"Stocks collected: {stocks_collected}")
    print(f"Stocks failed: {stocks_failed}")
    print(f"Total time: {total_time:.1f} seconds")
    
    if errors:
        print(f"Errors: {len(errors)}")
        for error in errors[:5]:  # Show first 5 errors
            print(f"  - {error}")
    
    # Show sample of collected data
    print(f"\nSample of collected data:")
    sample_data = collector.get_historical_data(days=1)
    if not sample_data.empty:
        print(sample_data[['ticker', 'timestamp', 'current_price', 'pe_ratio', 'margin_of_safety']].head())

if __name__ == "__main__":
    main()
