import pandas as pd
import numpy as np
import yfinance as yf
import sqlite3
import time
import os
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

class ComprehensiveHistoricalCollector:
    """Comprehensive historical data collector for 2000-2025"""
    
    def __init__(self, db_path: str = "comprehensive_historical_data.db"):
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Database setup
        self.db_path = db_path
        self.init_database()
        
        # Comprehensive stock universe (including delisted stocks)
        self.stock_universe = {
            # NZX Stocks (Current + Historical)
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
            
            # International Stocks (Current + Historical)
            'BRK-B': 'Berkshire Hathaway Class B',
            'BRK-A': 'Berkshire Hathaway Class A',
            'MSFT': 'Microsoft Corporation',
            'AAPL': 'Apple Inc.',
            'GOOGL': 'Alphabet Inc.',
            'GOOG': 'Alphabet Inc. Class A',
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
            'AMZN': 'Amazon.com Inc.',
            'TSLA': 'Tesla Inc.',
            'NVDA': 'NVIDIA Corporation',
            'META': 'Meta Platforms Inc.',
            'NFLX': 'Netflix Inc.',
            'CRM': 'Salesforce Inc.',
            
            # Historical/Delisted Stocks (if available)
            'LEH': 'Lehman Brothers Holdings Inc.',  # Delisted 2008
            'AIG': 'American International Group Inc.',
            'GM': 'General Motors Company',
            'F': 'Ford Motor Company',
            'GE': 'General Electric Company',
            'T': 'AT&T Inc.',
            'VZ': 'Verizon Communications Inc.',
            'DIS': 'The Walt Disney Company',
            'NKE': 'Nike Inc.',
            'MCD': 'McDonald\'s Corporation',
            'BA': 'The Boeing Company',
            'CAT': 'Caterpillar Inc.',
            'MMM': '3M Company',
            'UTX': 'United Technologies Corporation',  # Now RTX
            'RTX': 'Raytheon Technologies Corporation',
        }
        
        # Collection settings
        self.start_date = date(2000, 1, 1)
        self.end_date = date.today()
        self.max_workers = 5  # Concurrent downloads
        self.rate_limit_delay = 0.5  # seconds between API calls
        
        # Progress tracking
        self.progress_file = "historical_collection_progress.json"
        self.load_progress()
        
    def init_database(self):
        """Initialize optimized SQLite database for 25+ years of data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create main historical data table (optimized for time-series)
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
            
            # Create optimized indexes for time-series queries
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker_date ON historical_data(ticker, date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON historical_data(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticker ON historical_data(ticker)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_delisted ON historical_data(is_delisted)')
            
            # Create collection progress table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS collection_progress (
                    ticker TEXT PRIMARY KEY,
                    last_collected_date DATE,
                    total_records INTEGER DEFAULT 0,
                    is_complete BOOLEAN DEFAULT 0,
                    is_delisted BOOLEAN DEFAULT 0,
                    delisted_date DATE,
                    last_error TEXT,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create collection log table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS collection_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collection_date DATETIME NOT NULL,
                    tickers_processed INTEGER,
                    records_added INTEGER,
                    errors_count INTEGER,
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
    
    def load_progress(self):
        """Load collection progress from file"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    self.progress = json.load(f)
            else:
                self.progress = {
                    'completed_tickers': [],
                    'failed_tickers': [],
                    'last_update': None,
                    'total_records': 0
                }
        except Exception as e:
            self.logger.error(f"Error loading progress: {e}")
            self.progress = {
                'completed_tickers': [],
                'failed_tickers': [],
                'last_update': None,
                'total_records': 0
            }
    
    def save_progress(self):
        """Save collection progress to file"""
        try:
            self.progress['last_update'] = datetime.now().isoformat()
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving progress: {e}")
    
    def get_ticker_info(self, ticker: str) -> Dict:
        """Get comprehensive ticker information"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Check if stock is delisted
            is_delisted = False
            delisted_date = None
            
            if 'delistedDate' in info and info['delistedDate']:
                is_delisted = True
                delisted_date = pd.to_datetime(info['delistedDate']).date()
            elif 'regularMarketPrice' not in info or info.get('regularMarketPrice') is None:
                # Try to determine if delisted by checking if we can get recent data
                try:
                    recent_data = stock.history(period="1mo")
                    if recent_data.empty:
                        is_delisted = True
                except:
                    is_delisted = True
            
            return {
                'ticker': ticker,
                'company_name': self.stock_universe.get(ticker, ticker),
                'sector': info.get('sector', 'Unknown'),
                'industry': info.get('industry', 'Unknown'),
                'is_delisted': is_delisted,
                'delisted_date': delisted_date,
                'market_cap': info.get('marketCap', 0),
                'beta': info.get('beta', 1.0)
            }
            
        except Exception as e:
            self.logger.warning(f"Could not get info for {ticker}: {e}")
            return {
                'ticker': ticker,
                'company_name': self.stock_universe.get(ticker, ticker),
                'sector': 'Unknown',
                'industry': 'Unknown',
                'is_delisted': True,  # Assume delisted if we can't get info
                'delisted_date': None,
                'market_cap': 0,
                'beta': 1.0
            }
    
    def collect_historical_data(self, ticker: str) -> Tuple[List[HistoricalDataPoint], bool]:
        """Collect historical data for a single ticker"""
        try:
            self.logger.info(f"Collecting historical data for {ticker}")
            
            # Get ticker info
            ticker_info = self.get_ticker_info(ticker)
            
            # Get historical price data
            stock = yf.Ticker(ticker)
            
            # Try different periods to get maximum historical data
            historical_data = None
            
            # First try: Get all available data
            try:
                historical_data = stock.history(start=self.start_date, end=self.end_date, interval="1wk")
            except:
                # Fallback: Try with shorter periods
                try:
                    historical_data = stock.history(period="max", interval="1wk")
                except:
                    # Last resort: Try daily data and resample
                    try:
                        daily_data = stock.history(period="max")
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
                    except:
                        pass
            
            if historical_data is None or historical_data.empty:
                self.logger.warning(f"No historical data available for {ticker}")
                return [], ticker_info['is_delisted']
            
            # Convert to our data points
            data_points = []
            
            for date_idx, row in historical_data.iterrows():
                # Get fundamental data for this period (simplified - using most recent available)
                try:
                    # For efficiency, we'll use the most recent fundamental data
                    # In a more sophisticated system, you'd want to get historical fundamentals
                    stock_info = stock.info
                    
                    data_point = HistoricalDataPoint(
                        ticker=ticker,
                        date=date_idx.date(),
                        open_price=float(row['Open']) if not pd.isna(row['Open']) else 0,
                        high_price=float(row['High']) if not pd.isna(row['High']) else 0,
                        low_price=float(row['Low']) if not pd.isna(row['Low']) else 0,
                        close_price=float(row['Close']) if not pd.isna(row['Close']) else 0,
                        volume=int(row['Volume']) if not pd.isna(row['Volume']) else 0,
                        adjusted_close=float(row['Adj Close']) if not pd.isna(row['Adj Close']) else 0,
                        market_cap=ticker_info['market_cap'],
                        pe_ratio=self.safe_get(stock_info, 'trailingPE'),
                        pb_ratio=self.safe_get(stock_info, 'priceToBook'),
                        peg_ratio=self.safe_get(stock_info, 'pegRatio'),
                        dividend_yield=self.safe_get(stock_info, 'dividendYield', 0) * 100,
                        roe=self.safe_get(stock_info, 'returnOnEquity', 0) * 100,
                        debt_to_equity=self.safe_get(stock_info, 'debtToEquity'),
                        current_ratio=self.safe_get(stock_info, 'currentRatio'),
                        fcf_yield=self.calculate_fcf_yield(stock_info),
                        eps_ttm=self.safe_get(stock_info, 'trailingEps'),
                        eps_growth_5y=self.safe_get(stock_info, 'earningsGrowth', 0) * 100,
                        revenue_growth_5y=self.safe_get(stock_info, 'revenueGrowth', 0) * 100,
                        roa=self.safe_get(stock_info, 'returnOnAssets', 0) * 100,
                        roic=self.safe_get(stock_info, 'returnOnInvestedCapital', 0) * 100,
                        gross_margin=self.safe_get(stock_info, 'grossMargins', 0) * 100,
                        operating_margin=self.safe_get(stock_info, 'operatingMargins', 0) * 100,
                        net_margin=self.safe_get(stock_info, 'profitMargins', 0) * 100,
                        beta=ticker_info['beta'],
                        sector=ticker_info['sector'],
                        industry=ticker_info['industry'],
                        is_delisted=ticker_info['is_delisted'],
                        delisted_date=ticker_info['delisted_date'],
                        created_at=datetime.now()
                    )
                    
                    data_points.append(data_point)
                    
                except Exception as e:
                    self.logger.warning(f"Error processing data point for {ticker} on {date_idx.date()}: {e}")
                    continue
            
            self.logger.info(f"Collected {len(data_points)} data points for {ticker}")
            return data_points, ticker_info['is_delisted']
            
        except Exception as e:
            self.logger.error(f"Error collecting historical data for {ticker}: {e}")
            return [], True
    
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
    
    def save_data_points(self, data_points: List[HistoricalDataPoint], ticker: str):
        """Save data points to database efficiently"""
        if not data_points:
            return
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
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
                    ticker, last_collected_date, total_records, is_complete, is_delisted, delisted_date
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                ticker, 
                max(dp.date for dp in data_points),
                len(data_points),
                True,
                data_points[0].is_delisted if data_points else False,
                data_points[0].delisted_date if data_points else None
            ))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Saved {len(data_points)} records for {ticker}")
            
        except Exception as e:
            self.logger.error(f"Error saving data points for {ticker}: {e}")
    
    def collect_all_historical_data(self):
        """Collect historical data for all tickers"""
        start_time = time.time()
        total_records = 0
        errors = []
        
        self.logger.info(f"Starting comprehensive historical data collection for {len(self.stock_universe)} tickers")
        self.logger.info(f"Date range: {self.start_date} to {self.end_date}")
        
        # Use ThreadPoolExecutor for concurrent downloads
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_ticker = {
                executor.submit(self.collect_historical_data, ticker): ticker 
                for ticker in self.stock_universe.keys()
            }
            
            # Process completed tasks
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    data_points, is_delisted = future.result()
                    
                    if data_points:
                        self.save_data_points(data_points, ticker)
                        total_records += len(data_points)
                        self.progress['completed_tickers'].append(ticker)
                        
                        if is_delisted:
                            self.logger.info(f"✓ {ticker} (DELISTED) - {len(data_points)} records")
                        else:
                            self.logger.info(f"✓ {ticker} - {len(data_points)} records")
                    else:
                        self.progress['failed_tickers'].append(ticker)
                        errors.append(f"No data for {ticker}")
                        self.logger.warning(f"✗ {ticker} - No data available")
                    
                    # Rate limiting
                    time.sleep(self.rate_limit_delay)
                    
                except Exception as e:
                    error_msg = f"Error processing {ticker}: {str(e)}"
                    errors.append(error_msg)
                    self.progress['failed_tickers'].append(ticker)
                    self.logger.error(error_msg)
        
        total_time = time.time() - start_time
        
        # Log collection statistics
        self.log_collection_stats(len(self.stock_universe), total_records, len(errors), total_time, errors)
        
        # Save progress
        self.save_progress()
        
        self.logger.info(f"Collection complete: {total_records} total records in {total_time:.1f}s")
        self.logger.info(f"Completed: {len(self.progress['completed_tickers'])} tickers")
        self.logger.info(f"Failed: {len(self.progress['failed_tickers'])} tickers")
        
        return total_records, len(errors), total_time
    
    def log_collection_stats(self, tickers_processed: int, records_added: int, 
                           errors_count: int, total_time: float, errors: List[str]):
        """Log collection statistics to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO collection_log (
                    collection_date, tickers_processed, records_added,
                    errors_count, total_time_seconds, errors
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                datetime.now(), tickers_processed, records_added,
                errors_count, total_time, '; '.join(errors[:10]) if errors else ''  # Limit error text
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error logging collection stats: {e}")
    
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
            
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM historical_data WHERE is_delisted = 1')
            delisted_tickers = cursor.fetchone()[0]
            
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
                'delisted_tickers': delisted_tickers,
                'ticker_stats': ticker_stats
            }
            
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {}
    
    def export_to_excel(self, output_file: str = "comprehensive_historical_data.xlsx"):
        """Export historical data to Excel for analysis"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get all data
            df = pd.read_sql_query('''
                SELECT * FROM historical_data 
                ORDER BY ticker, date
            ''', conn)
            
            conn.close()
            
            if df.empty:
                self.logger.warning("No data to export")
                return
            
            # Export to Excel
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Main data sheet
                df.to_excel(writer, sheet_name='Historical Data', index=False)
                
                # Summary sheet
                summary_data = []
                for ticker in df['ticker'].unique():
                    ticker_data = df[df['ticker'] == ticker]
                    summary_data.append({
                        'Ticker': ticker,
                        'Records': len(ticker_data),
                        'First Date': ticker_data['date'].min(),
                        'Last Date': ticker_data['date'].max(),
                        'Is Delisted': ticker_data['is_delisted'].iloc[0],
                        'Sector': ticker_data['sector'].iloc[0],
                        'Industry': ticker_data['industry'].iloc[0]
                    })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            self.logger.info(f"Data exported to {output_file}")
            
        except Exception as e:
            self.logger.error(f"Error exporting to Excel: {e}")

def main():
    """Main function for comprehensive historical data collection"""
    collector = ComprehensiveHistoricalCollector()
    
    print("="*80)
    print("COMPREHENSIVE HISTORICAL DATA COLLECTION")
    print("="*80)
    print(f"Tickers: {len(collector.stock_universe)}")
    print(f"Date Range: {collector.start_date} to {collector.end_date}")
    print(f"Target: Weekly data points (25+ years)")
    print("="*80)
    
    # Start collection
    total_records, errors_count, total_time = collector.collect_all_historical_data()
    
    # Display results
    print(f"\nCOLLECTION RESULTS:")
    print(f"Total Records: {total_records:,}")
    print(f"Errors: {errors_count}")
    print(f"Time: {total_time:.1f} seconds")
    
    # Get database stats
    stats = collector.get_database_stats()
    if stats:
        print(f"\nDATABASE STATISTICS:")
        print(f"Unique Tickers: {stats['unique_tickers']}")
        print(f"Date Range: {stats['date_range'][0]} to {stats['date_range'][1]}")
        print(f"Delisted Records: {stats['delisted_records']:,}")
        print(f"Delisted Tickers: {stats['delisted_tickers']}")
        
        print(f"\nTOP 10 TICKERS BY RECORD COUNT:")
        for i, (ticker, count, first_date, last_date) in enumerate(stats['ticker_stats'][:10]):
            print(f"{i+1:2d}. {ticker:<8} - {count:4d} records ({first_date} to {last_date})")
    
    # Export to Excel
    collector.export_to_excel()
    
    print(f"\nHistorical data collection complete!")
    print(f"Database: {collector.db_path}")
    print(f"Excel export: comprehensive_historical_data.xlsx")

if __name__ == "__main__":
    main()
