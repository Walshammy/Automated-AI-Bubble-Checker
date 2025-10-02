#!/usr/bin/env python3
"""
Database Additions Test Script
Tests collection of additional data types for serious analysis
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
class QuarterlyFinancial:
    """Quarterly financial data point"""
    ticker: str
    quarter_date: date
    revenue: float
    net_income: float
    eps: float
    total_assets: float
    total_debt: float
    shareholders_equity: float
    operating_cashflow: float
    free_cashflow: float
    shares_outstanding: float
    # Calculated fields
    roe: float
    debt_to_equity: float
    fcf_per_share: float
    created_at: datetime

@dataclass
class CorporateAction:
    """Corporate action data"""
    ticker: str
    action_date: date
    action_type: str  # split, dividend, buyback, merger
    value: float
    description: str
    created_at: datetime

@dataclass
class MarketData:
    """Market benchmark data"""
    date: date
    index_name: str
    value: float
    created_at: datetime

class DataAdditionTester:
    """Test collection of additional data types"""
    
    def __init__(self, db_path: str = "db_additions_test/test_data.db"):
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Database setup
        self.db_path = db_path
        self.init_database()
        
        # Test tickers (mix of US, ASX, NZX)
        self.test_tickers = [
            'AAPL', 'MSFT', 'GOOGL',  # US tech
            'BHP.AX', 'CBA.AX', 'WBC.AX',  # ASX major
            'AIR.NZ', 'ANZ.NZ', 'FPH.NZ'  # NZX major
        ]
        
    def init_database(self):
        """Initialize test database with additional tables"""
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Quarterly financials table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS quarterly_financials (
                    ticker TEXT,
                    quarter_date DATE,
                    revenue REAL,
                    net_income REAL,
                    eps REAL,
                    total_assets REAL,
                    total_debt REAL,
                    shareholders_equity REAL,
                    operating_cashflow REAL,
                    free_cashflow REAL,
                    shares_outstanding REAL,
                    roe REAL,
                    debt_to_equity REAL,
                    fcf_per_share REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (ticker, quarter_date)
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
            
            # Market data table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS market_data (
                    date DATE,
                    index_name TEXT,
                    value REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (date, index_name)
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
            self.logger.info(f"Test database initialized: {self.db_path}")
            
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise
    
    def test_quarterly_financials(self, ticker: str) -> Tuple[bool, int, str]:
        """Test collection of quarterly financial data"""
        try:
            self.logger.info(f"Testing quarterly financials for {ticker}")
            
            stock = yf.Ticker(ticker)
            
            # Get quarterly data
            income_stmt = stock.quarterly_income_stmt
            balance_sheet = stock.quarterly_balance_sheet
            cashflow = stock.quarterly_cashflow
            
            records_collected = 0
            
            if income_stmt is not None and not income_stmt.empty:
                # Process quarterly income statements
                for quarter_date in income_stmt.columns:
                    try:
                        # Extract key metrics
                        revenue = self.safe_get(income_stmt, 'Total Revenue', quarter_date)
                        net_income = self.safe_get(income_stmt, 'Net Income', quarter_date)
                        eps = self.safe_get(income_stmt, 'Basic EPS', quarter_date)
                        
                        # Get balance sheet data
                        total_assets = self.safe_get(balance_sheet, 'Total Assets', quarter_date) if balance_sheet is not None else 0
                        total_debt = self.safe_get(balance_sheet, 'Total Debt', quarter_date) if balance_sheet is not None else 0
                        shareholders_equity = self.safe_get(balance_sheet, 'Stockholders Equity', quarter_date) if balance_sheet is not None else 0
                        
                        # Get cash flow data
                        operating_cf = self.safe_get(cashflow, 'Operating Cash Flow', quarter_date) if cashflow is not None else 0
                        free_cf = self.safe_get(cashflow, 'Free Cash Flow', quarter_date) if cashflow is not None else 0
                        
                        # Get shares outstanding
                        shares_outstanding = self.safe_get(balance_sheet, 'Ordinary Shares Number', quarter_date) if balance_sheet is not None else 0
                        
                        # Calculate derived metrics
                        roe = (net_income / shareholders_equity * 100) if shareholders_equity > 0 else 0
                        debt_to_equity = (total_debt / shareholders_equity) if shareholders_equity > 0 else 0
                        fcf_per_share = (free_cf / shares_outstanding) if shares_outstanding > 0 else 0
                        
                        # Create quarterly financial record
                        qf = QuarterlyFinancial(
                            ticker=ticker,
                            quarter_date=quarter_date.date(),
                            revenue=revenue,
                            net_income=net_income,
                            eps=eps,
                            total_assets=total_assets,
                            total_debt=total_debt,
                            shareholders_equity=shareholders_equity,
                            operating_cashflow=operating_cf,
                            free_cashflow=free_cf,
                            shares_outstanding=shares_outstanding,
                            roe=roe,
                            debt_to_equity=debt_to_equity,
                            fcf_per_share=fcf_per_share,
                            created_at=datetime.now()
                        )
                        
                        # Save to database
                        self.save_quarterly_financial(qf)
                        records_collected += 1
                        
                    except Exception as e:
                        self.logger.warning(f"Error processing quarter {quarter_date} for {ticker}: {e}")
                        continue
            
            return True, records_collected, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting quarterly financials for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            return False, 0, error_msg
    
    def test_corporate_actions(self, ticker: str) -> Tuple[bool, int, str]:
        """Test collection of corporate actions"""
        try:
            self.logger.info(f"Testing corporate actions for {ticker}")
            
            stock = yf.Ticker(ticker)
            info = stock.info
            
            records_collected = 0
            
            # Test dividend data
            try:
                dividend_data = stock.dividends
                if dividend_data is not None and not dividend_data.empty:
                    for date_idx, dividend in dividend_data.items():
                        ca = CorporateAction(
                            ticker=ticker,
                            action_date=date_idx.date(),
                            action_type='dividend',
                            value=float(dividend),
                            description=f"Dividend payment: ${dividend:.4f}",
                            created_at=datetime.now()
                        )
                        self.save_corporate_action(ca)
                        records_collected += 1
            except Exception as e:
                self.logger.warning(f"Error getting dividend data for {ticker}: {e}")
            
            # Test stock splits
            try:
                splits_data = stock.splits
                if splits_data is not None and not splits_data.empty:
                    for date_idx, split_ratio in splits_data.items():
                        ca = CorporateAction(
                            ticker=ticker,
                            action_date=date_idx.date(),
                            action_type='split',
                            value=float(split_ratio),
                            description=f"Stock split: {split_ratio}:1",
                            created_at=datetime.now()
                        )
                        self.save_corporate_action(ca)
                        records_collected += 1
            except Exception as e:
                self.logger.warning(f"Error getting splits data for {ticker}: {e}")
            
            return True, records_collected, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting corporate actions for {ticker}: {str(e)}"
            self.logger.error(error_msg)
            return False, 0, error_msg
    
    def test_market_data(self) -> Tuple[bool, int, str]:
        """Test collection of market benchmark data"""
        try:
            self.logger.info("Testing market data collection")
            
            records_collected = 0
            
            # Test major indices
            indices = {
                '^GSPC': 'S&P 500',
                '^AXJO': 'ASX 200',
                '^NZ50': 'NZX 50',
                '^IXIC': 'NASDAQ'
            }
            
            for ticker, name in indices.items():
                try:
                    index_ticker = yf.Ticker(ticker)
                    hist_data = index_ticker.history(period="1y", interval="1mo")
                    
                    if hist_data is not None and not hist_data.empty:
                        for date_idx, row in hist_data.iterrows():
                            md = MarketData(
                                date=date_idx.date(),
                                index_name=name,
                                value=float(row['Close']),
                                created_at=datetime.now()
                            )
                            self.save_market_data(md)
                            records_collected += 1
                            
                except Exception as e:
                    self.logger.warning(f"Error getting {name} data: {e}")
                    continue
            
            return True, records_collected, "Success"
            
        except Exception as e:
            error_msg = f"Error collecting market data: {str(e)}"
            self.logger.error(error_msg)
            return False, 0, error_msg
    
    def safe_get(self, df: pd.DataFrame, column: str, index) -> float:
        """Safely extract values from DataFrame"""
        try:
            if column in df.index:
                value = df.loc[column, index]
                if pd.isna(value):
                    return 0.0
                return float(value)
            return 0.0
        except:
            return 0.0
    
    def save_quarterly_financial(self, qf: QuarterlyFinancial):
        """Save quarterly financial to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO quarterly_financials (
                ticker, quarter_date, revenue, net_income, eps, total_assets,
                total_debt, shareholders_equity, operating_cashflow, free_cashflow,
                shares_outstanding, roe, debt_to_equity, fcf_per_share
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            qf.ticker, qf.quarter_date, qf.revenue, qf.net_income, qf.eps,
            qf.total_assets, qf.total_debt, qf.shareholders_equity,
            qf.operating_cashflow, qf.free_cashflow, qf.shares_outstanding,
            qf.roe, qf.debt_to_equity, qf.fcf_per_share
        ))
        
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
    
    def save_market_data(self, md: MarketData):
        """Save market data to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO market_data (
                date, index_name, value
            ) VALUES (?, ?, ?)
        ''', (md.date, md.index_name, md.value))
        
        conn.commit()
        conn.close()
    
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
        """Run comprehensive test of all data types"""
        print("=" * 80)
        print("DATABASE ADDITIONS COMPREHENSIVE TEST")
        print("=" * 80)
        
        total_tests = 0
        successful_tests = 0
        total_records = 0
        
        # Test quarterly financials
        print(f"\n1. TESTING QUARTERLY FINANCIALS")
        print("-" * 50)
        for ticker in self.test_tickers:
            success, records, error = self.test_quarterly_financials(ticker)
            self.save_test_result(ticker, 'quarterly_financials', success, records, error)
            
            total_tests += 1
            if success:
                successful_tests += 1
                total_records += records
                print(f"+ {ticker}: {records} quarterly records")
            else:
                print(f"- {ticker}: {error}")
            
            time.sleep(1)  # Rate limiting
        
        # Test corporate actions
        print(f"\n2. TESTING CORPORATE ACTIONS")
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
        
        # Test market data
        print(f"\n3. TESTING MARKET DATA")
        print("-" * 50)
        success, records, error = self.test_market_data()
        self.save_test_result('MARKET', 'market_data', success, records, error)
        
        total_tests += 1
        if success:
            successful_tests += 1
            total_records += records
            print(f"+ Market data: {records} records")
        else:
            print(f"- Market data: {error}")
        
        # Summary
        print(f"\n" + "=" * 80)
        print(f"TEST SUMMARY")
        print(f"=" * 80)
        print(f"Total tests: {total_tests}")
        print(f"Successful: {successful_tests}")
        print(f"Success rate: {(successful_tests/total_tests)*100:.1f}%")
        print(f"Total records collected: {total_records:,}")
        
        # Database analysis
        self.analyze_test_database()
    
    def analyze_test_database(self):
        """Analyze the test database results"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print(f"\nDATABASE ANALYSIS")
            print("-" * 50)
            
            # Quarterly financials
            cursor.execute('SELECT COUNT(*) FROM quarterly_financials')
            qf_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM quarterly_financials')
            qf_tickers = cursor.fetchone()[0]
            
            # Corporate actions
            cursor.execute('SELECT COUNT(*) FROM corporate_actions')
            ca_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM corporate_actions')
            ca_tickers = cursor.fetchone()[0]
            
            # Market data
            cursor.execute('SELECT COUNT(*) FROM market_data')
            md_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(DISTINCT index_name) FROM market_data')
            md_indices = cursor.fetchone()[0]
            
            print(f"Quarterly Financials: {qf_count:,} records ({qf_tickers} tickers)")
            print(f"Corporate Actions: {ca_count:,} records ({ca_tickers} tickers)")
            print(f"Market Data: {md_count:,} records ({md_indices} indices)")
            
            # Coverage analysis
            print(f"\nCOVERAGE ANALYSIS")
            print("-" * 50)
            
            cursor.execute('''
                SELECT ticker, COUNT(*) as quarters
                FROM quarterly_financials 
                GROUP BY ticker 
                ORDER BY quarters DESC
            ''')
            coverage = cursor.fetchall()
            
            for ticker, quarters in coverage:
                print(f"{ticker}: {quarters} quarters")
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error analyzing test database: {e}")

def main():
    """Main function for testing data additions"""
    tester = DataAdditionTester()
    tester.run_comprehensive_test()

if __name__ == "__main__":
    main()
