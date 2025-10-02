#!/usr/bin/env python3
"""
Simple test to debug the database insertion issue
"""

import sqlite3
import os
from datetime import datetime, date

# Remove existing database to start fresh
if os.path.exists("test_comprehensive_data.db"):
    os.remove("test_comprehensive_data.db")

# Create test database
conn = sqlite3.connect("test_comprehensive_data.db")
cursor = conn.cursor()

# Create table
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

# Test data tuple
test_data = (
    "TEST.NZ", date.today(), 100.0, 105.0, 95.0, 102.0, 1000, 102.0, 1000000,
    15.0, 2.0, 1.5, 3.0, 2.0, 10.0, 8.0, 12.0, 0.5, 1.2, 5.0,
    1.0, 5.0, 8.0, 30.0, 20.0, 15.0, 1.0, 25.0, -10.0,
    "Technology", "Software", "NZX", False, None
)

print(f"Test data tuple length: {len(test_data)}")

# Try to insert
try:
    cursor.execute('''
        INSERT OR REPLACE INTO comprehensive_data (
            ticker, date, open_price, high_price, low_price, close_price,
            volume, adjusted_close, market_cap, pe_ratio, pb_ratio, peg_ratio,
            ps_ratio, dividend_yield, roe, roa, roic, debt_to_equity, current_ratio,
            fcf_yield, eps_ttm, eps_growth_5y, revenue_growth_5y, gross_margin,
            operating_margin, net_margin, beta, volatility_1y, max_drawdown_5y,
            sector, industry, exchange, is_delisted, delisted_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', test_data)
    
    print("SUCCESS: Data inserted successfully!")
    
except Exception as e:
    print(f"ERROR: {e}")

conn.commit()
conn.close()

# Clean up
os.remove("test_comprehensive_data.db")
