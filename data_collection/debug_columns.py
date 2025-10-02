#!/usr/bin/env python3
"""
Debug script to check column count mismatch
"""

# Count the columns in the INSERT statement
insert_columns = [
    'ticker', 'date', 'open_price', 'high_price', 'low_price', 'close_price',
    'volume', 'adjusted_close', 'market_cap', 'pe_ratio', 'pb_ratio', 'peg_ratio',
    'ps_ratio', 'dividend_yield', 'roe', 'roa', 'roic', 'debt_to_equity', 'current_ratio',
    'fcf_yield', 'eps_ttm', 'eps_growth_5y', 'revenue_growth_5y', 'gross_margin',
    'operating_margin', 'net_margin', 'beta', 'volatility_1y', 'max_drawdown_5y',
    'sector', 'industry', 'exchange', 'is_delisted', 'delisted_date'
]

# Count the values in the data tuple
data_values = [
    'dp.ticker', 'dp.date', 'dp.open_price', 'dp.high_price', 'dp.low_price',
    'dp.close_price', 'dp.volume', 'dp.adjusted_close', 'dp.market_cap',
    'dp.pe_ratio', 'dp.pb_ratio', 'dp.peg_ratio', 'dp.ps_ratio', 'dp.dividend_yield',
    'dp.roe', 'dp.roa', 'dp.roic', 'dp.debt_to_equity', 'dp.current_ratio', 'dp.fcf_yield',
    'dp.eps_ttm', 'dp.eps_growth_5y', 'dp.revenue_growth_5y', 'dp.gross_margin',
    'dp.operating_margin', 'dp.net_margin', 'dp.beta', 'dp.volatility_1y', 'dp.max_drawdown_5y',
    'dp.sector', 'dp.industry', 'dp.exchange', 'dp.is_delisted', 'dp.delisted_date'
]

print(f"INSERT columns: {len(insert_columns)}")
print(f"Data values: {len(data_values)}")

print("\nINSERT columns:")
for i, col in enumerate(insert_columns):
    print(f"{i+1:2d}. {col}")

print("\nData values:")
for i, val in enumerate(data_values):
    print(f"{i+1:2d}. {val}")

# Check for mismatches
if len(insert_columns) != len(data_values):
    print(f"\nMISMATCH: {len(insert_columns)} columns vs {len(data_values)} values")
else:
    print(f"\nMATCH: {len(insert_columns)} columns = {len(data_values)} values")
