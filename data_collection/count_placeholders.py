#!/usr/bin/env python3
"""
Count SQL placeholders
"""

sql = '''
        INSERT OR REPLACE INTO comprehensive_data (
            ticker, date, open_price, high_price, low_price, close_price,
            volume, adjusted_close, market_cap, pe_ratio, pb_ratio, peg_ratio,
            ps_ratio, dividend_yield, roe, roa, roic, debt_to_equity, current_ratio,
            fcf_yield, eps_ttm, eps_growth_5y, revenue_growth_5y, gross_margin,
            operating_margin, net_margin, beta, volatility_1y, max_drawdown_5y,
            sector, industry, exchange, is_delisted, delisted_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

# Count placeholders
placeholders = sql.count('?')
print(f"Number of placeholders: {placeholders}")

# Count columns
columns = [
    'ticker', 'date', 'open_price', 'high_price', 'low_price', 'close_price',
    'volume', 'adjusted_close', 'market_cap', 'pe_ratio', 'pb_ratio', 'peg_ratio',
    'ps_ratio', 'dividend_yield', 'roe', 'roa', 'roic', 'debt_to_equity', 'current_ratio',
    'fcf_yield', 'eps_ttm', 'eps_growth_5y', 'revenue_growth_5y', 'gross_margin',
    'operating_margin', 'net_margin', 'beta', 'volatility_1y', 'max_drawdown_5y',
    'sector', 'industry', 'exchange', 'is_delisted', 'delisted_date'
]

print(f"Number of columns: {len(columns)}")

# Show the placeholders
placeholder_positions = []
for i, char in enumerate(sql):
    if char == '?':
        placeholder_positions.append(i)

print(f"\nPlaceholder positions:")
for i, pos in enumerate(placeholder_positions):
    print(f"{i+1:2d}. Position {pos}")

print(f"\nMISMATCH: {placeholders} placeholders vs {len(columns)} columns")
