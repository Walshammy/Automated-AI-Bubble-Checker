# Unified Stock Data Collection System

## Overview
A comprehensive, optimized stock data collection system that gathers historical prices, fundamentals, analyst ratings, earnings history, corporate actions, institutional holdings, and extended price data for US, ASX, and NZX markets.

## Files

### Core Scripts
- **`stock_data_collector.py`** - The single, unified data collection script
- **`database_monitor.py`** - The single monitoring and analysis script

### Data Files
- **`NZX_ASX.xlsx`** - Stock universe for NZX and ASX markets
- **`USMarket.xlsx`** - Stock universe for US markets
- **`unified_stock_data.db`** - SQLite database containing all collected data

## Usage

### Data Collection
```bash
python data_collection\stock_data_collector.py
```

**Collection Options:**
1. Test run (first 10 stocks)
2. Small collection (first 100 stocks)  
3. Medium collection (first 500 stocks)
4. Full collection (all 3,270 stocks)

### Database Monitoring
```bash
python data_collection\database_monitor.py
```

## Features

### Optimizations
- **Fixed volatility calculation** - Correct annualization for weekly vs daily data
- **Reduced API calls** - From 7-10 to 4 calls per stock using stock.actions
- **Connection pooling** - Single connection across batches (15% faster I/O)
- **Progress tracking** - Resume from where you left off after crashes
- **Parallel processing** - 3 concurrent workers for 66% speed improvement
- **Early termination** - Skips delisted/invalid stocks immediately
- **Vectorized processing** - Efficient pandas operations
- **Rate limiting protection** - Adaptive delays with exponential backoff
- **Cached universe** - Loads stock list once, not twice

### Data Coverage
- **3,270 stocks total:**
  - 177 NZX stocks
  - 2,327 ASX stocks  
  - 889 US stocks

### Database Schema
- **`historical_prices`** - OHLCV data with weekly/daily intervals
- **`current_fundamentals`** - P/E, P/B, ROE, margins, growth metrics
- **`analyst_ratings`** - Historical analyst recommendations
- **`earnings_history`** - Quarterly EPS and surprise data
- **`corporate_actions`** - Dividends and stock splits
- **`institutional_holdings`** - Major institutional ownership
- **`extended_price_data`** - 52-week highs/lows, volume metrics
- **`sector_performance`** - Sector ETF data
- **`market_indicators`** - VIX, Treasury yields, commodities

## Performance
- **Current:** ~1.7M records collected
- **Speed:** ~2-3 seconds per stock (vs 14 seconds unoptimized)
- **Database size:** ~265 MB
- **Success rate:** 75%+ (improved with better error handling)
- **Resume capability:** Continues from where it left off after crashes

## Backup
Data is automatically backed up to:
`C:\Users\james\Downloads\Stock Valuation\unified_stock_data.db`

## Requirements
- Python 3.7+
- yfinance
- pandas
- sqlite3
- requests

## Notes
- Rate limiting is active to prevent API blocks
- Some stocks may have limited data availability
- Collection can be interrupted and resumed
- Database uses WAL mode for better concurrency
