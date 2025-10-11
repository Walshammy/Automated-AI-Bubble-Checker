# Data Collection Module

## 🎯 Overview

The data collection module is the core component of the AI Bubble Detection Project, responsible for gathering comprehensive financial data from Yahoo Finance API across multiple exchanges. This module collects **ONLY REAL DATA** from external sources.

## 📊 Current Status

### ✅ Active Collection
- **Database**: `C:\Users\james\Downloads\StockDB\unified_stock_data.db` (886.3 MB)
- **Progress**: 7,074 stocks completed (56.1% of 12,617 total)
- **Collection Rate**: ~560 stocks per hour
- **Status**: ✅ **ACTIVE** - Running continuously

### 📈 Data Scale
- **Total Records**: 5,643,124 historical price records
- **Companies**: 7,074 with historical data
- **Fundamentals**: 6,095 companies with current metrics
- **Corporate Actions**: 198,285 dividend/split records
- **Analyst Ratings**: 78 ratings across 64 companies

## 🗂️ File Structure

### Core Scripts
- **`stock_data_collector.py`** - Main unified data collection engine
- **`database_monitor.py`** - Database monitoring and statistics
- **`completed_tickers.json`** - Progress tracking for resume capability

### Stock Universe Files
- **`USMarket_Comprehensive.xlsx`** - 10,142 US stocks (expanded universe)
- **`NZX_ASX.xlsx`** - 2,475 stocks (177 NZX + 2,327 ASX)
- **`USMarket.xlsx`** - Legacy US market file (889 stocks)

### Database Files
- **`unified_stock_data.db`** - Local backup database (282 MB)
- **Active Database**: `C:\Users\james\Downloads\StockDB\unified_stock_data.db` (886.3 MB)

### Backup Files
- **`backups/`** - Automatic backups of stock universe files
  - `NZX_ASX_20251011_132819.xlsx`
  - `USMarket_20251011_132819.xlsx`

## 🚀 Usage Instructions

### Starting Collection
```bash
# From project root directory
python run_collection.py

# Or run directly
python data_collection/stock_data_collector.py
```

### Monitoring Progress
```bash
# Real-time progress monitoring
python monitor_collection.py

# Database statistics
python data_collection/database_monitor.py
```

### Collection Options
1. **Test Run**: First 10 stocks (for testing)
2. **Small Collection**: First 100 stocks
3. **Medium Collection**: First 500 stocks
4. **Full Collection**: All 12,617 stocks (recommended)

## 🔧 Technical Features

### Data Collection Engine (`stock_data_collector.py`)
- **Single API Call**: One call per data type per stock
- **Batch Processing**: Processes stocks in batches of 5
- **Rate Limiting**: Adaptive delays with exponential backoff
- **Error Handling**: Robust retry logic and graceful failures
- **Progress Tracking**: Automatic resume capability
- **Concurrent Processing**: Uses ThreadPoolExecutor for efficiency

### Data Types Collected
1. **Historical Prices**: OHLCV data (daily/weekly intervals)
2. **Current Fundamentals**: P/E, P/B, ROE, margins, growth
3. **Analyst Ratings**: Recommendations and upgrades/downgrades
4. **Corporate Actions**: Dividends and stock splits
5. **Institutional Holdings**: Major institutional ownership
6. **Extended Price Data**: 52-week highs/lows, volume metrics

### Database Schema
```sql
-- Main tables in unified_stock_data.db
historical_prices (ticker, date, open, high, low, close, volume)
current_fundamentals (ticker, pe_ratio, pb_ratio, roe, margins, etc.)
analyst_ratings (ticker, rating, target_price, recommendation)
corporate_actions (ticker, action_type, amount, ex_date)
institutional_holdings (ticker, institution, shares, percentage)
extended_price_data (ticker, year_high, year_low, avg_volume)
```

## 📊 Performance Metrics

### Collection Efficiency
- **API Optimization**: Reduced from 7-10 to 4 calls per stock
- **Database Operations**: Single transaction per stock
- **Processing Speed**: 66% improvement with parallel processing
- **Success Rate**: ~90% (handles invalid tickers gracefully)

### Current Performance
- **Collection Rate**: 560 stocks per hour
- **Database Growth**: ~67 MB per hour
- **Memory Usage**: ~300-400 MB per process
- **CPU Usage**: High activity during collection

## 🔍 Data Quality Assurance

### ✅ Real Data Sources
- **Yahoo Finance API**: All market data verified as real
- **No Fake Data**: All manufactured/test data removed
- **Quality Validation**: Data consistency checks implemented
- **Error Handling**: Graceful handling of missing/invalid data

### ❌ Removed Components
- **Fake Balance Sheets**: All manufactured financial data removed
- **Test Data**: All placeholder and mock data eliminated
- **ASX Fake Announcements**: All generated announcements removed

## 🎯 Stock Universe Details

### US Market (10,142 stocks)
- **Source**: `USMarket_Comprehensive.xlsx`
- **Coverage**: Major US exchanges (NYSE, NASDAQ, AMEX)
- **Data Types**: All data types collected
- **Historical Range**: Up to 63 years (1962-2025)

### ASX Market (2,327 stocks)
- **Source**: `NZX_ASX.xlsx`
- **Coverage**: Australian Securities Exchange
- **Data Types**: Historical prices, fundamentals, corporate actions
- **Historical Range**: Up to 20+ years

### NZX Market (177 stocks)
- **Source**: `NZX_ASX.xlsx`
- **Coverage**: New Zealand Exchange
- **Data Types**: Historical prices, fundamentals, corporate actions
- **Historical Range**: Up to 20+ years

## 🔧 Configuration

### Database Paths
```python
# Main database (active)
db_path = r"C:\Users\james\Downloads\StockDB\unified_stock_data.db"

# Backup database
backup_path = r"C:\Users\james\Downloads\StockDB\unified_stock_data_backup.db"
```

### Collection Settings
```python
# Performance settings
max_workers = 1          # Conservative API usage
batch_size = 5           # Stocks per batch
retry_attempts = 3       # Retry failed requests
rate_limit_delay = 0.1   # Base delay between requests
```

## 📈 Monitoring and Statistics

### Real-Time Monitoring
- **Progress Tracking**: Shows completion percentage
- **Database Size**: Monitors growth in real-time
- **Collection Rate**: Tracks stocks per hour
- **Error Reporting**: Logs failed collections

### Database Statistics
- **Record Counts**: Total records per table
- **Coverage Analysis**: Companies with complete data
- **Date Ranges**: Historical data coverage
- **Performance Metrics**: Collection efficiency

## 🚨 Troubleshooting

### Common Issues
1. **Database Locked**: Ensure no other processes are accessing the database
2. **API Rate Limits**: Collection automatically handles rate limiting
3. **Invalid Tickers**: Gracefully skipped and logged
4. **Network Issues**: Automatic retry with exponential backoff

### Recovery Procedures
- **Resume Collection**: Automatically resumes from last completed ticker
- **Database Backup**: Automatic backups before major operations
- **Progress Tracking**: JSON file maintains completion state

## 📋 Dependencies

### Required Packages
```
yfinance>=0.2.18
pandas>=1.5.0
sqlite3 (built-in)
requests>=2.28.0
concurrent.futures (built-in)
```

### System Requirements
- **Python**: 3.8+
- **Memory**: 4GB+ RAM recommended
- **Storage**: 2GB+ free space for database
- **Network**: Stable internet connection

## 🎯 Future Enhancements

### Planned Improvements
- **Real-Time Updates**: Automated data refresh capabilities
- **Additional Exchanges**: European and Asian markets
- **Enhanced Error Handling**: More sophisticated retry logic
- **Performance Optimization**: Further speed improvements

### Data Expansion
- **Alternative Data**: ESG metrics and sentiment analysis
- **Extended History**: Deeper historical data collection
- **Real-Time Feeds**: Live market data integration
- **Options Data**: Options chain and volatility data

---

**Last Updated**: 2025-10-11  
**Status**: ✅ **ACTIVE COLLECTION** - 56.1% complete  
**Database Size**: 886.3 MB and growing  
**Collection Rate**: 560 stocks/hour