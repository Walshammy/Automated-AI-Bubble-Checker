# AI Bubble Detection Project

## Project Structure

### Main Directory
- `README.md` - This file

### Organized Subdirectories

#### 1. Data Collection (`data_collection/`)
**Purpose**: Comprehensive stock data collection and management

**Contents**:
- `stock_data_collector.py` - **UNIFIED** data collection script for all markets
- `database_monitor.py` - Database monitoring and analysis
- `db_visualizer.py` - Database visualization tools
- `NZX_ASX.xlsx` - Stock universe (177 NZX + 2,327 ASX stocks)
- `USMarket.xlsx` - Stock universe (889 US stocks)
- `unified_stock_data.db` - **MAIN** SQLite database with all collected data
- `completed_tickers.json` - Progress tracking for resume capability

#### 2. Valuation Analysis (`valuation_analysis/`)
**Purpose**: Stock valuation analysis and screening

**Contents**:
- `stock_valuation_scraper.py` - Comprehensive valuation analysis with bubble detection
- `stock_valuation_data.db` - SQLite database with valuation data
- `valuation_results/` - Excel output files with analysis results

#### 3. Bubble Analysis (`bubble_analysis/`)
**Purpose**: Bubble detection research and analysis

**Contents**:
- `Review of AI bubble Indicators.md` - Analysis documentation and research

## Current Data Scale

### Unified Stock Data Collection
- **Database**: `data_collection/unified_stock_data.db`
- **Coverage**: 3,270 stocks total
  - 177 NZX stocks
  - 2,327 ASX stocks  
  - 889 US stocks
- **Data Types**: Historical prices, fundamentals, analyst ratings, earnings, corporate actions, institutional holdings
- **Features**: Parallel processing, progress tracking, resume capability

### Valuation Data
- **Database**: `valuation_analysis/stock_valuation_data.db`
- **Coverage**: Comprehensive valuation analysis results
- **Output**: Excel files with detailed analysis

## Usage Instructions

### Data Collection
```bash
# Navigate to data collection directory
cd data_collection

# Run unified data collection
python stock_data_collector.py

# Collection Options:
# 1. Test run (first 10 stocks)
# 2. Small collection (first 100 stocks)  
# 3. Medium collection (first 500 stocks)
# 4. Full collection (all 3,270 stocks)

# Monitor database
python database_monitor.py

# Visualize data
python db_visualizer.py
```

### Valuation Analysis
```bash
# Navigate to valuation analysis directory
cd valuation_analysis

# Run stock valuation analysis
python stock_valuation_scraper.py
```

### Bubble Analysis
```bash
# Navigate to bubble analysis directory
cd bubble_analysis

# Review bubble indicators documentation
# (Open Review of AI bubble Indicators.md)
```

## Key Features

### Optimizations
- **Fixed SQLite threading** - Thread-safe database operations
- **Reduced API calls** - From 7-10 to 4 calls per stock
- **Connection pooling** - Thread-local connections for parallel processing
- **Progress tracking** - Resume from where you left off after crashes
- **Parallel processing** - 3 concurrent workers for 66% speed improvement
- **Early termination** - Skips delisted/invalid stocks immediately
- **Vectorized processing** - Efficient pandas operations
- **Rate limiting protection** - Adaptive delays with exponential backoff

### Database Schema
- **`historical_prices`** - OHLCV data with weekly/daily intervals
- **`current_fundamentals`** - P/E, P/B, ROE, margins, growth metrics
- **`analyst_ratings`** - Analyst recommendations and upgrades/downgrades
- **`earnings_history`** - Quarterly earnings data with surprises
- **`corporate_actions`** - Dividends and stock splits
- **`institutional_holdings`** - Major institutional ownership data
- **`extended_price_data`** - 52-week highs/lows, volume metrics

## Workflow

1. **Data Collection**: Use `stock_data_collector.py` to gather comprehensive data
2. **Valuation Analysis**: Use `stock_valuation_scraper.py` to analyze stock valuations
3. **Bubble Detection**: Use research in `bubble_analysis/` to identify bubble patterns
4. **Integration**: Combine insights from all three areas for comprehensive analysis

## Next Steps
1. **Run Full Collection**: Execute option 4 for all 3,270 stocks
2. **Analyze Results**: Use database monitor and visualizer tools
3. **Bubble Detection**: Apply bubble indicators to collected data
4. **Generate Reports**: Create comprehensive analysis reports