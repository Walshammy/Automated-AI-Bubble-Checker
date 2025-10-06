# AI Bubble Detection Project

## 🚀 Project Overview

A comprehensive system for detecting AI and technology bubbles through advanced financial data collection, analysis, and valuation screening. This project achieves **97.2% NZX market coverage** with **12,339+ financial announcements** and provides sophisticated bubble detection capabilities.

## 📊 Project Structure

### Main Directory
- `README.md` - This file
- `check_databases.py` - Database verification and analysis tools

### Organized Subdirectories

#### 1. Data Collection (`data_collection/`)
**Purpose**: Comprehensive stock data collection and management

**Contents**:
- `stock_data_collector.py` - **UNIFIED** data collection script for all markets
- `database_monitor.py` - Database monitoring and analysis
- `NZX_ASX.xlsx` - Stock universe (177 NZX + 2,327 ASX stocks)
- `USMarket.xlsx` - Stock universe (889 US stocks)
- `unified_stock_data.db` - **MAIN** SQLite database with all collected data
- `completed_tickers.json` - Progress tracking for resume capability

#### 2. Balance Sheet Scraper (`Balance_Sheet_Scraper/`) ⭐ **NEW**
**Purpose**: Comprehensive NZX financial announcement collection and balance sheet processing

**Contents**:
- `comprehensive_nzx_scraper.py` - **MAIN** NZX announcement scraper (97.2% coverage)
- `main_balance_sheet_scraper.py` - Orchestrator and PDF processor
- `balance_sheet_database.py` - Database management
- `balance_sheet_processor.py` - PDF financial data extraction
- `balance_sheet_data/` - Output directory with PDFs and datasets
- `README.md` - Detailed documentation
- `CHANGELOG.md` - Comprehensive change history

#### 3. Valuation Analysis (`valuation_analysis/`)
**Purpose**: Stock valuation analysis and screening

**Contents**:
- `stock_valuation_scraper.py` - Comprehensive valuation analysis with bubble detection
- `stock_valuation_data.db` - SQLite database with valuation data
- `valuation_results/` - Excel output files with analysis results

#### 4. Bubble Analysis (`bubble_analysis/`)
**Purpose**: Bubble detection research and analysis

**Contents**:
- `Review of AI bubble Indicators.md` - Analysis documentation and research

## 📈 Current Data Scale

### 🎯 NZX Financial Announcements (NEW!)
- **Database**: `data_collection/unified_stock_data.db`
- **Coverage**: **172 out of 177 NZX companies (97.2%)**
- **Announcements**: **12,339+ financial announcements**
- **Historical Data**: **3 years (2023-2025)**
- **Top Companies**: BIT (399), HFL (261), TEM (223), FCT (221), MDZ (197)

### 📊 Unified Stock Data Collection
- **Database**: `data_collection/unified_stock_data.db`
- **Coverage**: 3,270 stocks total
  - 177 NZX stocks
  - 2,327 ASX stocks  
  - 889 US stocks
- **Data Types**: Historical prices, fundamentals, analyst ratings, earnings, corporate actions, institutional holdings
- **Features**: Parallel processing, progress tracking, resume capability

### 💰 Valuation Data
- **Database**: `valuation_analysis/stock_valuation_data.db`
- **Coverage**: Comprehensive valuation analysis results
- **Output**: Excel files with detailed analysis

## 🚀 Usage Instructions

### 🎯 NZX Financial Announcements (NEW!)
```bash
# Navigate to Balance Sheet Scraper directory
cd Balance_Sheet_Scraper

# Collect 3 years of financial announcements for all NZX companies
python comprehensive_nzx_scraper.py --years 3

# Process existing PDFs for balance sheet data
python main_balance_sheet_scraper.py --process-existing

# View comprehensive database statistics
python main_balance_sheet_scraper.py --database-stats

# Export results to Excel
python main_balance_sheet_scraper.py --process-existing --export excel
```

### 📊 Data Collection
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
```

### 💰 Valuation Analysis
```bash
# Navigate to valuation analysis directory
cd valuation_analysis

# Run stock valuation analysis
python stock_valuation_scraper.py
```

### 🔍 Bubble Analysis
```bash
# Navigate to bubble analysis directory
cd bubble_analysis

# Review bubble indicators documentation
# (Open Review of AI bubble Indicators.md)
```

## ✨ Key Features

### 🎯 NZX Financial Announcements (NEW!)
- **97.2% Market Coverage** - 172 out of 177 NZX companies
- **12,339+ Announcements** - Comprehensive financial data collection
- **3 Years Historical Data** - 2023-2025 coverage
- **Real-Time Progress Tracking** - ETA calculations and progress updates
- **Robust Error Handling** - Rate limiting and retry mechanisms
- **Unicode Compatibility** - Windows console support

### 📊 Data Collection Optimizations
- **Fixed SQLite threading** - Thread-safe database operations
- **Reduced API calls** - From 7-10 to 4 calls per stock
- **Connection pooling** - Thread-local connections for parallel processing
- **Progress tracking** - Resume from where you left off after crashes
- **Parallel processing** - 3 concurrent workers for 66% speed improvement
- **Early termination** - Skips delisted/invalid stocks immediately
- **Vectorized processing** - Efficient pandas operations
- **Rate limiting protection** - Adaptive delays with exponential backoff

### 🗄️ Database Schema
- **`financial_announcements`** - NZX financial announcements (12,339+ records)
- **`balance_sheet_data`** - Extracted financial data from PDFs
- **`historical_prices`** - OHLCV data with weekly/daily intervals
- **`current_fundamentals`** - P/E, P/B, ROE, margins, growth metrics
- **`analyst_ratings`** - Analyst recommendations and upgrades/downgrades
- **`earnings_history`** - Quarterly earnings data with surprises
- **`corporate_actions`** - Dividends and stock splits
- **`institutional_holdings`** - Major institutional ownership data
- **`extended_price_data`** - 52-week highs/lows, volume metrics

## 🔄 Workflow

1. **NZX Financial Data**: Use `comprehensive_nzx_scraper.py` to collect financial announcements
2. **Balance Sheet Processing**: Use `main_balance_sheet_scraper.py` to extract financial data from PDFs
3. **Stock Data Collection**: Use `stock_data_collector.py` to gather comprehensive market data
4. **Valuation Analysis**: Use `stock_valuation_scraper.py` to analyze stock valuations
5. **Bubble Detection**: Use research in `bubble_analysis/` to identify bubble patterns
6. **Integration**: Combine insights from all areas for comprehensive analysis

## 🎯 Next Steps
1. **Run NZX Collection**: Execute `comprehensive_nzx_scraper.py --years 3` for full NZX coverage
2. **Process PDFs**: Run `main_balance_sheet_scraper.py --process-existing` for balance sheet data
3. **Run Full Collection**: Execute option 4 for all 3,270 stocks
4. **Analyze Results**: Use database monitor and statistics tools
5. **Bubble Detection**: Apply bubble indicators to collected data
6. **Generate Reports**: Create comprehensive analysis reports

## 📊 Performance Metrics

### NZX Financial Announcements
- **Coverage**: 97.2% (172/177 companies)
- **Data Volume**: 12,339+ announcements
- **Processing Speed**: ~3.5 seconds per company
- **Success Rate**: 97.1% (170/175 companies)
- **Historical Depth**: 3 years (2023-2025)

### Overall System
- **Total Stocks**: 3,270 (NZX + ASX + US)
- **Database Size**: Comprehensive financial data
- **Processing**: Parallel and optimized
- **Resume Capability**: Full crash recovery