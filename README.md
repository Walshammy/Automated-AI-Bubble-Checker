# AI Bubble Detection Project

## 🚀 Project Overview

A comprehensive system for detecting AI and technology bubbles through advanced financial data collection and analysis. This project provides sophisticated bubble detection capabilities across multiple exchanges using **ONLY REAL DATA** from external sources.

## 📊 Current Data Scale (REAL DATA ONLY)

### 🎯 Unified Stock Data Database (MAIN)
- **Database**: `C:\Users\james\Downloads\StockDB\unified_stock_data.db` (886.3 MB)
- **Total Records**: **5,643,124 records**
- **Coverage**: **7,074 companies** across US, ASX, and NZX exchanges
- **Historical Prices**: **5,643,124 price records** (1962-2025)
- **Current Fundamentals**: **6,095 companies** with financial metrics
- **Analyst Ratings**: **78 ratings** across 64 companies
- **Corporate Actions**: **198,285 actions** (dividends, splits) from 4,701 companies
- **Collection Progress**: **56.1% complete** (7,074/12,617 stocks)
- **Status**: ✅ **ACTIVE COLLECTION** - Growing at ~560 stocks/hour

### 📰 Consolidated Financial Database
- **Database**: `consolidated_data/unified_financial_data.db` (0.98 MB)
- **Total Announcements**: **1,852 financial announcements**
- **Coverage**: ASX and NZX exchanges
- **Date Range**: 2022-10-12 to 2025-10-11

### 💰 Valuation Analysis Database
- **Database**: `valuation_analysis/stock_valuation_data.db` (0.03 MB)
- **Coverage**: **3 companies** with comprehensive valuation analysis
- **Data Types**: Current price, sector analysis, financial ratios
- **Purpose**: Stock screening and bubble detection analysis

## 🗂️ Repository Structure

### 📊 Data Collection (`data_collection/`)
**Purpose**: Comprehensive stock data collection from Yahoo Finance API

**Contents**:
- `stock_data_collector.py` - **MAIN** unified data collection script
- `database_monitor.py` - Database monitoring and analysis
- `NZX_ASX.xlsx` - Stock universe (177 NZX + 2,327 ASX stocks)
- `USMarket_Comprehensive.xlsx` - **EXPANDED** US stock universe (10,142 stocks)
- `unified_stock_data.db` - Local SQLite database (backup/legacy)
- `completed_tickers.json` - Progress tracking for resume capability

**Active Database**: `C:\Users\james\Downloads\StockDB\unified_stock_data.db` (886.3 MB)

### 💰 Valuation Analysis (`valuation_analysis/`)
**Purpose**: Stock valuation analysis and screening

**Contents**:
- `stock_valuation_scraper.py` - Comprehensive valuation analysis with bubble detection
- `stock_valuation_data.db` - SQLite database with valuation data
- `valuation_results/` - Excel output files with analysis results

### 🔍 Bubble Analysis (`bubble_analysis/`)
**Purpose**: Bubble detection research and analysis

**Contents**:
- `Review of AI bubble Indicators.md` - Analysis documentation and research

### 📊 Monitoring Scripts (Root Directory)
**Purpose**: Real-time collection monitoring and management

**Contents**:
- `monitor_collection.py` - Real-time progress monitoring
- `run_collection.py` - Background collection execution

## 🚀 Usage Instructions

### 📊 Stock Data Collection
```bash
# Run comprehensive data collection (12,617 stocks)
python run_collection.py

# Monitor real-time progress
python monitor_collection.py

# Collection Options:
# 1. Test run (first 10 stocks)
# 2. Small collection (first 100 stocks)  
# 3. Medium collection (first 500 stocks)
# 4. Full collection (all 12,617 stocks)

# Monitor database growth
python data_collection/database_monitor.py
```

### 💰 Valuation Analysis
```bash
# Navigate to valuation analysis directory
cd valuation_analysis

# Run stock valuation analysis
python stock_valuation_scraper.py
```

### 🔗 Database Access
```bash
# Access main unified stock database
# Database: C:\Users\james\Downloads\StockDB\unified_stock_data.db

# Query examples:
# - Historical price data for any ticker (5.6M+ records)
# - Current fundamentals and ratios (6,095 companies)
# - Analyst ratings and recommendations (78 ratings)
# - Corporate actions and dividends (198K+ actions)
# - Institutional holdings data
```

## 🎯 Key Features

### ✅ Real Data Sources
- **Yahoo Finance API**: Historical prices, fundamentals, analyst data
- **NZX Website Scraping**: Financial announcements and reports
- **No Fake Data**: All manufactured/test data has been removed

### 📈 Data Quality
- **5.6M+ Records**: Comprehensive historical coverage (1962-2025)
- **Multi-Exchange**: US (10,142), ASX (2,327), and NZX (177) markets
- **Real-Time Updates**: Current market data and fundamentals
- **Progress Tracking**: Resume capability for large collections
- **Active Collection**: Currently collecting at 560 stocks/hour

### 🔧 Technical Features
- **Optimized Collection**: Single API call per data type per stock
- **Connection Pooling**: Efficient database operations
- **Rate Limiting**: Adaptive delays with exponential backoff
- **Error Handling**: Robust retry logic and progress tracking
- **Parallel Processing**: Concurrent data collection

## 🗄️ Database Schema

### Unified Stock Database (`C:\Users\james\Downloads\StockDB\unified_stock_data.db`) - 886.3 MB
- **`historical_prices`** - OHLCV data with weekly/daily intervals (5,643,124 records)
- **`current_fundamentals`** - P/E, P/B, ROE, margins, growth metrics (6,095 records)
- **`analyst_ratings`** - Analyst recommendations and upgrades/downgrades (78 records)
- **`corporate_actions`** - Dividends and stock splits (198,285 records)
- **`institutional_holdings`** - Major institutional ownership data
- **`extended_price_data`** - 52-week highs/lows, volume metrics
- **`market_indicators`** - Market index and indicator data
- **`financial_announcements`** - NZX financial announcements

### Consolidated Financial Database (`consolidated_data/unified_financial_data.db`) - 0.98 MB
- **`financial_announcements`** - Cross-exchange financial announcements (1,852 records)
- **`company_metadata`** - Company information across exchanges (0 records)
- **`collection_stats`** - System performance and coverage statistics (2 records)

### Valuation Analysis Database (`valuation_analysis/stock_valuation_data.db`) - 0.03 MB
- **`investment_theses`** - Comprehensive stock analysis and investment recommendations
- **`valuation_metrics`** - Calculated financial ratios and metrics
- **`bubble_indicators`** - AI bubble detection analysis

## 📊 Performance Metrics

### Collection Efficiency
- **API Calls**: Reduced from 7-10 to 4 calls per stock
- **Database Operations**: Single transaction per stock
- **Processing Speed**: 66% improvement with parallel processing
- **Resume Capability**: Automatic progress tracking and recovery

### Data Coverage
- **US Market**: 10,142 stocks with comprehensive data
- **ASX Market**: 2,327 stocks with market data
- **NZX Market**: 177 stocks with market data + announcements
- **Total Coverage**: 12,617+ companies across multiple exchanges
- **Collection Progress**: 56.1% complete (7,074/12,617 stocks)

## 🔍 Data Quality Assurance

### ✅ Verified Real Data Sources
- **Yahoo Finance API**: All market data verified as real
- **NZX Website**: All announcements scraped from official sources
- **No Test Data**: All fake/manufactured data removed
- **Quality Validation**: Data consistency checks implemented

### ❌ Removed Fake Data
- **Balance Sheet Data**: 482 fake records removed
- **Financial Documents**: 720 fake records removed
- **ASX Announcements**: All fake generated data removed
- **Test Data**: All placeholder and mock data eliminated

## 🎯 Investment Analysis Capabilities

### Stock Screening
- **Valuation Metrics**: P/E, P/B, ROE, margins analysis
- **Growth Analysis**: Revenue and earnings growth trends
- **Risk Assessment**: Volatility and drawdown analysis
- **Sector Analysis**: Industry-specific metrics and comparisons

### Bubble Detection
- **AI Sector Analysis**: Technology stock valuation assessment
- **Market Indicators**: Overall market health and trends
- **Institutional Activity**: Major investor behavior analysis
- **Earnings Quality**: Revenue and profit sustainability

## 📈 Future Enhancements

### Planned Improvements
- **Enhanced ASX Collection**: Real ASX announcement scraping
- **Real-Time Updates**: Automated data refresh capabilities
- **Advanced Analytics**: Machine learning bubble detection
- **Portfolio Analysis**: Multi-stock portfolio evaluation

### Data Expansion
- **Additional Exchanges**: European and Asian markets
- **Alternative Data**: ESG metrics and sentiment analysis
- **Historical Analysis**: Extended historical data collection
- **Real-Time Feeds**: Live market data integration

## 🤝 Contributing

This project focuses on **real data collection only**. All contributions must:
- Use verified external data sources
- Include proper error handling and validation
- Maintain data quality standards
- Avoid any fake or test data generation

## 📄 License

This project is designed for educational and research purposes in financial analysis and bubble detection.

---

**Last Updated**: 2025-10-11  
**Data Status**: All fake data removed, only real external sources used  
**Total Real Records**: 5,643,124+ across multiple databases  
**Collection Status**: ✅ **ACTIVE** - 56.1% complete (7,074/12,617 stocks)  
**Database Size**: 886.3 MB and growing