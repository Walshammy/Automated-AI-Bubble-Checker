# AI Bubble Detection Project

## 🚀 Project Overview

A comprehensive system for detecting AI and technology bubbles through advanced financial data collection, analysis, and machine learning. This project provides sophisticated bubble detection capabilities across multiple exchanges using from external sources.

## 📊 Current Status (October 2025)

### 🎯 Database Status
- **Main Database**: `C:\Users\james\Downloads\StockDB\unified_stock_data.db` (950 MB)
- **Total Records**: **5,767,035 historical price records**
- **Coverage**: **7,239 companies** across US, ASX, and NZX exchanges
- **Collection Progress**: **46.6% complete** (5,875/12,617 stocks)
- **Status**: ⏸️ **PAUSED** for anomaly analysis

### 🤖 Machine Learning Capabilities
- **Anomaly Detection**: Isolation Forest algorithm implemented
- **Anomalies Detected**: 695 stocks (10% anomaly rate)
- **Features**: 20 technical indicators per stock
- **Visualization**: Comprehensive analysis charts
- **Export**: CSV reports and summary files

## 🗂️ Repository Structure

### 📊 Data Collection (`data_collection/`)
**Purpose**: Comprehensive stock data collection from Yahoo Finance API

**Contents**:
- `stock_data_collector.py` - **MAIN** unified data collection script
- `database_monitor.py` - Database monitoring and analysis
- `NZX_ASX.xlsx` - Stock universe (177 NZX + 2,327 ASX stocks)
- `USMarket_Comprehensive.xlsx` - **EXPANDED** US stock universe (10,142 stocks)
- `unified_stock_data.db` - Local SQLite database (282 MB backup)
- `completed_tickers.json` - Progress tracking for resume capability

**Active Database**: `C:\Users\james\Downloads\StockDB\unified_stock_data.db` (950 MB)

### 🤖 Machine Learning (`MachineLearning/`)
**Purpose**: Advanced analytics and anomaly detection

**Contents**:
- `anomaly_detection.py` - Isolation Forest anomaly detection system
- `anomaly_detection_full_results.csv` - Complete analysis results (3.1 MB)
- `detected_anomalies.csv` - Anomalous stocks only (305 KB)
- `anomaly_detection_report.txt` - Summary report
- `anomaly_detection_results.png` - Visualization charts (493 KB)

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

### 📊 Core Scripts (Root Directory)
**Purpose**: Automated collection execution and monitoring

**Contents**:
- `run_automated_collection.py` - **NEW** Automated collection runner (starts with updates, then historical)
- `monitor_collection.py` - Real-time progress monitoring
- `run_collection.py` - Background collection execution
- `optimized_schema_exporter.py` - Database schema export (5 sheets, 62% smaller)
- `PROJECT_SUMMARY.md` - Comprehensive project documentation

## 🚀 Usage Instructions

### 📊 Automated Stock Data Collection
```bash
# AUTOMATED COLLECTION - Starts with updates, then historical data
python run_automated_collection.py

# OR run the collector directly
python data_collection/stock_data_collector.py

# Monitor real-time progress
python monitor_collection.py

# Background collection with monitoring
python run_collection.py

# Monitor database growth
python data_collection/database_monitor.py
```

### 🤖 Automated Collection Process
The system now automatically:
1. **Phase 1**: Updates existing companies with latest data (equivalent to option 5)
2. **Phase 2**: Collects historical data for all companies
3. **No User Input Required**: Fully automated process
4. **Progress Tracking**: Clear phase separation and progress reporting

### 🤖 Machine Learning Analysis
```bash
# Navigate to MachineLearning directory
cd MachineLearning

# Run anomaly detection
python anomaly_detection.py

# Results will be saved as:
# - anomaly_detection_full_results.csv
# - detected_anomalies.csv
# - anomaly_detection_report.txt
# - anomaly_detection_results.png
```

### 💰 Valuation Analysis
```bash
# Navigate to valuation analysis directory
cd valuation_analysis

# Run stock valuation analysis
python stock_valuation_scraper.py
```

### 📊 Database Schema Export
```bash
# Export optimized database schema
python optimized_schema_exporter.py

# Creates: optimized_schema_export_YYYYMMDD_HHMMSS.xlsx
# Contains: 5 consolidated sheets (vs 42 in original)
```

## 🎯 Key Features

### ✅ Real Data Sources
- **Yahoo Finance API**: Historical prices, fundamentals, analyst data
- **NZX Website Scraping**: Financial announcements and reports
- **No Fake Data**: All manufactured/test data has been removed

### 📈 Data Quality
- **5.7M+ Records**: Comprehensive historical coverage (1962-2025)
- **Multi-Exchange**: US (10,142), ASX (2,327), and NZX (177) markets
- **Real-Time Updates**: Current market data and fundamentals
- **Progress Tracking**: Resume capability for large collections
- **Collection Rate**: ~76 stocks/hour when active

### 🤖 Machine Learning Features
- **Isolation Forest**: Unsupervised anomaly detection
- **Feature Engineering**: 20 technical indicators per stock
- **Dimensionality Reduction**: PCA to 14 components (95.9% variance)
- **Visualization**: 6-panel analysis charts
- **Export Capabilities**: CSV reports and summary files

### 🔧 Technical Features
- **Automated Collection**: Two-phase process (updates first, then historical data)
- **Optimized Collection**: Single API call per data type per stock
- **Connection Pooling**: Efficient database operations
- **Rate Limiting**: Adaptive delays with exponential backoff
- **Error Handling**: Robust retry logic and progress tracking
- **Parallel Processing**: Concurrent data collection
- **No User Input**: Fully automated execution

## 🗄️ Database Schema

### Unified Stock Database (`C:\Users\james\Downloads\StockDB\unified_stock_data.db`) - 950 MB
- **`historical_prices`** - OHLCV data with weekly/daily intervals (5,767,035 records)
- **`current_fundamentals`** - P/E, P/B, ROE, margins, growth metrics (6,403 records)
- **`analyst_ratings`** - Analyst recommendations and upgrades/downgrades (78 records)
- **`corporate_actions`** - Dividends and stock splits (207,706 records)
- **`institutional_holdings`** - Major institutional ownership data
- **`extended_price_data`** - 52-week highs/lows, volume metrics
- **`market_indicators`** - Market index and indicator data
- **`financial_announcements`** - NZX financial announcements

## 📊 Anomaly Detection Results

### 🚨 Key Findings
- **Total Anomalies**: 695 stocks (10.0% anomaly rate)
- **Most Anomalous**: CTTRF (PNK) - 35.2% return, extremely low volatility
- **Exchange Distribution**: 
  - PNK (Pink Sheets): 32.5% of anomalies
  - NCM (Nasdaq Capital): 20.1%
  - ASX (Australian): 18.3%

### 📈 Anomaly Characteristics
- **Average 6-month return**: 208.2% (vs normal stocks)
- **Average volatility**: 0.689 (higher than normal)
- **Average volume ratio**: 1.31 (elevated trading)
- **Average max drawdown**: -70.2% (severe losses)

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
- **Collection Progress**: 46.6% complete (5,875/12,617 stocks)

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
- **Anomaly Detection**: Machine learning-based outlier identification
- **Market Indicators**: Overall market health and trends
- **Institutional Activity**: Major investor behavior analysis
- **Earnings Quality**: Revenue and profit sustainability

## 📈 Future Enhancements

### Planned Improvements
- **Enhanced ASX Collection**: Real ASX announcement scraping
- **Real-Time Updates**: Automated data refresh capabilities
- **Advanced Analytics**: Additional ML models for bubble detection
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

**Last Updated**: 2025-10-14  
**Data Status**: All fake data removed, only real external sources used  
**Total Real Records**: 5,767,035+ across multiple databases  
**Collection Status**: ⏸️ **PAUSED** - 46.6% complete (5,875/12,617 stocks)  
**Database Size**: 950 MB  
**ML Status**: ✅ **ANOMALY DETECTION COMPLETE** - 695 anomalies identified