# AI Bubble Detection Project

## Project Structure

### Main Directory
- `README.md` - This file
- `old_scripts/` - Legacy scripts (archived)

### Organized Subdirectories

#### 1. Data Collection (`data_collection/`)
**Purpose**: Historical data collection and management

**Contents**:
- `unified_historical_collector.py` - Historical data collection for US/ASX/NZX stocks
- `data_scale_summary.py` - Data scale analysis script
- `NZX_ASX.xlsx` - Stock universe (177 NZX + 2,300 ASX stocks)
- `*.db` - SQLite databases with historical data
- `*_progress.json` - Progress tracking files

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

### Historical Data Collections
1. **Smart Historical Data** (`data_collection/smart_historical_data.db`)
   - **12,007 records** from 10 US stocks
   - **Date Range**: 2000-01-01 to 2025-09-29
   - **Coverage**: Major US stocks (AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA, META, BRK-B, JNJ, PG)
   - **File Size**: 4.2 MB

2. **NZX/ASX Historical Data** (`data_collection/nzx_asx_historical_data.db`)
   - **2,739 records** from 5 NZX stocks
   - **Date Range**: 2006-01-02 to 2025-09-22
   - **Coverage**: NZX stocks (AFC.NZ, AFI.NZ, AFT.NZ, AGG.NZ, 2CC.NZ)
   - **File Size**: 0.9 MB

3. **Unified Historical Data** (`data_collection/unified_historical_data.db`)
   - **1,899 records** from 3 NZX stocks
   - **Date Range**: 2006-01-02 to 2025-09-22
   - **Coverage**: Test collection (2CC.NZ, AFC.NZ, AFI.NZ)
   - **File Size**: 0.7 MB

### Valuation Data
4. **Stock Valuation Data** (`valuation_analysis/stock_valuation_data.db`)
   - **3 records** from 3 stocks
   - **Coverage**: Test analysis results
   - **File Size**: 0.03 MB

### Progress Tracking
- **NZX/ASX Progress**: 5 completed, 0 failed, 2,472 pending
- **Smart Collection Progress**: 10 completed, 0 failed, 64 pending
- **Unified Collection Progress**: 3 completed, 0 failed, 2,542 pending

## Total Data Summary
- **Total Records**: 16,645 historical data points
- **Total Stocks Analyzed**: 18 unique stocks
- **Total Database Size**: ~5.8 MB
- **Date Coverage**: 2000-2025 (25+ years)
- **Markets Covered**: US, NZX, ASX

## Usage Instructions

### Data Collection
```bash
# Navigate to data collection directory
cd data_collection

# Run historical data collection
python unified_historical_collector.py

# Check data scale
python data_scale_summary.py
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

## Workflow

1. **Data Collection**: Use scripts in `data_collection/` to gather historical data
2. **Valuation Analysis**: Use scripts in `valuation_analysis/` to analyze stock valuations
3. **Bubble Detection**: Use research in `bubble_analysis/` to identify bubble patterns
4. **Integration**: Combine insights from all three areas for comprehensive analysis

## Next Steps
1. **Expand Collection**: Run full collection for all 2,545 stocks in universe
2. **Data Analysis**: Use collected data for comprehensive bubble analysis
3. **Regular Updates**: Set up periodic data collection
4. **Visualization**: Create charts and dashboards for data insights
5. **Research**: Develop advanced bubble detection algorithms