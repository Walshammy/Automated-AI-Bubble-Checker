# Balance Sheet Scraper

A comprehensive system for scraping and extracting balance sheet and financial statement data from NZSX companies' financial reports.

## Overview

This system automatically:
- Scrapes financial announcements from NZX
- Downloads relevant PDF reports (annual reports, interim reports, etc.)
- Extracts structured financial data using advanced text parsing
- Stores data in a unified PostgreSQL database alongside your existing stock data
- Provides comprehensive reporting and analysis capabilities

## System Components

### 1. `balance_sheet_database.py`
- **Purpose**: Database schema and operations manager
- **Features**: 
  - Creates tables for financial announcements, documents, and balance sheet data
  - Handles data insertion and retrieval
  - Provides database statistics and reporting

### 2. `balance_sheet_processor.py`
- **Purpose**: Advanced PDF text extraction and parsing engine
- **Features**:
  - Sophisticated pattern matching for financial terms
  - Extracts balance sheet, P&L, and cash flow data
  - Calculates financial ratios automatically
  - Validates extracted data for accuracy

### 3. `enhanced_balance_sheet_scraper.py`
- **Purpose**: Main scraping engine with database integration
- **Features**:
  - Scrapes NZX announcements systematically
  - Downloads and categorizes financial documents
  - Orchestrates the data extraction process
  - Provides progress tracking and error handling

### 4. `main_balance_sheet_scraper.py`
- **Purpose**: Command-line interface and orchestrator
- **Features**:
  - Flexible ticker selection and filtering
  - Resume functionality for interrupted runs
  - Export capabilities (CSV, Excel, JSON)
  - Comprehensive progress reporting

## Installation & Setup

### Prerequisites
```bash
pip install pandas beautifulsoup4 pdfplumber requests sqlite3
```

### Database Setup
The system automatically initializes the database tables when first run. It uses the same database location as your existing stock data:
- **Location**: `../data_collection/unified_stock_data.db`

### Directory Structure
```
Balance_Sheet_Scraper/
├── balance_sheet_database.py      # Database manager
├── enhanced_balance_sheet_scraper.py  # Main scraper
├── balance_sheet_processor.py     # PDF processor
├── main_balance_sheet_scraper.py   # CLI orchestrator
├── README.md                       # This file
└── balance_sheet_data/            # Generated data directory
    ├── pdfs/                       # Downloaded PDFs
    └── datasets/                   # Exported datasets
```

## Usage

### Basic Usage

**Process all NZSX companies:**
```bash
python main_balance_sheet_scraper.py
```

**Process specific companies:**
```bash
python main_balance_sheet_scraper.py --tickers AIR FPH MCY SPK
```

**Dry run (test without downloading):**
```bash
python main_balance_sheet_scraper.py --dry-run
```

### Advanced Options

**Resume interrupted runs:**
```bash
python main_balance_sheet_scraper.py --resume
```

**Export results:**
```bash
python main_balance_sheet_scraper.py --export excel
```

**Show database statistics:**
```bash
python main_balance_sheet_scraper.py --database-stats
```

**Custom output directory:**
```bash
python main_balance_sheet_scraper.py --output-dir /path/to/custom/directory
```

### Command Line Options

- `--tickers` / `-t`: Specific ticker symbols to process
- `--exchange` / `-e`: Exchange to scrape (NZX, ASX)
- `--years` / `-y`: Number of years of data to retrieve
- `--dry-run`: Perform dry run without downloads
- `--resume`: Resume from previously processed tickers
- `--database-stats`: Show database statistics only
- `--export`: Export format (csv, excel, json)
- `--output-dir`: Custom output directory

## Database Schema

### Financial Announcements Table
Stores metadata about financial announcements:
- `announcement_id`: Unique announcement identifier
- `ticker`: Company ticker symbol
- `title`: Announcement title
- `announcement_url`: Link to announcement page
- `announcement_date`: Date of announcement
- `announcement_type`: Type (FLLYR, HALFYR, INTERIM, etc.)
- `exchange`: Exchange identifier
- `processed`: Whether announcement has been processed

### Balance Sheet Data Table
Stores extracted financial statement data:
- **Balance Sheet Items**: total_assets, current_assets, cash_and_equivalents, etc.
- **Profit & Loss Items**: revenue, gross_profit, operating_income, net_income, etc.
- **Cash Flow Items**: operating_cash_flow, investing_cash_flow, financing_cash_flow, etc.
- **Calculated Ratios**: current_ratio, debt_to_equity, return_on_assets, etc.
- **Metadata**: extraction_confidence, data_source, report_type

### Financial Documents Table
Stores PDF document metadata:
- `announcement_id`: Reference to announcement
- `pdf_url`: Original PDF URL
- `pdf_filename`: Local filename
- `pdf_path`: Local file path
- `document_type`: Type of document (annual_report, interim_report, etc.)
- `file_size_kb`: Document size
- `status`: Processing status

## Data Quality Features

### Extraction Confidence Scoring
- Calculates confidence based on successful extraction of key metrics
- Higher confidence = more complete financial data

### Data Validation
- Verifies accounting equations (Assets = Liabilities + Equity)
- Checks logical relationships (current assets ≤ total assets)
- Validates extracted numbers for reasonableness

### Advanced Pattern Matching
- Uses comprehensive financial terms dictionary
- Handles NZ accounting terminology
- Supports various number formats and currencies
- Processes negative numbers and multipliers

## Example Results

### Extracted Financial Data
```python
{
    'ticker': 'AIR',
    'announcement_id': '12345',
    'report_date': '2023-12-31',
    'report_type': 'FLLYR',
    'total_assets': 5245.6,
    'current_assets': 1234.5,
    'cash_and_equivalents': 89.2,
    'total_liabilities': 3456.7,
    'current_liabilities': 567.8,
    'total_equity': 1788.9,
    'revenue': 2345.6,
    'net_income': 123.4,
    'current_ratio': 2.17,
    'return_on_equity': 0.069,
    'extraction_confidence': 0.85
}
```

## Troubleshooting

### Common Issues

1. **PDF Download Fails**
   - Check internet connection
   - Some announcements may have broken links
   - System continues with next document on failure

2. **Poor Extraction Quality**
   - Some PDFs may have unusual formatting
   - System logs extraction confidence for monitoring
   - Manual verification recommended for critical data

3. **Database Errors**
   - Ensure database path is correct
   - Check file permissions
   - System creates backup tables automatically

### Logging
- Comprehensive logging to `balance_sheet_scraper.log`
- Real-time progress updates to console
- Error tracking and recovery

## Performance Considerations

### Recommended Settings
- **Small Batches**: Process 10-20 companies at a time
- **Delay Between Requests**: Built-in 2-second delays
- **Resume Functionality**: Use `--resume` for interrupted runs
- **Progress Monitoring**: System reports progress every 5 companies

### Resource Usage
- **Disk Space**: PDFs and extracted data require ~1-5MB per company
- **Memory**: Moderate usage, processes documents individually
- **Network**: Respectful delays built-in

## Integration with Existing System

This balance sheet scraper integrates seamlessly with your existing stock data collection system:

- **Same Database**: Uses `unified_stock_data.db`
- **Consistent Schema**: Follows existing naming conventions
- **Complementary Data**: Adds fundamental data to your price/market data
- **Unified Reporting**: Can be queried alongside stock price data

## Future Enhancements

Potential improvements and extensions:
- ASX company support
- Real-time announcement monitoring
- Automated ratio trend analysis
- Alert system for significant changes
- Machine learning for improved extraction accuracy

## Support

For issues or questions:
1. Check the log files for error details
2. Verify database connectivity and permissions
3. Review PDF download logs for failed requests
4. Confirm ticker symbols are valid NZSX codes

## License

This system is designed to work with your existing Automated AI Bubble Checker framework and follows the same data collection principles and ethical guidelines.
