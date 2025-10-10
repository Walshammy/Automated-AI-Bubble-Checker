# ASX Scraper - Beta Version

## Overview
This is a beta implementation of an ASX announcement scraper, based on the successful NZX balance sheet scraper approach. It fetches financial announcements from the Australian Securities Exchange (ASX) and stores them in a SQLite database.

## Features
- ✅ Official ASX API integration
- ✅ Financial report filtering
- ✅ SQLite database with proper indexing
- ✅ Rate limiting and error handling
- ✅ Progress tracking
- ✅ PDF download capability
- ✅ Beta testing with major ASX companies

## Quick Start

### 1. Install Dependencies
```bash
pip install requests pandas tqdm
```

### 2. Run Beta Test
```bash
python asx_announcement_scraper.py --beta
```

### 3. Download PDFs
```bash
python asx_pdf_downloader.py --all
```

### 4. Check Statistics
```bash
python asx_pdf_downloader.py --stats
```

## Beta Test Companies
The beta test includes these major ASX companies:
- CBA (Commonwealth Bank)
- BHP (BHP Group)
- WBC (Westpac)
- ANZ (ANZ Banking)
- NAB (National Australia Bank)
- WES (Wesfarmers)
- CSL (CSL Limited)
- TLS (Telstra)
- WOW (Woolworths)
- RIO (Rio Tinto)

## File Structure
```
asx_scraper/
├── asx_announcement_scraper.py    # Main scraper
├── asx_database.py                # Database management
├── asx_pdf_downloader.py          # PDF download handler
├── asx_config.py                  # Configuration
├── ASX_stock_list.csv             # Stock list
├── requirements.txt                # Dependencies
├── asx_data/                       # Output directory
│   ├── pdfs/                       # Downloaded PDFs
│   └── asx_announcements.db        # SQLite database
└── README.md                       # This file
```

## Configuration
Edit `asx_config.py` to modify:
- Financial keywords for filtering
- Years to scrape
- Rate limiting settings
- Beta test tickers

## Database Schema
The scraper creates three main tables:
- `asx_announcements` - Main announcement data
- `asx_documents` - PDF document metadata
- `asx_financial_data` - Extracted financial data (future use)

## API Endpoint
Uses the official ASX API:
```
https://www.asx.com.au/asx/1/company/{ticker}/announcements?count=100
```

## Next Steps
1. Test with beta companies
2. Evaluate data quality
3. Expand to full ASX list
4. Add PDF processing capabilities
5. Integrate with existing NZX data

