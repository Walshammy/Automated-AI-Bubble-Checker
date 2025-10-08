"""
Configuration for ASX Scraper
Beta version for testing ASX announcement scraping
"""
import os

# API Configuration - Updated based on research
ASX_API_BASE = "https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode={ticker}"
ASX_ANNOUNCEMENTS_URL = "https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode={ticker}"
DEFAULT_COUNT = 100  # Number of announcements to fetch per company

# File Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'asx_data')
PDF_DIR = os.path.join(DATA_DIR, 'pdfs')
DATABASE_PATH = os.path.join(DATA_DIR, 'asx_announcements.db')
STOCK_LIST_PATH = os.path.join(BASE_DIR, 'ASX_stock_list.csv')

# Create directories
os.makedirs(PDF_DIR, exist_ok=True)

# Financial Report Keywords (enhanced from NZX scraper)
FINANCIAL_KEYWORDS = [
    'annual report',
    'financial report', 
    'balance sheet',
    'half year',
    'full year',
    'quarterly report',
    'financial results',
    'preliminary final report',
    'appendix 4e',
    'financial statements',
    'interim report',
    'audited',
    'unaudited',
    'consolidated',
    'profit',
    'revenue',
    'earnings',
    'cash flow',
    'dividend',
    'performance',
    'outlook',
    'guidance',
    'fy20', 'fy21', 'fy22', 'fy23', 'fy24', 'fy25', 'fy26'  # Fiscal year patterns
]

# Scraping Configuration
REQUEST_TIMEOUT = 30  # seconds
RATE_LIMIT_DELAY = 1  # seconds between requests
MAX_RETRIES = 3

# Years to scrape
YEARS_TO_SCRAPE = None  # None = all time, or set to number of years

# Beta testing configuration
BETA_TICKERS = [
    'CBA',  # Commonwealth Bank
    'BHP',  # BHP Group
    'WBC',  # Westpac Banking Corp
    'ANZ',  # ANZ Banking Group
    'NAB',  # National Australia Bank
    'WES',  # Wesfarmers
    'CSL',  # CSL Limited
    'TLS',  # Telstra
    'WOW',  # Woolworths Group
    'RIO'   # Rio Tinto
]

# Logging configuration
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_FILE = os.path.join(BASE_DIR, 'asx_scraper.log')
