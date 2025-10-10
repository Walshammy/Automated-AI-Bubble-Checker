"""
PDF Downloader for ASX Announcements
Downloads PDFs from ASX website
Based on the successful NZX approach with enhanced error handling
"""
import requests
import os
import time
import logging
from typing import Optional
from tqdm import tqdm
from pathlib import Path

import asx_config as config
from asx_database import ASXDatabase

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler()
    ]
)

class ASXPDFDownloader:
    """PDF downloader for ASX announcements - adapted from NZX approach"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = ASXDatabase()
        
        # Session setup with retry strategy
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/pdf,application/octet-stream,*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        
        # Rate limiting
        self.base_delay = config.RATE_LIMIT_DELAY
        self.max_delay = 10.0
        self.current_delay = self.base_delay
        self.consecutive_errors = 0
        
        self.logger.info("ASX PDF Downloader initialized")
    
    def download_pdf(self, url: str, filename: str, ticker: str) -> bool:
        """
        Download a single PDF
        
        Args:
            url: PDF URL
            filename: Filename to save as
            ticker: Company ticker for organization
        
        Returns:
            True if successful, False otherwise
        """
        # Create ticker-specific directory
        ticker_dir = Path(config.PDF_DIR) / ticker
        ticker_dir.mkdir(exist_ok=True)
        
        filepath = ticker_dir / filename
        
        # Skip if already exists
        if filepath.exists():
            self.logger.debug(f"PDF already exists: {filepath}")
            return True
        
        for attempt in range(config.MAX_RETRIES):
            try:
                self.logger.debug(f"Downloading {filename} (attempt {attempt + 1})")
                
                response = self.session.get(url, timeout=config.REQUEST_TIMEOUT, stream=True)
                response.raise_for_status()
                
                # Check if response is actually a PDF
                content_type = response.headers.get('content-type', '').lower()
                if 'pdf' not in content_type and 'application/octet-stream' not in content_type:
                    self.logger.warning(f"Unexpected content type for {filename}: {content_type}")
                
                # Download with progress
                total_size = int(response.headers.get('content-length', 0))
                
                with open(filepath, 'wb') as f:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                
                # Verify file was created and has content
                if filepath.exists() and filepath.stat().st_size > 0:
                    self.logger.debug(f"Successfully downloaded {filename} ({filepath.stat().st_size} bytes)")
                    return True
                else:
                    self.logger.error(f"Downloaded file is empty or doesn't exist: {filepath}")
                    if filepath.exists():
                        filepath.unlink()  # Remove empty file
                    return False
                
            except requests.exceptions.RequestException as e:
                if attempt < config.MAX_RETRIES - 1:
                    wait_time = 2 ** attempt
                    self.logger.warning(f"Download failed for {filename} (attempt {attempt + 1}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Failed to download {filename} after {config.MAX_RETRIES} attempts: {e}")
                    self.handle_rate_limiting()
                    return False
            except Exception as e:
                self.logger.error(f"Unexpected error downloading {filename}: {e}")
                return False
        
        return False
    
    def download_all_pending(self):
        """Download all pending financial reports"""
        pending = self.db.get_pending_downloads()
        
        if not pending:
            self.logger.info("No pending downloads found")
            return
        
        self.logger.info("=" * 60)
        self.logger.info("ASX PDF DOWNLOADER")
        self.logger.info("=" * 60)
        self.logger.info(f"Pending downloads: {len(pending)}")
        self.logger.info("=" * 60)
        
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        for ann in tqdm(pending, desc="Downloading PDFs"):
            filename = ann['pdf_filename']
            url = ann['url']
            ann_id = ann['announcement_id']
            ticker = ann['ticker']
            
            if not filename or not url:
                self.logger.warning(f"Skipping {ann_id}: missing filename or URL")
                self.db.update_download_status(ann_id, 'failed')
                skipped_count += 1
                continue
            
            if self.download_pdf(url, filename, ticker):
                self.db.update_download_status(ann_id, 'downloaded', filename)
                success_count += 1
            else:
                self.db.update_download_status(ann_id, 'failed')
                failed_count += 1
            
            # Rate limiting
            time.sleep(self.current_delay)
        
        self.logger.info("=" * 60)
        self.logger.info("DOWNLOAD COMPLETE")
        self.logger.info("=" * 60)
        self.logger.info(f"Successful: {success_count}")
        self.logger.info(f"Failed: {failed_count}")
        self.logger.info(f"Skipped: {skipped_count}")
        self.logger.info(f"Success rate: {success_count/(success_count+failed_count)*100:.1f}%")
    
    def download_ticker_pdfs(self, ticker: str):
        """Download all PDFs for a specific ticker"""
        announcements = self.db.get_announcements_by_ticker(ticker)
        financial_announcements = [ann for ann in announcements if ann['is_financial_report']]
        
        if not financial_announcements:
            self.logger.info(f"No financial announcements found for {ticker}")
            return
        
        self.logger.info(f"Downloading {len(financial_announcements)} PDFs for {ticker}")
        
        success_count = 0
        failed_count = 0
        
        for ann in financial_announcements:
            filename = ann['pdf_filename']
            url = ann['url']
            ann_id = ann['announcement_id']
            
            if not filename or not url:
                self.logger.warning(f"Skipping {ann_id}: missing filename or URL")
                failed_count += 1
                continue
            
            if self.download_pdf(url, filename, ticker):
                self.db.update_download_status(ann_id, 'downloaded', filename)
                success_count += 1
            else:
                self.db.update_download_status(ann_id, 'failed')
                failed_count += 1
            
            time.sleep(self.current_delay)
        
        self.logger.info(f"Download complete for {ticker}: {success_count} successful, {failed_count} failed")
    
    def handle_rate_limiting(self):
        """Handle rate limiting by increasing delay"""
        self.consecutive_errors += 1
        self.current_delay = min(self.base_delay * (2 ** self.consecutive_errors), self.max_delay)
        self.logger.warning(f"Rate limiting detected. Increasing delay to {self.current_delay}s")
        time.sleep(self.current_delay)
    
    def get_download_statistics(self) -> dict:
        """Get download statistics"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT COUNT(*) FROM asx_announcements WHERE is_financial_report = 1')
            total_financial = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM asx_announcements WHERE download_status = "downloaded"')
            downloaded = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM asx_announcements WHERE download_status = "failed"')
            failed = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM asx_announcements WHERE download_status = "pending"')
            pending = cursor.fetchone()[0]
            
            return {
                'total_financial_reports': total_financial,
                'downloaded': downloaded,
                'failed': failed,
                'pending': pending,
                'success_rate': downloaded / total_financial * 100 if total_financial > 0 else 0
            }
        except Exception as e:
            self.logger.error(f"Error getting download statistics: {e}")
            return {}
        finally:
            conn.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ASX PDF Downloader - Beta Version')
    parser.add_argument('--all', action='store_true', default=True,
                        help='Download all pending PDFs (default)')
    parser.add_argument('--ticker', type=str, default=None,
                        help='Download PDFs for specific ticker')
    parser.add_argument('--stats', action='store_true',
                        help='Show download statistics only')
    
    args = parser.parse_args()
    
    downloader = ASXPDFDownloader()
    
    if args.stats:
        stats = downloader.get_download_statistics()
        print("Download Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
    elif args.ticker:
        downloader.download_ticker_pdfs(args.ticker.upper())
    else:
        downloader.download_all_pending()


if __name__ == "__main__":
    main()

