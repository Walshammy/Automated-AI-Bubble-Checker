"""
Main Balance Sheet Scraper Orchestrator
Main entry point for the balance sheet scraping system
"""

import argparse
import logging
from datetime import datetime
import pandas as pd
from pathlib import Path
import sys

# Import our modules
from balance_sheet_database import BalanceSheetDatabase

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('balance_sheet_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Enhanced Balance Sheet Scraper for NZSX Companies')
    
    parser.add_argument('--tickers', '-t', nargs='+', default=None,
                        help='Specific ticker symbols to process (default: all NZSX companies)')
    
    parser.add_argument('--exchange', '-e', default='NZX',
                        choices=['NZX', 'ASX'], help='Exchange to scrape (default: NZX)')
    
    parser.add_argument('--years', '-y', type=int, default=3,
                        help='Number of years of data to retrieve (default: 3)')
    
    parser.add_argument('--dry-run', action='store_true',
                        help='Perform a dry run without downloading PDFs')
    
    parser.add_argument('--resume', action='store_true',
                        help='Resume from tickers already processed')
    
    parser.add_argument('--database-stats', action='store_true',
                        help='Show database statistics only')
    
    parser.add_argument('--process-existing', action='store_true',
                        help='Process existing PDFs instead of scraping web')
    
    parser.add_argument('--export', choices=['csv', 'excel', 'json'], default=None,
                        help='Export results to file')
    
    parser.add_argument('--output-dir', default='./balance_sheet_data',
                        help='Output directory for results (default: ./balance_sheet_data)')
    
    return parser.parse_args()

def print_banner():
    """Print application banner"""
    print("=" * 80)
    print("ENHANCED BALANCE SHEET SCRAPER")
    print("Advanced Financial Data Extraction System")
    print("=" * 80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

def show_database_stats():
    """Show comprehensive database statistics"""
    db = BalanceSheetDatabase()
    stats = db.get_database_stats()
    
    print("\nDATABASE STATISTICS")
    print("-" * 50)
    
    if not stats:
        print("Error retrieving database statistics")
        return
    
    # Basic stats
    print(f"Total Announcements: {stats.get('total_announcements', 0):,}")
    print(f"Total Documents: {stats.get('total_documents', 0):,}")
    print(f"Balance Sheet Records: {stats.get('total_balance_sheet_records', 0):,}")
    print(f"Unique Companies: {stats.get('unique_tickers', 0):,}")
    print(f"Last Scraped: {stats.get('last_scraped', 'Never')}")
    
    # Get tickers with data
    tickers_df = db.get_tickers_with_financial_data()
    if not tickers_df.empty:
        print(f"\nTop 10 Companies by Record Count:")
        top_10 = tickers_df.head(10)
        for _, row in top_10.iterrows():
            print(f"  {row['ticker']}: {row['report_count']} reports (latest: {row['latest_report']})")
    
    return stats

def export_results(export_format, output_path):
    """Export collected results"""
    db = BalanceSheetDatabase()
    
    try:
        if export_format == 'csv':
            # Export all balance sheet data
            query = "SELECT * FROM balance_sheet_data ORDER BY ticker, report_date DESC"
            df = pd.read_sql_query(query, sqlite3.connect(db.db_path))
            
            export_path = Path(output_path) / f"balance_sheet_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(export_path, index=False)
            print(f"Exported {len(df)} records to {export_path}")
            
        elif export_format == 'excel':
            # Export to Excel with multiple sheets
            with pd.ExcelWriter(f"{output_path}/balance_sheet_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx") as writer:
                
                # Balance sheet data
                import sqlite3
                conn = sqlite3.connect(db.db_path)
                query = "SELECT * FROM balance_sheet_data ORDER BY ticker, report_date DESC"
                df = pd.read_sql_query(query, conn)
                df.to_excel(writer, sheet_name='Balance_Sheet_Data', index=False)
                
                # Summary by ticker
                summary_df = db.get_tickers_with_financial_data()
                summary_df.to_excel(writer, sheet_name='Summary_by_Ticker', index=False)
                
                # Announcements
                query = "SELECT * FROM financial_announcements ORDER BY announcement_date DESC"
                ann_df = pd.read_sql_query(query, sqlite3.connect(db.db_path))
                ann_df.to_excel(writer, sheet_name='Financial_Announcements', index=False)
            
            print(f"Exported comprehensive data to Excel file")
            
        elif export_format == 'json':
            # Export as JSON
            query = "SELECT * FROM balance_sheet_data ORDER BY ticker, report_date DESC"
            df = pd.read_sql_query(query, sqlite3.connect(db.db_path))
            
            export_path = Path(output_path) / f"balance_sheet_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            df.to_json(export_path, orient='records', indent=2, date_format='iso')
            print(f"Exported {len(df)} records to {export_path}")
    
    except Exception as e:
        logging.error(f"Export error: {e}")
        print(f"Export failed: {e}")

def get_completed_tickers():
    """Get list of already processed tickers"""
    db = BalanceSheetDatabase()
    completed_df = db.get_tickers_with_financial_data()
    
    if completed_df.empty:
        return set()
    
    return set(completed_df['ticker'].unique())

def process_existing_pdfs(output_dir):
    """Process all existing PDFs in the pdfs directory"""
    
    print("PROCESSING EXISTING PDFs")
    print("=" * 60)
    
    # Initialize database and processor
    db = BalanceSheetDatabase()
    from balance_sheet_processor import FinancialStatementProcessor
    processor = FinancialStatementProcessor()
    
    # Get the PDFs directory
    pdfs_dir = Path(output_dir) / "pdfs"
    
    if not pdfs_dir.exists():
        print(f"PDFs directory not found: {pdfs_dir}")
        return []
    
    # Get all company directories
    company_dirs = [d for d in pdfs_dir.iterdir() if d.is_dir()]
    
    print(f"Found {len(company_dirs)} companies with PDFs")
    
    # Progress tracking
    total_companies = len(company_dirs)
    processed_companies = 0
    successful_companies = 0
    total_records_extracted = 0
    failed_companies = []
    results = []
    
    start_time = datetime.now()
    
    for i, company_dir in enumerate(company_dirs, 1):
        ticker = company_dir.name
        print(f"\n[{i}/{total_companies}] Processing {ticker}...")
        print("-" * 50)
        
        try:
            # Get all PDF files in this company's directory
            pdf_files = list(company_dir.glob("*.pdf"))
            
            if not pdf_files:
                print(f"   [WARN] No PDF files found in {ticker} directory")
                continue
            
            print(f"   [INFO] Found {len(pdf_files)} PDF files")
            
            company_records = 0
            
            for j, pdf_file in enumerate(pdf_files, 1):
                print(f"   [PROC] Processing PDF {j}/{len(pdf_files)}: {pdf_file.name[:50]}...")
                
                try:
                    # Extract financial data from PDF
                    financial_data = processor.extract_comprehensive_financial_data(
                        pdf_path=pdf_file,
                        ticker=ticker,
                        announcement_id=f"{ticker}_{pdf_file.stem}",
                        report_date=None,  # Will be extracted from PDF
                        report_type="PDF"  # Default type
                    )
                    
                    if financial_data:
                        # Insert into database
                        success = db.insert_balance_sheet_data(financial_data)
                        if success:
                            company_records += 1
                            total_records_extracted += 1
                            results.append(financial_data)
                            print(f"      [OK] Financial data extracted successfully")
                            
                            # Show extracted data summary
                            if financial_data.get('revenue'):
                                print(f"         Revenue: ${financial_data['revenue']:,.0f}")
                            if financial_data.get('total_assets'):
                                print(f"         Total Assets: ${financial_data['total_assets']:,.0f}")
                            if financial_data.get('net_income'):
                                print(f"         Net Income: ${financial_data['net_income']:,.0f}")
                        else:
                            print(f"      [ERROR] Failed to store financial data in database")
                    else:
                        print(f"      [WARN] No financial data could be extracted from PDF")
                
                except Exception as e:
                    print(f"      [ERROR] Error processing PDF: {str(e)}")
                    logging.error(f"Error processing {pdf_file}: {e}")
                    continue
            
            if company_records > 0:
                successful_companies += 1
                print(f"   [SUCCESS] {ticker}: Successfully extracted {company_records} financial records")
            else:
                print(f"   [WARN] {ticker}: No financial records extracted")
            
            processed_companies += 1
            
            # Progress update every 5 companies
            if i % 5 == 0 or i == total_companies:
                elapsed_time = datetime.now() - start_time
                avg_time_per_company = elapsed_time.total_seconds() / i
                remaining_companies = total_companies - i
                estimated_remaining_time = remaining_companies * avg_time_per_company
                
                print(f"\n[PROGRESS] UPDATE ({i}/{total_companies})")
                print(f"   Successfully processed: {successful_companies}/{processed_companies}")
                print(f"   Total records extracted: {total_records_extracted:,}")
                print(f"   Elapsed time: {elapsed_time}")
                print(f"   Estimated remaining time: {estimated_remaining_time/60:.1f} minutes")
                print(f"   Success rate: {successful_companies/processed_companies*100:.1f}%")
            
            # Small delay between companies
            import time
            time.sleep(1)
            
        except Exception as e:
            failed_companies.append(ticker)
            print(f"[ERROR] {ticker}: Error - {str(e)}")
            logging.error(f"Error processing {ticker}: {e}")
            processed_companies += 1
            continue
    
    # Final summary
    end_time = datetime.now()
    total_time = end_time - start_time
    
    print(f"\n{'='*60}")
    print(f"PDF PROCESSING COMPLETE!")
    print(f"{'='*60}")
    print(f"Total companies processed: {processed_companies}/{total_companies}")
    print(f"Successful companies: {successful_companies}")
    print(f"Failed companies: {len(failed_companies)}")
    print(f"Total financial records extracted: {total_records_extracted:,}")
    print(f"Total processing time: {total_time}")
    print(f"Average time per company: {total_time.total_seconds()/processed_companies:.1f} seconds")
    print(f"Success rate: {successful_companies/processed_companies*100:.1f}%")
    
    if failed_companies:
        print(f"\nFailed companies: {', '.join(failed_companies)}")
    
    return results

def main():
    """Main orchestrator function"""
    args = parse_arguments()
    
    print_banner()
    
    # If only showing stats, do that and exit
    if args.database_stats:
        show_database_stats()
        return
    
    # If processing existing PDFs, do that and exit
    if args.process_existing:
        results = process_existing_pdfs(args.output_dir)
        
        # Export if requested
        if args.export:
            export_results(args.export, args.output_dir)
        
        # Update database stats
        print("\nUpdated Database Statistics:")
        show_database_stats()
        
        logging.info(f"PDF processing completed at {datetime.now()}")
        return
    
    # Web scraping is currently disabled due to NZX website changes
    print("Web scraping mode is currently disabled due to NZX website structure changes.")
    print("Please use --process-existing to process existing PDFs instead.")
    print("Or use --database-stats to view current data.")
    return

if __name__ == "__main__":
    main()
