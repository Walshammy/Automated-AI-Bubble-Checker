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
from enhanced_balance_sheet_scraper import EnhancedBalanceSheetScraper
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
            df = pd.read_sql_query(query, db.get_connection())
            
            export_path = Path(output_path) / f"balance_sheet_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(export_path, index=False)
            print(f"Exported {len(df)} records to {export_path}")
            
        elif export_format == 'excel':
            # Export to Excel with multiple sheets
            with pd.ExcelWriter(f"{output_path}/balance_sheet_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx") as writer:
                
                # Balance sheet data
                query = "SELECT * FROM balance_sheet_data ORDER BY ticker, report_date DESC"
                df = pd.read_sql_query(query, db.get_connection())
                df.to_excel(writer, sheet_name='Balance_Sheet_Data', index=False)
                
                # Summary by ticker
                summary_df = db.get_tickers_with_financial_data()
                summary_df.to_excel(writer, sheet_name='Summary_by_Ticker', index=False)
                
                # Announcements
                query = "SELECT * FROM financial_announcements ORDER BY announcement_date DESC"
                ann_df = pd.read_sql_query(query, db.get_connection())
                ann_df.to_excel(writer, sheet_name='Financial_Announcements', index=False)
            
            print(f"Exported comprehensive data to Excel file")
            
        elif export_format == 'json':
            # Export as JSON
            query = "SELECT * FROM balance_sheet_data ORDER BY ticker, report_date DESC"
            df = pd.read_sql_query(query, db.get_connection())
            
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

def main():
    """Main orchestrator function"""
    args = parse_arguments()
    
    print_banner()
    
    # Initialize scraper
    scraper = EnhancedBalanceSheetScraper(base_dir=args.output_dir)
    
    # If only showing stats, do that and exit
    if args.database_stats:
        show_database_stats()
        return
    
    # Handle resume functionality
    if args.resume:
        completed_tickers = get_completed_tickers()
        if completed_tickers:
            print(f"Found {len(completed_tickers)} already processed tickers: {sorted(completed_tickers)}")
            
            if args.tickers:
                # Remove already completed tickers from the list
                args.tickers = [t for t in args.tickers if t not in completed_tickers]
                if not args.tickers:
                    print("All specified tickers have been processed!")
                    return
                print(f"Processing remaining tickers: {args.tickers}")
    
    # Get tickers to process
    if args.tickers:
        tickers_to_process = args.tickers
        logging.info(f"Processing specified tickers: {tickers_to_process}")
    else:
        # Get all NZSX companies
        companies_df = scraper.get_nzsx_companies()
        tickers_to_process = companies_df['ticker'].tolist()
        
        if args.resume:
            # Remove completed tickers
            completed_tickers = get_completed_tickers()
            tickers_to_process = [t for t in tickers_to_process if t not in completed_tickers]
            
        logging.info(f"Processing all {len(tickers_to_process)} NZSX companies")
    
    if not tickers_to_process:
        print("No tickers to process!")
        return
    
    print(f"Processing {len(tickers_to_process)} tickers")
    print(f"Years to retrieve: {args.years}")
    print(f"Exchange: {args.exchange}")
    
    if args.dry_run:
        print("DRY RUN MODE - No actual downloads will occur")
    
    # Process each ticker
    successful = 0
    failed = 0
    results = []
    
    for i, ticker in enumerate(tickers_to_process, 1):
        try:
            logging.info(f"\n{'='*60}")
            logging.info(f"Processing {ticker} ({i}/{len(tickers_to_process)})")
            logging.info(f"{'='*60}")
            
            if args.dry_run:
                logging.info(f"DRY RUN: Would process {ticker}")
                continue
            
            # Process the ticker
            ticker_results = scraper.process_company_financials(ticker, years_back=args.years)
            
            if ticker_results:
                results.extend(ticker_results)
                successful += 1
                logging.info(f"✓ Successfully processed {ticker}: {len(ticker_results)} records")
            else:
                logging.warning(f"✗ No data extracted for {ticker}")
                failed += 1
            
            # Progress update
            if i % 5 == 0:
                logging.info(f"Progress: {i}/{len(tickers_to_process)} completed. Success: {successful}, Failed: {failed}")
            
            # Small delay to be respectful
            import time
            time.sleep(2)
            
        except Exception as e:
            logging.error(f"Error processing {ticker}: {e}")
            failed += 1
    
    # Final summary
    print(f"\n{'='*80}")
    print("SCRAPING COMPLETE")
    print(f"{'='*80}")
    print(f"Total Processed: {successful + failed}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Success Rate: {successful/(successful + failed)*100:.1f}%" if (successful + failed) > 0 else "0%")
    
    if results:
        print(f"Total Records Extracted: {len(results)}")
        
        # Show top performers
        ticker_counts = {}
        for result in results:
            ticker = result['ticker']
            ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
        
        print(f"\nTop 5 Companies by Records:")
        for ticker, count in sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {ticker}: {count} records")
    
    # Export if requested
    if args.export:
        export_results(args.export, args.output_dir)
    
    # Update database stats
    print("\nUpdated Database Statistics:")
    show_database_stats()
    
    logging.info(f"Balance sheet scraping completed at {datetime.now()}")

if __name__ == "__main__":
    main()
