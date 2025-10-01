#!/usr/bin/env python3
"""
Test script for historical data collection functionality
This script demonstrates how to use the new historical data collection features
"""

from stock_valuation_scraper import StockValuationScraper
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_single_stock_historical():
    """Test historical data collection for a single stock"""
    logger.info("Testing single stock historical data collection...")
    
    scraper = StockValuationScraper()
    
    # Test with FPH.NZ (Fisher & Paykel Healthcare)
    ticker = 'FPH.NZ'
    logger.info(f"Collecting historical data for {ticker}")
    
    historical_data = scraper.collect_comprehensive_historical_data(ticker)
    
    if historical_data:
        logger.info(f"Successfully collected data for {ticker}")
        logger.info(f"Data range: {historical_data['data_start_date']} to {historical_data['data_end_date']}")
        logger.info(f"Total trading days: {historical_data['total_days']}")
        logger.info(f"Current price: ${historical_data['current_price']:.2f}")
        logger.info(f"Price change 1Y: {historical_data['price_change_1y']:.2f}%")
        logger.info(f"Price percentile (all time): {historical_data['price_percentile_all']:.1f}%")
        logger.info(f"CAGR (all time): {historical_data['cagr_all']:.2f}%")
        logger.info(f"Max drawdown: {historical_data['max_drawdown']:.2f}%")
        logger.info(f"Sharpe ratio: {historical_data['sharpe_ratio']:.2f}")
        
        # Show some quarterly data
        if historical_data['quarterly_data']:
            logger.info(f"Quarterly data points: {len(historical_data['quarterly_data'])}")
            latest_quarter = historical_data['quarterly_data'][-1]
            logger.info(f"Latest quarter: {latest_quarter['quarter']}")
            logger.info(f"Latest revenue: ${latest_quarter['revenue']:,.0f}" if latest_quarter['revenue'] else "No revenue data")
        
        # Show P/E ratio data
        if historical_data['historical_pe_ratios']:
            pe_data = historical_data['historical_pe_ratios']
            logger.info(f"P/E ratio data points: {len(pe_data)}")
            
            # Find summary data
            summary_data = None
            for item in pe_data:
                if item['date'] == 'SUMMARY':
                    summary_data = item['summary_stats']
                    break
            
            if summary_data:
                logger.info(f"Current P/E: {summary_data['current_pe']:.2f}")
                logger.info(f"P/E Mean: {summary_data['pe_mean']:.2f}")
                logger.info(f"P/E Median: {summary_data['pe_median']:.2f}")
                logger.info(f"P/E Min: {summary_data['pe_min']:.2f}")
                logger.info(f"P/E Max: {summary_data['pe_max']:.2f}")
                logger.info(f"P/E Percentile (current): {summary_data['current_pe_percentile']:.1f}%")
                logger.info(f"Valuation Assessment: {summary_data['valuation_assessment']}")
        
        # Show data structure info
        logger.info(f"Data structure:")
        logger.info(f"  - Daily comprehensive records: {len(historical_data['dates'])}")
        logger.info(f"  - Quarterly financial records: {len(historical_data['quarterly_data'])}")
        logger.info(f"  - Annual financial records: {len(historical_data['annual_data'])}")
        logger.info(f"  - P/E ratio records: {len([p for p in historical_data['historical_pe_ratios'] if p['date'] != 'SUMMARY'])}")
        
        return True
    else:
        logger.error(f"Failed to collect historical data for {ticker}")
        return False

def test_historical_analysis():
    """Test historical trend analysis"""
    logger.info("Testing historical trend analysis...")
    
    scraper = StockValuationScraper()
    
    # Test analysis for FPH.NZ
    ticker = 'FPH.NZ'
    analysis = scraper.analyze_historical_trends(ticker)
    
    if analysis and 'error' not in analysis:
        logger.info(f"Analysis for {ticker}:")
        logger.info(f"  Current price: ${analysis['current_price']:.2f}")
        logger.info(f"  Data range: {analysis['data_start_date']} to {analysis['data_end_date']}")
        logger.info(f"  Total trading days: {analysis['total_trading_days']}")
        logger.info(f"  Price percentile (1Y): {analysis['price_percentile_1y']:.1f}%")
        logger.info(f"  Price percentile (5Y): {analysis['price_percentile_5y']:.1f}%")
        logger.info(f"  Price percentile (all): {analysis['price_percentile_all']:.1f}%")
        logger.info(f"  Total return: {analysis['total_return']:.2f}%")
        logger.info(f"  Annualized return: {analysis['annualized_return']:.2f}%")
        logger.info(f"  Max drawdown: {analysis['max_drawdown']:.2f}%")
        logger.info(f"  Volatility: {analysis['volatility']:.2f}%")
        logger.info(f"  Above 200d MA: {analysis['above_200d_ma']}")
        logger.info(f"  Above 50d MA: {analysis['above_50d_ma']}")
        logger.info(f"  MA Trend: {analysis['ma_trend']}")
        return True
    else:
        logger.error(f"Analysis failed for {ticker}: {analysis.get('error', 'Unknown error')}")
        return False

def test_progress_tracking():
    """Test progress tracking functionality"""
    logger.info("Testing progress tracking...")
    
    scraper = StockValuationScraper()
    
    # Load progress
    progress = scraper._load_historical_progress()
    logger.info(f"Current progress: {len(progress['completed'])} completed, {len(progress['failed'])} failed")
    
    if progress['completed']:
        logger.info(f"Completed stocks: {progress['completed'][:5]}...")  # Show first 5
    if progress['failed']:
        logger.info(f"Failed stocks: {progress['failed'][:5]}...")  # Show first 5
    
    return True

def main():
    """Run all tests"""
    logger.info("Starting historical data collection tests...")
    
    # Test 1: Single stock historical collection
    logger.info("\n" + "="*50)
    logger.info("TEST 1: Single Stock Historical Collection")
    logger.info("="*50)
    test1_success = test_single_stock_historical()
    
    # Test 2: Historical analysis
    logger.info("\n" + "="*50)
    logger.info("TEST 2: Historical Trend Analysis")
    logger.info("="*50)
    test2_success = test_historical_analysis()
    
    # Test 3: Progress tracking
    logger.info("\n" + "="*50)
    logger.info("TEST 3: Progress Tracking")
    logger.info("="*50)
    test3_success = test_progress_tracking()
    
    # Summary
    logger.info("\n" + "="*50)
    logger.info("TEST SUMMARY")
    logger.info("="*50)
    logger.info(f"Test 1 (Historical Collection): {'PASSED' if test1_success else 'FAILED'}")
    logger.info(f"Test 2 (Trend Analysis): {'PASSED' if test2_success else 'FAILED'}")
    logger.info(f"Test 3 (Progress Tracking): {'PASSED' if test3_success else 'FAILED'}")
    
    if all([test1_success, test2_success, test3_success]):
        logger.info("All tests PASSED! Historical data collection is working correctly.")
    else:
        logger.warning("Some tests FAILED. Check the logs above for details.")
    
    logger.info("\nTo run full historical collection for all stocks:")
    logger.info("python stock_valuation_scraper.py historical")
    
    logger.info("\nTo analyze trends for a specific stock:")
    logger.info("python stock_valuation_scraper.py analyze FPH.NZ")

if __name__ == "__main__":
    main()
