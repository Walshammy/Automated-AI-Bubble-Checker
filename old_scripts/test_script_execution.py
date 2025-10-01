import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from stock_valuation_scraper import StockValuationScraper
import pandas as pd

def test_script_execution():
    """Test the actual script execution with a few stocks"""
    try:
        print("=== TESTING SCRIPT EXECUTION ===")
        
        # Create scraper instance
        scraper = StockValuationScraper()
        
        # Test with just a few stocks
        test_stocks = ['NVDA', 'AAPL', 'MSFT']
        
        all_valuation_data = []
        
        for ticker in test_stocks:
            print(f"\n--- Processing {ticker} ---")
            
            # Get stock data
            stock_data = scraper.get_stock_data(ticker)
            if not stock_data:
                print(f"Failed to get data for {ticker}")
                continue
            
            # Get historical data
            historical_data = scraper.get_historical_financial_data(ticker)
            
            # Collect valuation metrics
            valuation_data = scraper.collect_valuation_metrics(ticker)
            
            if valuation_data:
                print(f"Collected {len(valuation_data)} metrics for {ticker}")
                
                # Check if new methods are in the data
                new_methods = [
                    'enhanced_dcf_status', 'relative_valuation_status', 
                    'reverse_dcf_assessment', 'epv_assessment', 'rim_assessment'
                ]
                
                print(f"New method data for {ticker}:")
                for method in new_methods:
                    if method in valuation_data:
                        print(f"  {method}: {valuation_data[method]}")
                    else:
                        print(f"  {method}: Not found")
                
                all_valuation_data.append(valuation_data)
        
        # Create DataFrame
        if all_valuation_data:
            df = pd.DataFrame(all_valuation_data)
            print(f"\nDataFrame created with {len(df.columns)} columns")
            
            # Check if new method columns are present
            new_method_columns = [
                'enhanced_dcf_status', 'enhanced_dcf_delta',
                'relative_valuation_status', 'relative_valuation_delta',
                'reverse_dcf_assessment', 'reverse_dcf_implied_growth',
                'epv_assessment', 'epv_delta',
                'rim_assessment', 'rim_delta'
            ]
            
            print("\nNew method columns in DataFrame:")
            for col in new_method_columns:
                if col in df.columns:
                    non_null_count = df[col].notna().sum()
                    print(f"  {col}: Present, {non_null_count} non-null values")
                else:
                    print(f"  {col}: Not found")
        
        return True
        
    except Exception as e:
        print(f"Error testing script execution: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_script_execution()
