import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from stock_valuation_scraper import StockValuationScraper
import yfinance as yf

def test_new_valuation_methods():
    """Test if the new valuation methods are working"""
    try:
        print("=== TESTING NEW VALUATION METHODS ===")
        
        # Create scraper instance
        scraper = StockValuationScraper()
        
        # Get data for a test stock (NVDA)
        ticker = "NVDA"
        print(f"Testing with {ticker}")
        
        # Get stock data
        stock_data = scraper.get_stock_data(ticker)
        if not stock_data:
            print(f"Failed to get data for {ticker}")
            return False
        
        print(f"Got stock data for {ticker}")
        print(f"Current price: ${stock_data.get('current_price', 'N/A')}")
        print(f"Free cash flow: ${stock_data.get('free_cashflow', 'N/A')}")
        print(f"Shares outstanding: {stock_data.get('shares_outstanding', 'N/A')}")
        
        # Test Enhanced DCF
        print("\n--- Testing Enhanced DCF ---")
        enhanced_dcf = scraper.calculate_enhanced_dcf_valuation(stock_data)
        print(f"Enhanced DCF result: {enhanced_dcf}")
        
        # Test Relative Valuation
        print("\n--- Testing Relative Valuation ---")
        relative_val = scraper.calculate_relative_valuation(stock_data)
        print(f"Relative Valuation result: {relative_val}")
        
        # Test Reverse DCF
        print("\n--- Testing Reverse DCF ---")
        reverse_dcf = scraper.calculate_reverse_dcf(stock_data)
        print(f"Reverse DCF result: {reverse_dcf}")
        
        # Test EPV
        print("\n--- Testing EPV ---")
        epv = scraper.calculate_earnings_power_value(stock_data)
        print(f"EPV result: {epv}")
        
        # Test RIM
        print("\n--- Testing RIM ---")
        rim = scraper.calculate_residual_income_model(stock_data)
        print(f"RIM result: {rim}")
        
        return True
        
    except Exception as e:
        print(f"Error testing new valuation methods: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_new_valuation_methods()
