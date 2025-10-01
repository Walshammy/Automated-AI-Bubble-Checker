import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from stock_valuation_scraper import StockValuationScraper

def create_fresh_excel():
    """Create a fresh Excel file with all new valuation methods"""
    try:
        print("=== CREATING FRESH EXCEL FILE WITH NEW VALUATION METHODS ===")
        
        # Create scraper instance
        scraper = StockValuationScraper()
        
        # Override focus_stocks to test with a smaller set
        test_stocks = {
            'NVDA': 'NVIDIA Corporation',
            'AAPL': 'Apple Inc.',
            'MSFT': 'Microsoft Corporation',
            'GOOGL': 'Alphabet Inc.',
            'AMZN': 'Amazon.com Inc.',
            'META': 'Meta Platforms',
            'TSM': 'Taiwan Semiconductor Manufacturing',
            'NVO': 'Novo Nordisk',
            'BRK-B': 'Berkshire Hathaway Class B',
            'LMT': 'Lockheed Martin Corporation'
        }
        
        # Temporarily replace focus_stocks
        original_focus_stocks = scraper.focus_stocks
        scraper.focus_stocks = test_stocks
        
        print(f"Testing with {len(test_stocks)} stocks:")
        for ticker, name in test_stocks.items():
            print(f"  {ticker}: {name}")
        
        # Run the analysis
        scraper.run()
        
        # Restore original focus_stocks
        scraper.focus_stocks = original_focus_stocks
        
        print("\n=== FRESH EXCEL FILE CREATED ===")
        print("The new Excel file should now include all 9 valuation methods:")
        print("1. Peter Lynch")
        print("2. DCF Valuation") 
        print("3. Munger Farm")
        print("4. Enhanced DCF (Tier 1)")
        print("5. Relative Valuation (Tier 1)")
        print("6. Reverse DCF (Tier 2)")
        print("7. EPV/RIM (Tier 2)")
        print("8. Current Price")
        print("9. Additional metrics")
        
        return True
        
    except Exception as e:
        print(f"Error creating fresh Excel file: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    create_fresh_excel()
