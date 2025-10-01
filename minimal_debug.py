#!/usr/bin/env python3
"""
Simple test to debug the focus_stocks issue - minimal version
"""

def test_minimal_init():
    """Test minimal initialization to find the issue"""
    try:
        print("Importing StockValuationScraper...")
        from stock_valuation_scraper import StockValuationScraper
        print("Import successful")
        
        print("Creating instance...")
        scraper = StockValuationScraper()
        print("Instance created")
        
        print(f"Has focus_stocks: {hasattr(scraper, 'focus_stocks')}")
        if hasattr(scraper, 'focus_stocks'):
            print(f"Focus stocks length: {len(scraper.focus_stocks)}")
        else:
            print("focus_stocks not found")
            
        return True
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_minimal_init()
