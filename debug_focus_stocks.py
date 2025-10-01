#!/usr/bin/env python3
"""
Simple test to debug the focus_stocks issue
"""

from stock_valuation_scraper import StockValuationScraper

def test_focus_stocks():
    """Test if focus_stocks is properly initialized"""
    try:
        print("Creating StockValuationScraper instance...")
        scraper = StockValuationScraper()
        
        print("Instance created successfully")
        print(f"Has focus_stocks attribute: {hasattr(scraper, 'focus_stocks')}")
        
        if hasattr(scraper, 'focus_stocks'):
            print(f"Focus stocks type: {type(scraper.focus_stocks)}")
            print(f"Focus stocks length: {len(scraper.focus_stocks)}")
            print(f"First few stocks: {list(scraper.focus_stocks.items())[:3]}")
            
            # Test if FPH.NZ is in focus_stocks
            if 'FPH.NZ' in scraper.focus_stocks:
                print(f"FPH.NZ found: {scraper.focus_stocks['FPH.NZ']}")
            else:
                print("FPH.NZ NOT found in focus_stocks")
        else:
            print("focus_stocks attribute not found")
            print(f"Available attributes: {[attr for attr in dir(scraper) if not attr.startswith('_')]}")
        
        return True
    except Exception as e:
        print(f"Error during initialization: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_init_step_by_step():
    """Test initialization step by step"""
    print("Testing initialization step by step...")
    
    # Test just the basic imports and class definition
    try:
        from stock_valuation_scraper import StockValuationScraper
        print("✓ Import successful")
        
        # Create instance with minimal initialization
        scraper = StockValuationScraper.__new__(StockValuationScraper)
        print("✓ Instance creation successful")
        
        # Test if we can access the class attributes
        print(f"Class has focus_stocks: {hasattr(StockValuationScraper, 'focus_stocks')}")
        
        return True
    except Exception as e:
        print(f"Error in step-by-step test: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_focus_stocks()
    print("\n" + "="*50)
    test_init_step_by_step()
