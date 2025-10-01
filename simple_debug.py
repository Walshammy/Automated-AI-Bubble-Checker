#!/usr/bin/env python3
"""
Debug script to find exactly where initialization fails - no unicode
"""

def test_init_with_try_catch():
    """Test initialization with detailed error catching"""
    try:
        print("Step 1: Importing...")
        from stock_valuation_scraper import StockValuationScraper
        print("OK - Import successful")
        
        print("Step 2: Creating instance...")
        scraper = StockValuationScraper()
        print("OK - Instance created")
        
        print("Step 3: Checking attributes...")
        print(f"  - Has focus_stocks: {hasattr(scraper, 'focus_stocks')}")
        print(f"  - Has logger: {hasattr(scraper, 'logger')}")
        print(f"  - Has onedrive_dir: {hasattr(scraper, 'onedrive_dir')}")
        print(f"  - Has downloads_dir: {hasattr(scraper, 'downloads_dir')}")
        
        if hasattr(scraper, 'focus_stocks'):
            print(f"OK - Focus stocks length: {len(scraper.focus_stocks)}")
            print(f"OK - First stock: {list(scraper.focus_stocks.items())[0]}")
        else:
            print("ERROR - Focus stocks not found")
            
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_class_definition():
    """Test if the class definition itself has issues"""
    try:
        print("Testing class definition...")
        from stock_valuation_scraper import StockValuationScraper
        
        # Check if the class has the method
        print(f"Class has __init__: {hasattr(StockValuationScraper, '__init__')}")
        
        # Try to inspect the __init__ method
        import inspect
        init_source = inspect.getsource(StockValuationScraper.__init__)
        print(f"__init__ method length: {len(init_source)} characters")
        
        # Check if focus_stocks is mentioned in the source
        if 'focus_stocks' in init_source:
            print("OK - focus_stocks found in __init__ source")
        else:
            print("ERROR - focus_stocks NOT found in __init__ source")
            
        return True
        
    except Exception as e:
        print(f"Error in class definition test: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=== INITIALIZATION TEST ===")
    test_init_with_try_catch()
    
    print("\n=== CLASS DEFINITION TEST ===")
    test_class_definition()
