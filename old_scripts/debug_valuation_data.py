import pandas as pd
from openpyxl import load_workbook

def debug_valuation_data(excel_file):
    """Debug what's actually in the valuation data"""
    try:
        print("=== DEBUGGING VALUATION DATA ===")
        
        # Read the latest data
        df = pd.read_excel(excel_file, sheet_name='Valuation Data')
        latest_data = df.sort_values(by='timestamp').groupby('ticker').last().reset_index()
        
        print(f"Total records: {len(latest_data)}")
        print(f"Total columns: {len(latest_data.columns)}")
        
        # Check a specific stock to see what data it has
        sample_stock = latest_data[latest_data['ticker'] == 'NVDA'].iloc[0] if len(latest_data[latest_data['ticker'] == 'NVDA']) > 0 else latest_data.iloc[0]
        
        print(f"\nSample stock: {sample_stock['ticker']} ({sample_stock['company_name']})")
        print(f"Timestamp: {sample_stock['timestamp']}")
        
        # Check what valuation methods have data
        valuation_methods = {
            'Peter Lynch': ['lynch_valuation_status', 'lynch_delta_percentage'],
            'DCF': ['dcf_valuation_status', 'dcf_delta_percentage'],
            'Munger Farm': ['munger_7pct_assessment', 'munger_7pct_delta_percentage'],
            'Enhanced DCF': ['enhanced_dcf_status', 'enhanced_dcf_delta'],
            'Relative Valuation': ['relative_valuation_status', 'relative_valuation_delta'],
            'Reverse DCF': ['reverse_dcf_assessment', 'reverse_dcf_implied_growth'],
            'EPV': ['epv_assessment', 'epv_delta'],
            'RIM': ['rim_assessment', 'rim_delta']
        }
        
        print("\nValuation method data availability:")
        for method, columns in valuation_methods.items():
            available = 0
            for col in columns:
                if col in sample_stock and pd.notna(sample_stock[col]) and sample_stock[col] != 'N/A':
                    available += 1
            print(f"  {method}: {available}/{len(columns)} columns have data")
            
            # Show actual values for this method
            for col in columns:
                if col in sample_stock:
                    value = sample_stock[col]
                    print(f"    {col}: {value}")
        
        # Check if the new methods are being calculated at all
        print("\n=== CHECKING NEW METHOD CALCULATIONS ===")
        
        # Look for any records that have the new method data
        new_method_columns = [
            'enhanced_dcf_status', 'relative_valuation_status', 
            'reverse_dcf_assessment', 'epv_assessment', 'rim_assessment'
        ]
        
        for col in new_method_columns:
            if col in latest_data.columns:
                non_null_count = latest_data[col].notna().sum()
                non_n_a_count = (latest_data[col] != 'N/A').sum()
                print(f"{col}: {non_null_count} non-null, {non_n_a_count} not 'N/A'")
                
                # Show sample values
                sample_values = latest_data[col].dropna().head(3).tolist()
                print(f"  Sample values: {sample_values}")
            else:
                print(f"{col}: Column not found!")
        
        return True
        
    except Exception as e:
        print(f"Error debugging valuation data: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    excel_file_path = r"C:\Users\james\Downloads\Stock Valuation\stock_valuation_dataset.xlsx"
    debug_valuation_data(excel_file_path)
