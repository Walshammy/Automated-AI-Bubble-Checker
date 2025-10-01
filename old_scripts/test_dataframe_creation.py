import pandas as pd
import numpy as np

def test_dataframe_creation():
    """Test DataFrame creation with new valuation methods"""
    try:
        print("=== TESTING DATAFRAME CREATION ===")
        
        # Create sample valuation data with new methods
        sample_data = {
            'ticker': 'NVDA',
            'company_name': 'NVIDIA Corporation',
            'current_price': 186.58,
            'lynch_valuation_status': 'FAIRLY VALUED',
            'lynch_delta_percentage': 39.56,
            'dcf_valuation_status': 'SIGNIFICANTLY OVERVALUED',
            'dcf_delta_percentage': -78.29,
            'munger_7pct_assessment': 'STRONG SELL',
            'munger_7pct_delta_percentage': -71.82,
            # New methods
            'enhanced_dcf_status': 'SIGNIFICANTLY UNDERVALUED',
            'enhanced_dcf_delta': 2350.93,
            'enhanced_dcf_intrinsic_value': 4572.94,
            'relative_valuation_status': 'SIGNIFICANTLY OVERVALUED',
            'relative_valuation_delta': -55223234920647.48,
            'reverse_dcf_assessment': 'NO HISTORICAL GROWTH DATA',
            'reverse_dcf_implied_growth': 22.44,
            'epv_assessment': 'N/A',
            'epv_delta': 0,
            'rim_assessment': 'SIGNIFICANTLY OVERVALUED',
            'rim_delta': -99.99999999964054
        }
        
        print("Sample data keys:")
        for key in sample_data.keys():
            print(f"  {key}: {sample_data[key]}")
        
        # Create DataFrame
        df = pd.DataFrame([sample_data])
        print(f"\nDataFrame created with {len(df.columns)} columns")
        print(f"DataFrame shape: {df.shape}")
        
        print("\nDataFrame columns:")
        for i, col in enumerate(df.columns, 1):
            print(f"{i:2d}. {col}")
        
        # Check if new method columns are present
        new_method_columns = [
            'enhanced_dcf_status', 'enhanced_dcf_delta', 'enhanced_dcf_intrinsic_value',
            'relative_valuation_status', 'relative_valuation_delta',
            'reverse_dcf_assessment', 'reverse_dcf_implied_growth',
            'epv_assessment', 'epv_delta',
            'rim_assessment', 'rim_delta'
        ]
        
        print("\nNew method columns check:")
        for col in new_method_columns:
            if col in df.columns:
                print(f"✓ {col}: {df[col].iloc[0]}")
            else:
                print(f"✗ {col}: Not found")
        
        return True
        
    except Exception as e:
        print(f"Error testing DataFrame creation: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_dataframe_creation()
