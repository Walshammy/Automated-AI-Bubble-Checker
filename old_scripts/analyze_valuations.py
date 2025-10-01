#!/usr/bin/env python3
"""
Analyze the stock valuation dataset to identify improvement opportunities
"""

import pandas as pd
import numpy as np
from openpyxl import load_workbook
import os

def analyze_valuation_data():
    """Analyze the valuation data to identify issues and improvements"""
    
    excel_file = r"C:\Users\james\Downloads\Stock Valuation\stock_valuation_dataset.xlsx"
    
    if not os.path.exists(excel_file):
        print(f"Excel file not found: {excel_file}")
        return
    
    try:
        # Load the data
        df = pd.read_excel(excel_file, sheet_name='Valuation Data')
        
        print("=== STOCK VALUATION DATA ANALYSIS ===\n")
        print(f"Total records: {len(df)}")
        print(f"Unique tickers: {df['ticker'].nunique()}")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        
        # Get latest data for each ticker
        latest_data = df.groupby('ticker').last().reset_index()
        
        print(f"\nLatest data records: {len(latest_data)}")
        
        # Analyze each valuation method
        print("\n=== VALUATION METHOD ANALYSIS ===")
        
        # Peter Lynch Analysis
        print("\n1. PETER LYNCH VALUATION:")
        lynch_data = latest_data[latest_data['lynch_valuation_status'].notna()]
        print(f"   Records with data: {len(lynch_data)}/{len(latest_data)} ({len(lynch_data)/len(latest_data)*100:.1f}%)")
        
        if len(lynch_data) > 0:
            print(f"   Status distribution:")
            print(f"   - VERY UNDERVALUED: {len(lynch_data[lynch_data['lynch_valuation_status'] == 'VERY UNDERVALUED'])}")
            print(f"   - UNDERVALUED: {len(lynch_data[lynch_data['lynch_valuation_status'] == 'UNDERVALUED'])}")
            print(f"   - FAIRLY VALUED: {len(lynch_data[lynch_data['lynch_valuation_status'] == 'FAIRLY VALUED'])}")
            print(f"   - OVERVALUED: {len(lynch_data[lynch_data['lynch_valuation_status'] == 'OVERVALUED'])}")
            print(f"   - SIGNIFICANTLY OVERVALUED: {len(lynch_data[lynch_data['lynch_valuation_status'] == 'SIGNIFICANTLY OVERVALUED'])}")
            
            # Check for extreme values
            extreme_lynch = lynch_data[abs(lynch_data['lynch_delta_percentage']) > 1000]
            if len(extreme_lynch) > 0:
                print(f"   EXTREME VALUES (>1000%): {len(extreme_lynch)} stocks")
                for _, stock in extreme_lynch.head(5).iterrows():
                    print(f"      {stock['ticker']}: {stock['lynch_delta_percentage']:+.1f}%")
        
        # DCF Analysis
        print("\n2. DCF VALUATION:")
        dcf_data = latest_data[latest_data['dcf_valuation_status'].notna()]
        print(f"   Records with data: {len(dcf_data)}/{len(latest_data)} ({len(dcf_data)/len(latest_data)*100:.1f}%)")
        
        if len(dcf_data) > 0:
            print(f"   Status distribution:")
            print(f"   - SIGNIFICANTLY UNDERVALUED: {len(dcf_data[dcf_data['dcf_valuation_status'] == 'SIGNIFICANTLY UNDERVALUED'])}")
            print(f"   - UNDERVALUED: {len(dcf_data[dcf_data['dcf_valuation_status'] == 'UNDERVALUED'])}")
            print(f"   - FAIRLY VALUED: {len(dcf_data[dcf_data['dcf_valuation_status'] == 'FAIRLY VALUED'])}")
            print(f"   - OVERVALUED: {len(dcf_data[dcf_data['dcf_valuation_status'] == 'OVERVALUED'])}")
            print(f"   - SIGNIFICANTLY OVERVALUED: {len(dcf_data[dcf_data['dcf_valuation_status'] == 'SIGNIFICANTLY OVERVALUED'])}")
            
            # Check for extreme values
            extreme_dcf = dcf_data[abs(dcf_data['dcf_delta_percentage']) > 500]
            if len(extreme_dcf) > 0:
                print(f"   EXTREME VALUES (>500%): {len(extreme_dcf)} stocks")
                for _, stock in extreme_dcf.head(5).iterrows():
                    print(f"      {stock['ticker']}: {stock['dcf_delta_percentage']:+.1f}%")
        
        # Enhanced DCF Analysis
        print("\n3. ENHANCED DCF VALUATION:")
        enhanced_dcf_data = latest_data[latest_data['enhanced_dcf_status'].notna()]
        print(f"   Records with data: {len(enhanced_dcf_data)}/{len(latest_data)} ({len(enhanced_dcf_data)/len(latest_data)*100:.1f}%)")
        
        if len(enhanced_dcf_data) > 0:
            print(f"   Status distribution:")
            status_counts = enhanced_dcf_data['enhanced_dcf_status'].value_counts()
            for status, count in status_counts.items():
                print(f"   - {status}: {count}")
        
        # Relative Valuation Analysis
        print("\n4. RELATIVE VALUATION:")
        relative_data = latest_data[latest_data['relative_valuation_status'].notna()]
        print(f"   Records with data: {len(relative_data)}/{len(latest_data)} ({len(relative_data)/len(latest_data)*100:.1f}%)")
        
        if len(relative_data) > 0:
            print(f"   Status distribution:")
            status_counts = relative_data['relative_valuation_status'].value_counts()
            for status, count in status_counts.items():
                print(f"   - {status}: {count}")
        
        # Check for missing data patterns
        print("\n=== MISSING DATA ANALYSIS ===")
        
        missing_patterns = {}
        for col in ['lynch_valuation_status', 'dcf_valuation_status', 'munger_farm_assessment', 
                   'enhanced_dcf_status', 'relative_valuation_status', 'reverse_dcf_assessment',
                   'epv_assessment', 'rim_assessment']:
            if col in latest_data.columns:
                missing_count = latest_data[col].isna().sum()
                missing_patterns[col] = missing_count
                print(f"{col}: {missing_count}/{len(latest_data)} missing ({missing_count/len(latest_data)*100:.1f}%)")
        
        # Analyze specific issues
        print("\n=== POTENTIAL ISSUES IDENTIFIED ===")
        
        # Check for NaN values in key metrics
        nan_issues = []
        for col in ['current_price', 'free_cashflow', 'net_income', 'shares_outstanding']:
            if col in latest_data.columns:
                nan_count = latest_data[col].isna().sum()
                if nan_count > 0:
                    nan_issues.append(f"{col}: {nan_count} missing values")
        
        if nan_issues:
            print("Missing fundamental data:")
            for issue in nan_issues:
                print(f"  - {issue}")
        
        # Check for zero or negative values
        zero_issues = []
        for col in ['current_price', 'free_cashflow', 'net_income', 'shares_outstanding']:
            if col in latest_data.columns:
                zero_count = (latest_data[col] <= 0).sum()
                if zero_count > 0:
                    zero_issues.append(f"{col}: {zero_count} zero/negative values")
        
        if zero_issues:
            print("Zero/negative values:")
            for issue in zero_issues:
                print(f"  - {issue}")
        
        # Sample of problematic stocks
        print("\n=== SAMPLE PROBLEMATIC STOCKS ===")
        
        # Stocks with no valuation data
        no_data_stocks = latest_data[
            latest_data['lynch_valuation_status'].isna() & 
            latest_data['dcf_valuation_status'].isna() & 
            latest_data['munger_farm_assessment'].isna()
        ]
        
        if len(no_data_stocks) > 0:
            print(f"Stocks with no valuation data ({len(no_data_stocks)}):")
            for _, stock in no_data_stocks.head(10).iterrows():
                print(f"  - {stock['ticker']}: {stock.get('company_name', 'Unknown')}")
        
        # Stocks with extreme valuations
        extreme_valuations = latest_data[
            (abs(latest_data['lynch_delta_percentage']) > 1000) |
            (abs(latest_data['dcf_delta_percentage']) > 500)
        ]
        
        if len(extreme_valuations) > 0:
            print(f"\nStocks with extreme valuations ({len(extreme_valuations)}):")
            for _, stock in extreme_valuations.head(10).iterrows():
                lynch_delta = stock.get('lynch_delta_percentage', 0)
                dcf_delta = stock.get('dcf_delta_percentage', 0)
                print(f"  - {stock['ticker']}: Lynch {lynch_delta:+.1f}%, DCF {dcf_delta:+.1f}%")
        
        print("\n=== RECOMMENDATIONS ===")
        print("1. Check data quality for stocks with missing fundamental data")
        print("2. Review extreme valuations - may indicate data errors")
        print("3. Improve error handling for edge cases")
        print("4. Add data validation checks")
        print("5. Consider sector-specific adjustments")
        
    except Exception as e:
        print(f"Error analyzing data: {e}")

if __name__ == "__main__":
    analyze_valuation_data()
