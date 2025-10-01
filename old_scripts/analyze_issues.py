#!/usr/bin/env python3
"""
Focused analysis of valuation issues
"""

import pandas as pd
import numpy as np

def analyze_valuation_issues():
    """Analyze specific valuation issues"""
    
    excel_file = r"C:\Users\james\Downloads\Stock Valuation\stock_valuation_dataset.xlsx"
    
    try:
        # Load the data
        df = pd.read_excel(excel_file, sheet_name='Valuation Data')
        latest_data = df.groupby('ticker').last().reset_index()
        
        print("=== CRITICAL VALUATION ISSUES IDENTIFIED ===\n")
        
        # Issue 1: Peter Lynch showing extreme undervaluation
        print("1. PETER LYNCH EXTREME UNDERVALUATION:")
        extreme_lynch = latest_data[latest_data['lynch_delta_percentage'] > 1000]
        print(f"   {len(extreme_lynch)} stocks showing >1000% undervaluation")
        
        if len(extreme_lynch) > 0:
            print("   Sample extreme cases:")
            for _, stock in extreme_lynch.head(10).iterrows():
                lynch_ratio = stock.get('lynch_ratio', 0)
                pe_ratio = stock.get('pe_ratio', 0)
                print(f"   - {stock['ticker']}: Lynch Ratio {lynch_ratio:.2f}, P/E {pe_ratio:.2f}, Delta {stock['lynch_delta_percentage']:+.1f}%")
        
        # Issue 2: Check P/E ratios causing extreme Lynch ratios
        print("\n2. P/E RATIO ANALYSIS:")
        pe_issues = latest_data[
            (latest_data['pe_ratio'] < 1) | 
            (latest_data['pe_ratio'] > 100) |
            (latest_data['pe_ratio'].isna())
        ]
        print(f"   {len(pe_issues)} stocks with problematic P/E ratios")
        
        if len(pe_issues) > 0:
            print("   Problematic P/E ratios:")
            for _, stock in pe_issues.head(10).iterrows():
                pe = stock.get('pe_ratio', 0)
                eps = stock.get('eps', 0)
                current_price = stock.get('current_price', 0)
                print(f"   - {stock['ticker']}: P/E {pe:.2f}, EPS {eps:.4f}, Price ${current_price:.2f}")
        
        # Issue 3: DCF showing extreme values
        print("\n3. DCF EXTREME VALUES:")
        extreme_dcf = latest_data[
            (latest_data['dcf_delta_percentage'] > 500) | 
            (latest_data['dcf_delta_percentage'] < -100)
        ]
        print(f"   {len(extreme_dcf)} stocks with extreme DCF values")
        
        if len(extreme_dcf) > 0:
            print("   Sample extreme DCF cases:")
            for _, stock in extreme_dcf.head(10).iterrows():
                fcf = stock.get('free_cashflow', 0)
                shares = stock.get('shares_outstanding', 0)
                dcf_value = stock.get('dcf_intrinsic_value_per_share', 0)
                print(f"   - {stock['ticker']}: FCF ${fcf:,.0f}, Shares {shares:,.0f}, DCF Value ${dcf_value:.2f}, Delta {stock['dcf_delta_percentage']:+.1f}%")
        
        # Issue 4: Missing fundamental data
        print("\n4. MISSING FUNDAMENTAL DATA:")
        missing_data = latest_data[
            latest_data['free_cashflow'].isna() | 
            (latest_data['free_cashflow'] <= 0) |
            latest_data['shares_outstanding'].isna() |
            (latest_data['shares_outstanding'] <= 0)
        ]
        print(f"   {len(missing_data)} stocks with missing/zero fundamental data")
        
        if len(missing_data) > 0:
            print("   Stocks with missing data:")
            for _, stock in missing_data.head(10).iterrows():
                fcf = stock.get('free_cashflow', 0)
                shares = stock.get('shares_outstanding', 0)
                print(f"   - {stock['ticker']}: FCF ${fcf:,.0f}, Shares {shares:,.0f}")
        
        # Issue 5: Check for data quality issues
        print("\n5. DATA QUALITY ISSUES:")
        
        # Negative prices
        negative_prices = latest_data[latest_data['current_price'] <= 0]
        print(f"   Negative/zero prices: {len(negative_prices)}")
        
        # Negative earnings
        negative_earnings = latest_data[latest_data['net_income'] < 0]
        print(f"   Negative earnings: {len(negative_earnings)}")
        
        # Zero shares outstanding
        zero_shares = latest_data[latest_data['shares_outstanding'] <= 0]
        print(f"   Zero/negative shares: {len(zero_shares)}")
        
        # Issue 6: New valuation methods not working
        print("\n6. NEW VALUATION METHODS STATUS:")
        
        methods = ['enhanced_dcf_status', 'relative_valuation_status', 'reverse_dcf_assessment', 'epv_assessment', 'rim_assessment']
        for method in methods:
            if method in latest_data.columns:
                non_null = latest_data[method].notna().sum()
                print(f"   {method}: {non_null}/{len(latest_data)} ({non_null/len(latest_data)*100:.1f}%)")
            else:
                print(f"   {method}: Column not found")
        
        print("\n=== ROOT CAUSE ANALYSIS ===")
        print("1. Peter Lynch extreme undervaluation likely due to:")
        print("   - Very low P/E ratios (possibly negative earnings)")
        print("   - Lynch ratio calculation: (Growth Rate + Dividend Yield) / P/E")
        print("   - When P/E is very low, Lynch ratio becomes extremely high")
        
        print("\n2. DCF extreme values likely due to:")
        print("   - Missing or incorrect free cash flow data")
        print("   - Incorrect shares outstanding")
        print("   - Growth rate assumptions too aggressive")
        
        print("\n3. New valuation methods not working because:")
        print("   - Missing fundamental data (FCF, earnings, etc.)")
        print("   - Data validation issues")
        print("   - Method implementation errors")
        
        print("\n=== RECOMMENDED FIXES ===")
        print("1. Add data validation for P/E ratios")
        print("2. Cap extreme Lynch ratios (e.g., max 5x)")
        print("3. Improve free cash flow data collection")
        print("4. Add sanity checks for shares outstanding")
        print("5. Implement better error handling for edge cases")
        print("6. Add sector-specific adjustments")
        print("7. Review growth rate assumptions")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze_valuation_issues()
