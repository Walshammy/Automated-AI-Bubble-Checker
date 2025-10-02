#!/usr/bin/env python3
"""
Data Addition Test Results Analysis
Analyzes what additional data we can collect for serious analysis
"""

import sqlite3
import os
from datetime import datetime

def analyze_test_results():
    """Analyze the test results to understand data availability"""
    
    db_path = 'db_additions_test/test_data.db'
    
    if not os.path.exists(db_path):
        print("Test database not found!")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 80)
    print("DATA ADDITION TEST RESULTS ANALYSIS")
    print("=" * 80)
    
    # Get test results
    cursor.execute('''
        SELECT test_type, ticker, success, records_collected, error_message
        FROM test_results 
        ORDER BY test_type, ticker
    ''')
    results = cursor.fetchall()
    
    print(f"\nDETAILED TEST RESULTS:")
    print("-" * 50)
    
    current_test = None
    for test_type, ticker, success, records, error in results:
        if test_type != current_test:
            print(f"\n{test_type.upper()}:")
            current_test = test_type
        
        status = "+" if success else "-"
        if success:
            print(f"  {status} {ticker}: {records} records")
        else:
            print(f"  {status} {ticker}: {error}")
    
    # Coverage analysis
    print(f"\nCOVERAGE ANALYSIS:")
    print("-" * 50)
    
    # Quarterly financials coverage
    cursor.execute('''
        SELECT ticker, COUNT(*) as quarters
        FROM quarterly_financials 
        GROUP BY ticker 
        ORDER BY quarters DESC
    ''')
    qf_coverage = cursor.fetchall()
    
    print(f"\nQuarterly Financials Coverage:")
    us_coverage = 0
    asx_coverage = 0
    nzx_coverage = 0
    
    for ticker, quarters in qf_coverage:
        print(f"  {ticker}: {quarters} quarters")
        if '.AX' in ticker:
            asx_coverage += 1
        elif '.NZ' in ticker:
            nzx_coverage += 1
        else:
            us_coverage += 1
    
    print(f"\nCoverage Summary:")
    print(f"  US stocks: {us_coverage}/3 ({us_coverage/3*100:.1f}%)")
    print(f"  ASX stocks: {asx_coverage}/3 ({asx_coverage/3*100:.1f}%)")
    print(f"  NZX stocks: {nzx_coverage}/3 ({nzx_coverage/3*100:.1f}%)")
    
    # Corporate actions coverage
    cursor.execute('''
        SELECT ticker, COUNT(*) as actions
        FROM corporate_actions 
        GROUP BY ticker 
        ORDER BY actions DESC
    ''')
    ca_coverage = cursor.fetchall()
    
    print(f"\nCorporate Actions Coverage:")
    for ticker, actions in ca_coverage:
        print(f"  {ticker}: {actions} actions")
    
    # Market data coverage
    cursor.execute('''
        SELECT index_name, COUNT(*) as records
        FROM market_data 
        GROUP BY index_name 
        ORDER BY records DESC
    ''')
    md_coverage = cursor.fetchall()
    
    print(f"\nMarket Data Coverage:")
    for index_name, records in md_coverage:
        print(f"  {index_name}: {records} records")
    
    conn.close()
    
    # Recommendations
    print(f"\n" + "=" * 80)
    print(f"RECOMMENDATIONS FOR SERIOUS ANALYSIS")
    print(f"=" * 80)
    
    print(f"\n1. QUARTERLY FINANCIALS:")
    print(f"   + US stocks: Excellent coverage (5-6 quarters)")
    print(f"   - ASX stocks: Poor coverage (0 quarters)")
    print(f"   - NZX stocks: Very poor coverage (0-1 quarters)")
    print(f"   Recommendation: Use paid API for ASX/NZX quarterly data")
    
    print(f"\n2. CORPORATE ACTIONS:")
    print(f"   + All exchanges: Good coverage (35-96 actions per stock)")
    print(f"   + Includes: Dividends, stock splits")
    print(f"   Recommendation: This data is valuable and available")
    
    print(f"\n3. MARKET DATA:")
    print(f"   + Major indices: Good coverage (12-13 records each)")
    print(f"   + Includes: S&P 500, ASX 200, NZX 50, NASDAQ")
    print(f"   Recommendation: Expand to daily data for better analysis")
    
    print(f"\n4. DATA STRATEGY:")
    print(f"   Phase 1: Collect corporate actions for all stocks (high value)")
    print(f"   Phase 2: Collect market data daily for 5+ years")
    print(f"   Phase 3: Use paid API for ASX/NZX quarterly financials")
    print(f"   Phase 4: Add US quarterly financials to main database")
    
    print(f"\n5. DATABASE IMPACT:")
    print(f"   Corporate actions: ~500K records (+50MB)")
    print(f"   Market data: ~50K records (+10MB)")
    print(f"   Quarterly financials: ~200K records (+100MB)")
    print(f"   Total addition: ~750K records (+160MB)")
    
    print(f"\n6. NEXT STEPS:")
    print(f"   1. Implement corporate actions collection")
    print(f"   2. Add market data collection")
    print(f"   3. Test paid API for ASX/NZX quarterly data")
    print(f"   4. Create historical P/E calculation system")
    
    print(f"=" * 80)

if __name__ == "__main__":
    analyze_test_results()
