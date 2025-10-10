#!/usr/bin/env python3
"""
ASX vs NZX Dataset Comparison
Compare the two datasets for completion, datapoints, and effectiveness
"""
import sqlite3
import pandas as pd
import os
from datetime import datetime

def compare_datasets():
    """Compare ASX and NZX datasets"""
    print("=" * 80)
    print("ASX vs NZX DATASET COMPARISON")
    print("=" * 80)
    
    # ASX Database
    asx_db_path = os.path.join('asx_scraper', 'asx_data', 'asx_announcements.db')
    
    # NZX Database
    nzx_db_path = os.path.join('data_collection', 'unified_stock_data.db')
    
    print(f"\nDATABASE PATHS:")
    print(f"  ASX: {asx_db_path}")
    print(f"  NZX: {nzx_db_path}")
    
    # Check if databases exist
    asx_exists = os.path.exists(asx_db_path)
    nzx_exists = os.path.exists(nzx_db_path)
    
    print(f"\nDATABASE EXISTENCE:")
    print(f"  ASX Database: {'EXISTS' if asx_exists else 'NOT FOUND'}")
    print(f"  NZX Database: {'EXISTS' if nzx_exists else 'NOT FOUND'}")
    
    if not asx_exists or not nzx_exists:
        print("\nWARNING: Cannot proceed with comparison - missing databases")
        return
    
    # ASX Database Analysis
    print(f"\n" + "=" * 40)
    print("ASX DATABASE ANALYSIS")
    print("=" * 40)
    
    asx_conn = sqlite3.connect(asx_db_path)
    asx_cursor = asx_conn.cursor()
    
    # ASX Tables
    asx_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    asx_tables = [table[0] for table in asx_cursor.fetchall()]
    print(f"\nASX Tables ({len(asx_tables)}):")
    for table in asx_tables:
        print(f"  - {table}")
    
    # ASX Statistics
    asx_total = 0
    asx_financial = 0
    asx_balance_sheets = 0
    asx_companies = 0
    asx_downloaded = 0
    
    if 'asx_announcements' in asx_tables:
        asx_cursor.execute("SELECT COUNT(*) FROM asx_announcements")
        asx_total = asx_cursor.fetchone()[0]
        
        asx_cursor.execute("SELECT COUNT(*) FROM asx_announcements WHERE is_financial_report = 1")
        asx_financial = asx_cursor.fetchone()[0]
        
        try:
            asx_cursor.execute("SELECT COUNT(*) FROM asx_announcements WHERE is_balance_sheet = 1")
            asx_balance_sheets = asx_cursor.fetchone()[0]
        except sqlite3.OperationalError:
            asx_balance_sheets = 0
        
        asx_cursor.execute("SELECT COUNT(DISTINCT ticker) FROM asx_announcements")
        asx_companies = asx_cursor.fetchone()[0]
        
        asx_cursor.execute("SELECT COUNT(*) FROM asx_announcements WHERE download_status = 'downloaded'")
        asx_downloaded = asx_cursor.fetchone()[0]
        
        print(f"\nASX Statistics:")
        print(f"  Total Announcements: {asx_total}")
        print(f"  Financial Reports: {asx_financial}")
        print(f"  Balance Sheet Reports: {asx_balance_sheets}")
        print(f"  Companies Covered: {asx_companies}")
        print(f"  Downloaded PDFs: {asx_downloaded}")
        
        # ASX Company Breakdown
        asx_cursor.execute("""
            SELECT ticker, COUNT(*) as count, 
                   SUM(CASE WHEN is_financial_report = 1 THEN 1 ELSE 0 END) as financial_count
            FROM asx_announcements 
            GROUP BY ticker 
            ORDER BY count DESC
        """)
        asx_company_stats = asx_cursor.fetchall()
        
        print(f"\nASX Company Breakdown:")
        for ticker, total, financial in asx_company_stats:
            print(f"  {ticker}: {total} total ({financial} financial)")
    
    asx_conn.close()
    
    # NZX Database Analysis
    print(f"\n" + "=" * 40)
    print("NZX DATABASE ANALYSIS")
    print("=" * 40)
    
    nzx_conn = sqlite3.connect(nzx_db_path)
    nzx_cursor = nzx_conn.cursor()
    
    # NZX Tables
    nzx_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    nzx_tables = [table[0] for table in nzx_cursor.fetchall()]
    print(f"\nNZX Tables ({len(nzx_tables)}):")
    for table in nzx_tables:
        print(f"  - {table}")
    
    # NZX Statistics
    nzx_total = 0
    nzx_companies = 0
    nzx_documents = 0
    nzx_balance_sheets = 0
    
    if 'financial_announcements' in nzx_tables:
        nzx_cursor.execute("SELECT COUNT(*) FROM financial_announcements")
        nzx_total = nzx_cursor.fetchone()[0]
        
        nzx_cursor.execute("SELECT COUNT(DISTINCT ticker) FROM financial_announcements")
        nzx_companies = nzx_cursor.fetchone()[0]
        
        print(f"\nNZX Statistics:")
        print(f"  Total Announcements: {nzx_total}")
        print(f"  Companies Covered: {nzx_companies}")
        
        # NZX Company Breakdown
        nzx_cursor.execute("""
            SELECT ticker, COUNT(*) as count
            FROM financial_announcements 
            GROUP BY ticker 
            ORDER BY count DESC
            LIMIT 10
        """)
        nzx_company_stats = nzx_cursor.fetchall()
        
        print(f"\nNZX Top 10 Companies:")
        for ticker, count in nzx_company_stats:
            print(f"  {ticker}: {count} announcements")
    
    # NZX PDF Documents
    if 'financial_documents' in nzx_tables:
        nzx_cursor.execute("SELECT COUNT(*) FROM financial_documents")
        nzx_documents = nzx_cursor.fetchone()[0]
        
        nzx_cursor.execute("SELECT COUNT(DISTINCT document_type) FROM financial_documents")
        nzx_doc_types = nzx_cursor.fetchone()[0]
        
        print(f"\nNZX Documents:")
        print(f"  Total Documents: {nzx_documents}")
        print(f"  Document Types: {nzx_doc_types}")
        
        # Document types breakdown
        nzx_cursor.execute("""
            SELECT document_type, COUNT(*) as count
            FROM financial_documents 
            GROUP BY document_type 
            ORDER BY count DESC
        """)
        nzx_doc_breakdown = nzx_cursor.fetchall()
        
        print(f"  Document Breakdown:")
        for doc_type, count in nzx_doc_breakdown:
            print(f"    {doc_type}: {count}")
    
    # NZX Balance Sheet Data
    if 'balance_sheet_data' in nzx_tables:
        nzx_cursor.execute("SELECT COUNT(*) FROM balance_sheet_data")
        nzx_balance_sheets = nzx_cursor.fetchone()[0]
        
        nzx_cursor.execute("SELECT COUNT(DISTINCT ticker) FROM balance_sheet_data")
        nzx_bs_companies = nzx_cursor.fetchone()[0]
        
        print(f"\nNZX Balance Sheet Data:")
        print(f"  Total Records: {nzx_balance_sheets}")
        print(f"  Companies with Data: {nzx_bs_companies}")
        
        # Check for actual financial data
        nzx_cursor.execute("SELECT COUNT(*) FROM balance_sheet_data WHERE total_assets IS NOT NULL")
        nzx_with_assets = nzx_cursor.fetchone()[0]
        
        nzx_cursor.execute("SELECT COUNT(*) FROM balance_sheet_data WHERE revenue IS NOT NULL")
        nzx_with_revenue = nzx_cursor.fetchone()[0]
        
        print(f"  Records with Assets Data: {nzx_with_assets}")
        print(f"  Records with Revenue Data: {nzx_with_revenue}")
    
    nzx_conn.close()
    
    # Comparison Summary
    print(f"\n" + "=" * 40)
    print("COMPARISON SUMMARY")
    print("=" * 40)
    
    print(f"\nDATA VOLUME COMPARISON:")
    print(f"  ASX Announcements: {asx_total}")
    print(f"  NZX Announcements: {nzx_total}")
    if asx_total > 0:
        print(f"  Ratio (NZX/ASX): {nzx_total/asx_total:.1f}x")
    else:
        print(f"  Ratio: N/A (ASX has no data)")
    
    print(f"\nCOMPANY COVERAGE:")
    print(f"  ASX Companies: {asx_companies}")
    print(f"  NZX Companies: {nzx_companies}")
    if asx_companies > 0:
        print(f"  Ratio (NZX/ASX): {nzx_companies/asx_companies:.1f}x")
    else:
        print(f"  Ratio: N/A (ASX has no companies)")
    
    print(f"\nDOCUMENT PROCESSING:")
    print(f"  ASX Downloaded: {asx_downloaded}")
    print(f"  NZX Documents: {nzx_documents}")
    
    print(f"\nFINANCIAL DATA EXTRACTION:")
    print(f"  ASX Balance Sheets: {asx_balance_sheets}")
    print(f"  NZX Balance Sheet Records: {nzx_balance_sheets}")
    
    # Effectiveness Analysis
    print(f"\nEFFECTIVENESS ANALYSIS:")
    
    if asx_total > 0:
        asx_financial_rate = (asx_financial / asx_total) * 100
        print(f"  ASX Financial Report Rate: {asx_financial_rate:.1f}%")
    
    if nzx_total > 0:
        nzx_financial_rate = (nzx_documents / nzx_total) * 100
        print(f"  NZX Document Processing Rate: {nzx_financial_rate:.1f}%")
    
    # Recommendations
    print(f"\nRECOMMENDATIONS:")
    if asx_total < nzx_total:
        print(f"  - ASX scraper needs improvement (NZX has {nzx_total/asx_total:.1f}x more data)")
    if asx_companies < nzx_companies:
        print(f"  - ASX needs broader company coverage")
    if asx_balance_sheets == 0 and nzx_balance_sheets > 0:
        print(f"  - ASX needs balance sheet data extraction implementation")
    if asx_downloaded == 0 and nzx_documents > 0:
        print(f"  - ASX PDF download system needs activation")
    
    # Data Quality Assessment
    print(f"\nDATA QUALITY ASSESSMENT:")
    print(f"  ASX Completion Rate: {(asx_total/26)*100:.1f}% (assuming 26 target companies)")
    print(f"  NZX Completion Rate: {(nzx_companies/50)*100:.1f}% (assuming 50 target companies)")
    
    if asx_total == 0:
        print(f"  ASX Status: FAILED - No data collected")
    elif asx_total < 10:
        print(f"  ASX Status: POOR - Minimal data collected")
    elif asx_total < 50:
        print(f"  ASX Status: FAIR - Some data collected")
    else:
        print(f"  ASX Status: GOOD - Substantial data collected")
    
    if nzx_total > 100:
        print(f"  NZX Status: EXCELLENT - Comprehensive data collection")
    elif nzx_total > 50:
        print(f"  NZX Status: GOOD - Good data collection")
    else:
        print(f"  NZX Status: FAIR - Limited data collection")

if __name__ == "__main__":
    compare_datasets()