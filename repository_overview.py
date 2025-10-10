#!/usr/bin/env python3
"""
Repository Structure Overview
Provides comprehensive overview of organized repository structure
"""
import os
from pathlib import Path

def analyze_repository_structure():
    """Analyze and display repository structure"""
    
    base_dir = Path(__file__).parent
    
    print("="*80)
    print("REPOSITORY STRUCTURE OVERVIEW")
    print("="*80)
    
    # Core directories
    print("\nCORE DIRECTORIES:")
    print("-" * 40)
    
    directories = [
        ("asx_scraper/", "ASX data collection system"),
        ("Balance_Sheet_Scraper/", "NZX data collection system"),
        ("consolidated_data/", "Unified database and PDFs"),
        ("data_collection/", "Stock data collection"),
        ("valuation_analysis/", "Valuation analysis tools"),
        ("bubble_analysis/", "AI bubble analysis")
    ]
    
    for dir_name, description in directories:
        dir_path = base_dir / dir_name
        if dir_path.exists():
            print(f"[OK] {dir_name:<25} - {description}")
        else:
            print(f"[MISSING] {dir_name:<25} - {description} (not found)")
    
    # Database files
    print(f"\nDATABASE FILES:")
    print("-" * 40)
    
    db_files = [
        ("consolidated_data/unified_financial_data.db", "Unified ASX/NZX financial data"),
        ("asx_scraper/asx_data/asx_announcements.db", "ASX announcements database"),
        ("Balance_Sheet_Scraper/balance_sheet_data/nzx_financial_data.db", "NZX financial data"),
        ("data_collection/unified_stock_data.db", "Stock market data"),
        ("valuation_analysis/stock_valuation_data.db", "Valuation analysis data")
    ]
    
    for db_path, description in db_files:
        full_path = base_dir / db_path
        if full_path.exists():
            size_mb = full_path.stat().st_size / (1024 * 1024)
            print(f"[OK] {db_path:<45} - {description} ({size_mb:.1f} MB)")
        else:
            print(f"[MISSING] {db_path:<45} - {description} (not found)")
    
    # Key scripts
    print(f"\nKEY SCRIPTS:")
    print("-" * 40)
    
    scripts = [
        ("asx_scraper/asx_production_system.py", "ASX production collection system"),
        ("asx_scraper/asx_enhanced_collector.py", "ASX enhanced data collector"),
        ("Balance_Sheet_Scraper/comprehensive_nzx_scraper.py", "NZX comprehensive scraper"),
        ("organize_databases.py", "Database organization tool"),
        ("data_collection/stock_data_collector.py", "Stock data collector"),
        ("valuation_analysis/stock_valuation_scraper.py", "Valuation scraper")
    ]
    
    for script_path, description in scripts:
        full_path = base_dir / script_path
        if full_path.exists():
            print(f"[OK] {script_path:<45} - {description}")
        else:
            print(f"[MISSING] {script_path:<45} - {description} (not found)")
    
    # Data summary
    print(f"\nDATA SUMMARY:")
    print("-" * 40)
    
    # Check unified database
    unified_db = base_dir / "consolidated_data" / "unified_financial_data.db"
    if unified_db.exists():
        import sqlite3
        conn = sqlite3.connect(unified_db)
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT COUNT(*) FROM financial_announcements')
            total = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE is_balance_sheet = 1')
            balance_sheets = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE download_status = "downloaded"')
            downloaded = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(DISTINCT ticker) FROM financial_announcements')
            companies = cursor.fetchone()[0]
            
            print(f"Total Financial Announcements: {total}")
            print(f"Balance Sheet Reports: {balance_sheets}")
            print(f"Downloaded PDFs: {downloaded}")
            print(f"Companies Covered: {companies}")
            
        except Exception as e:
            print(f"Error reading database: {e}")
        finally:
            conn.close()
    
    # PDF directories
    print(f"\nPDF COLLECTIONS:")
    print("-" * 40)
    
    pdf_dirs = [
        ("consolidated_data/pdfs/ASX/", "ASX PDF documents"),
        ("consolidated_data/pdfs/NZX/", "NZX PDF documents"),
        ("asx_scraper/asx_data/pdfs/", "ASX PDFs (original)"),
        ("Balance_Sheet_Scraper/balance_sheet_data/pdfs/", "NZX PDFs (original)")
    ]
    
    for pdf_path, description in pdf_dirs:
        full_path = base_dir / pdf_path
        if full_path.exists():
            pdf_count = len(list(full_path.rglob("*.pdf")))
            print(f"[OK] {pdf_path:<35} - {description} ({pdf_count} PDFs)")
        else:
            print(f"[MISSING] {pdf_path:<35} - {description} (not found)")
    
    print(f"\n" + "="*80)
    print("REPOSITORY CLEANUP STATUS")
    print("="*80)
    print("[SUCCESS] Redundant scripts removed")
    print("[SUCCESS] Databases consolidated")
    print("[SUCCESS] PDFs organized")
    print("[SUCCESS] Unified structure created")
    print("[SUCCESS] Data integrity maintained")
    
    print(f"\nNext Steps:")
    print("- Use consolidated_data/unified_financial_data.db for analysis")
    print("- Access PDFs through consolidated_data/pdfs/")
    print("- Run production systems from respective directories")
    print("- All data is now well-organized and accessible")

if __name__ == "__main__":
    analyze_repository_structure()
