#!/usr/bin/env python3
"""
ASX Scraper Database Schema Demonstration
"""
import sqlite3

def show_database_schema():
    """Show the database schema and current data"""
    conn = sqlite3.connect('asx_data/asx_announcements.db')
    cursor = conn.cursor()
    
    print("=" * 60)
    print("ASX SCRAPER DATABASE SCHEMA DEMONSTRATION")
    print("=" * 60)
    
    # Show all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print("\nDatabase Tables:")
    for table in tables:
        print(f"  - {table[0]}")
    
    # Show table schemas
    print("\nTable Schemas:")
    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        print(f"\n  {table_name}:")
        for col in columns:
            print(f"    - {col[1]} ({col[2]})")
    
    # Show current data
    print("\nCurrent Data:")
    cursor.execute("SELECT COUNT(*) FROM asx_announcements")
    count = cursor.fetchone()[0]
    print(f"  Total announcements: {count}")
    
    if count > 0:
        cursor.execute("SELECT ticker, title, announcement_date, is_financial_report FROM asx_announcements LIMIT 3")
        rows = cursor.fetchall()
        print("  Recent announcements:")
        for row in rows:
            print(f"    {row[0]}: {row[1][:50]}... (Financial: {bool(row[3])})")
    
    conn.close()

if __name__ == "__main__":
    show_database_schema()