#!/usr/bin/env python3
"""
ASX Full Collection with Mock Data
Demonstrates complete ASX collection process with realistic data
"""
import pandas as pd
import os
import sqlite3
from datetime import datetime, timedelta
import random
from asx_database import ASXDatabase

def create_realistic_asx_data():
    """Create realistic ASX announcement data for demonstration"""
    
    # Load the top companies list
    csv_path = os.path.join(os.path.dirname(__file__), 'ASX_top_companies.csv')
    companies_df = pd.read_csv(csv_path)
    
    # Realistic announcement templates
    announcement_templates = {
        'Banking': [
            'Annual Financial Report {year}',
            'Half Year Results {year}',
            'Quarterly Trading Update',
            'Dividend/Distribution Announcement',
            'Capital Adequacy Report',
            'Risk Management Report',
            'Corporate Governance Statement',
            'Sustainability Report {year}',
            'Audit Committee Report',
            'Remuneration Report'
        ],
        'Mining': [
            'Annual Financial Report {year}',
            'Half Year Results {year}',
            'Quarterly Production Report',
            'Mineral Resources and Ore Reserves Statement',
            'Exploration Update',
            'Environmental Report {year}',
            'Safety Performance Report',
            'Community Investment Report',
            'Climate Change Report',
            'Operational Review'
        ],
        'Healthcare': [
            'Annual Financial Report {year}',
            'Half Year Results {year}',
            'Quarterly Business Update',
            'Research and Development Report',
            'Clinical Trial Results',
            'Regulatory Update',
            'Product Pipeline Report',
            'Market Access Report',
            'Quality Assurance Report',
            'Innovation Report'
        ],
        'Retail': [
            'Annual Financial Report {year}',
            'Half Year Results {year}',
            'Quarterly Sales Update',
            'Store Performance Report',
            'Digital Transformation Update',
            'Supply Chain Report',
            'Customer Experience Report',
            'Brand Performance Review',
            'Market Expansion Update',
            'Sustainability Report {year}'
        ],
        'Energy': [
            'Annual Financial Report {year}',
            'Half Year Results {year}',
            'Quarterly Production Report',
            'Reserves Report',
            'Environmental Performance Report',
            'Safety Report',
            'Energy Transition Update',
            'Carbon Emissions Report',
            'Community Engagement Report',
            'Operational Excellence Report'
        ],
        'Technology': [
            'Annual Financial Report {year}',
            'Half Year Results {year}',
            'Quarterly Business Update',
            'Product Development Report',
            'Technology Innovation Report',
            'Market Expansion Update',
            'Partnership Announcement',
            'Digital Strategy Report',
            'Cybersecurity Report',
            'Data Privacy Report'
        ]
    }
    
    announcements = []
    
    for _, company in companies_df.iterrows():
        ticker = company['ticker']
        company_name = company['name']
        sector = company['sector']
        
        # Get templates for this sector
        templates = announcement_templates.get(sector, announcement_templates['Banking'])
        
        # Generate 8-15 announcements per company
        num_announcements = random.randint(8, 15)
        
        for i in range(num_announcements):
            # Random date in the last 2 years
            days_ago = random.randint(1, 730)
            announcement_date = datetime.now() - timedelta(days=days_ago)
            
            # Select random template
            template = random.choice(templates)
            title = template.format(year=announcement_date.year)
            
            # Generate PDF URL
            pdf_id = f"{random.randint(100000, 999999)}"
            pdf_url = f"https://www.asx.com.au/asxpdf/{announcement_date.strftime('%Y%m%d')}/pdf/{pdf_id}.pdf"
            pdf_filename = f"{pdf_id}.pdf"
            
            # Determine if it's financial
            is_financial = any(keyword in title.lower() for keyword in 
                             ['annual', 'half year', 'quarterly', 'financial', 'results', 'report'])
            
            # Determine if it's balance sheet
            is_balance_sheet = any(keyword in title.lower() for keyword in 
                                 ['annual', 'half year', 'quarterly', 'financial statements'])
            
            # Market sensitivity
            market_sensitive = any(keyword in title.lower() for keyword in 
                                 ['results', 'dividend', 'earnings', 'profit', 'loss'])
            
            announcement = {
                'announcement_id': f"{ticker}_{hash(title) % 1000000}",
                'ticker': ticker,
                'company_name': company_name,
                'announcement_date': announcement_date,
                'title': title,
                'url': pdf_url,
                'file_size': f"{random.randint(100, 5000)}KB",
                'market_sensitive': market_sensitive,
                'is_financial_report': is_financial,
                'is_balance_sheet': is_balance_sheet,
                'pdf_filename': pdf_filename
            }
            
            announcements.append(announcement)
    
    return announcements

def run_full_asx_collection():
    """Run full ASX collection with realistic data"""
    print("=" * 80)
    print("ASX FULL COLLECTION - REALISTIC DATA DEMONSTRATION")
    print("=" * 80)
    
    # Initialize database
    db = ASXDatabase()
    
    # Create realistic data
    print("\nGenerating realistic ASX announcement data...")
    announcements = create_realistic_asx_data()
    
    print(f"Generated {len(announcements)} announcements")
    
    # Insert into database
    print("\nInserting announcements into database...")
    successful_inserts = 0
    financial_reports = 0
    balance_sheets = 0
    market_sensitive = 0
    
    for announcement in announcements:
        try:
            if db.insert_announcement(announcement):
                successful_inserts += 1
                if announcement['is_financial_report']:
                    financial_reports += 1
                if announcement['is_balance_sheet']:
                    balance_sheets += 1
                if announcement['market_sensitive']:
                    market_sensitive += 1
        except Exception as e:
            print(f"Error inserting announcement: {e}")
            continue
    
    # Show results
    print(f"\n" + "=" * 60)
    print("COLLECTION RESULTS")
    print("=" * 60)
    print(f"Total Announcements: {successful_inserts}")
    print(f"Financial Reports: {financial_reports}")
    print(f"Balance Sheet Reports: {balance_sheets}")
    print(f"Market Sensitive: {market_sensitive}")
    
    # Company breakdown
    print(f"\nCompany Breakdown:")
    company_stats = {}
    for ann in announcements:
        ticker = ann['ticker']
        if ticker not in company_stats:
            company_stats[ticker] = {'total': 0, 'financial': 0}
        company_stats[ticker]['total'] += 1
        if ann['is_financial_report']:
            company_stats[ticker]['financial'] += 1
    
    for ticker, stats in sorted(company_stats.items()):
        print(f"  {ticker}: {stats['total']} total ({stats['financial']} financial)")
    
    # Database statistics
    db_stats = db.get_statistics()
    print(f"\nDatabase Statistics:")
    for key, value in db_stats.items():
        print(f"  {key}: {value}")
    
    # Sector analysis
    print(f"\nSector Analysis:")
    sector_stats = {}
    for ann in announcements:
        # Extract sector from company name or use default
        sector = "Mixed"
        if any(bank in ann['company_name'].lower() for bank in ['bank', 'banking']):
            sector = "Banking"
        elif any(mining in ann['company_name'].lower() for mining in ['bhp', 'rio', 'fortescue', 'woodside']):
            sector = "Mining/Energy"
        elif any(health in ann['company_name'].lower() for health in ['csl', 'health']):
            sector = "Healthcare"
        elif any(retail in ann['company_name'].lower() for retail in ['wesfarmers', 'woolworths', 'coles']):
            sector = "Retail"
        
        if sector not in sector_stats:
            sector_stats[sector] = {'total': 0, 'financial': 0}
        sector_stats[sector]['total'] += 1
        if ann['is_financial_report']:
            sector_stats[sector]['financial'] += 1
    
    for sector, stats in sector_stats.items():
        print(f"  {sector}: {stats['total']} total ({stats['financial']} financial)")
    
    print(f"\n" + "=" * 60)
    print("ASX FULL COLLECTION COMPLETE")
    print("=" * 60)
    print("Successfully demonstrated:")
    print(f"  - {successful_inserts} announcements collected")
    print(f"  - {financial_reports} financial reports identified")
    print(f"  - {balance_sheets} balance sheet reports")
    print(f"  - {len(company_stats)} companies covered")
    print(f"  - Complete database integration")
    print(f"  - Realistic data structure")
    
    return successful_inserts

if __name__ == "__main__":
    run_full_asx_collection()
