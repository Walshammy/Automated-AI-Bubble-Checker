#!/usr/bin/env python3
"""
ASX Realistic Data Generator
Generate realistic ASX announcement data based on company patterns
"""
import random
from datetime import datetime, timedelta
import sqlite3
import os
import logging
from tqdm import tqdm
from asx_database import ASXDatabase
import asx_config as config

class ASXRealisticGenerator:
    """Generate realistic ASX announcement data"""
    
    def __init__(self):
        self.db = ASXDatabase()
        
        # ASX companies with realistic announcement patterns
        self.companies = {
            'CBA': {
                'name': 'Commonwealth Bank of Australia',
                'sector': 'Banking',
                'announcement_frequency': 'high',
                'report_types': ['quarterly', 'annual', 'dividend', 'trading']
            },
            'WBC': {
                'name': 'Westpac Banking Corporation',
                'sector': 'Banking',
                'announcement_frequency': 'high',
                'report_types': ['quarterly', 'annual', 'dividend', 'trading']
            },
            'ANZ': {
                'name': 'ANZ Group Holdings',
                'sector': 'Banking',
                'announcement_frequency': 'high',
                'report_types': ['quarterly', 'annual', 'dividend', 'trading']
            },
            'NAB': {
                'name': 'National Australia Bank',
                'sector': 'Banking',
                'announcement_frequency': 'high',
                'report_types': ['quarterly', 'annual', 'dividend', 'trading']
            },
            'BHP': {
                'name': 'BHP Group Limited',
                'sector': 'Mining',
                'announcement_frequency': 'high',
                'report_types': ['quarterly', 'annual', 'production', 'dividend']
            },
            'RIO': {
                'name': 'Rio Tinto Limited',
                'sector': 'Mining',
                'announcement_frequency': 'high',
                'report_types': ['quarterly', 'annual', 'production', 'dividend']
            },
            'FMG': {
                'name': 'Fortescue Metals Group',
                'sector': 'Mining',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'production']
            },
            'WDS': {
                'name': 'Woodside Energy Group',
                'sector': 'Energy',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'production', 'dividend']
            },
            'CSL': {
                'name': 'CSL Limited',
                'sector': 'Healthcare',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            },
            'TLS': {
                'name': 'Telstra Group Limited',
                'sector': 'Telecommunications',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            },
            'WES': {
                'name': 'Wesfarmers Limited',
                'sector': 'Retail',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            },
            'WOW': {
                'name': 'Woolworths Group Limited',
                'sector': 'Retail',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            },
            'COL': {
                'name': 'Coles Group Limited',
                'sector': 'Retail',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            },
            'GMG': {
                'name': 'Goodman Group',
                'sector': 'Real Estate',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            },
            'SGP': {
                'name': 'Stockland Corporation',
                'sector': 'Real Estate',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            },
            'ORG': {
                'name': 'Origin Energy Limited',
                'sector': 'Energy',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'production']
            },
            'STO': {
                'name': 'Santos Limited',
                'sector': 'Energy',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'production']
            },
            'AGL': {
                'name': 'AGL Energy Limited',
                'sector': 'Energy',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'production']
            },
            'ALL': {
                'name': 'Aristocrat Leisure Limited',
                'sector': 'Gaming',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            },
            'CAR': {
                'name': 'Carsales.com Limited',
                'sector': 'Technology',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            },
            'IAG': {
                'name': 'Insurance Australia Group',
                'sector': 'Insurance',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            },
            'JHX': {
                'name': 'James Hardie Industries',
                'sector': 'Materials',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            },
            'QAN': {
                'name': 'Qantas Airways Limited',
                'sector': 'Airlines',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'trading']
            },
            'REA': {
                'name': 'REA Group Limited',
                'sector': 'Technology',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            },
            'SUN': {
                'name': 'Suncorp Group Limited',
                'sector': 'Insurance',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            },
            'TCL': {
                'name': 'Transurban Group',
                'sector': 'Infrastructure',
                'announcement_frequency': 'medium',
                'report_types': ['quarterly', 'annual', 'dividend']
            }
        }
        
        # Realistic announcement templates
        self.announcement_templates = {
            'quarterly': [
                "{company} Quarterly Report for {period}",
                "{company} {period} Quarterly Results",
                "{company} Quarterly Financial Report {period}",
                "{company} {period} Quarterly Trading Update"
            ],
            'annual': [
                "{company} Annual Report {year}",
                "{company} {year} Annual Financial Report",
                "{company} Full Year Results {year}",
                "{company} Annual Financial Statements {year}"
            ],
            'half_year': [
                "{company} Half Year Results {year}",
                "{company} Interim Report {year}",
                "{company} {year} Half Year Financial Report",
                "{company} Interim Financial Statements {year}"
            ],
            'dividend': [
                "{company} Dividend Announcement",
                "{company} Dividend Payment Notice",
                "{company} Dividend Declaration",
                "{company} Dividend Record Date"
            ],
            'production': [
                "{company} Production Report {period}",
                "{company} {period} Production Update",
                "{company} Quarterly Production Results",
                "{company} Production Guidance Update"
            ],
            'trading': [
                "{company} Trading Update",
                "{company} Market Update",
                "{company} Business Update",
                "{company} Operational Update"
            ]
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('asx_realistic_generator.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def generate_realistic_announcements(self, ticker, company_info):
        """Generate realistic announcements for a company"""
        announcements = []
        
        # Determine number of announcements based on frequency
        frequency_map = {'high': 8, 'medium': 6, 'low': 4}
        num_announcements = frequency_map.get(company_info['announcement_frequency'], 6)
        
        # Generate announcements over the last 12 months
        start_date = datetime.now() - timedelta(days=365)
        
        for i in range(num_announcements):
            # Random date within the last 12 months
            days_ago = random.randint(0, 365)
            announcement_date = start_date + timedelta(days=days_ago)
            
            # Select report type
            report_type = random.choice(company_info['report_types'])
            
            # Generate title
            if report_type == 'quarterly':
                period = f"Q{random.randint(1, 4)} {announcement_date.year}"
                title = random.choice(self.announcement_templates['quarterly']).format(
                    company=company_info['name'], period=period
                )
            elif report_type == 'annual':
                title = random.choice(self.announcement_templates['annual']).format(
                    company=company_info['name'], year=announcement_date.year
                )
            elif report_type == 'half_year':
                title = random.choice(self.announcement_templates['half_year']).format(
                    company=company_info['name'], year=announcement_date.year
                )
            elif report_type == 'dividend':
                title = random.choice(self.announcement_templates['dividend']).format(
                    company=company_info['name']
                )
            elif report_type == 'production':
                period = f"Q{random.randint(1, 4)} {announcement_date.year}"
                title = random.choice(self.announcement_templates['production']).format(
                    company=company_info['name'], period=period
                )
            elif report_type == 'trading':
                title = random.choice(self.announcement_templates['trading']).format(
                    company=company_info['name']
                )
            else:
                title = f"{company_info['name']} Financial Report {announcement_date.strftime('%B %Y')}"
            
            # Create announcement
            announcement = {
                'announcement_id': f"{ticker}_{hash(title) % 1000000}",
                'ticker': ticker.upper(),
                'company_name': company_info['name'],
                'announcement_date': announcement_date,
                'title': title,
                'url': f"https://www.asx.com.au/asx/statistics/announcements.do?asxCode={ticker}&announcementId={hash(title) % 1000000}",
                'file_size': f"{random.randint(500, 5000)} KB",
                'market_sensitive': self.is_market_sensitive(title),
                'is_financial_report': self.is_financial_report(title),
                'is_balance_sheet': self.is_balance_sheet_report(title),
                'pdf_filename': f"{ticker}_{announcement_date.strftime('%Y%m%d')}_{hash(title) % 1000}.pdf"
            }
            
            announcements.append(announcement)
        
        return announcements
    
    def is_financial_report(self, title):
        """Check if announcement is a financial report"""
        title_lower = title.lower()
        financial_keywords = [
            'annual report', 'financial', 'balance sheet', 'results', 'earnings',
            'quarterly', 'half year', 'full year', 'financial statements',
            'profit', 'revenue', 'dividend', 'audit', 'interim', 'preliminary',
            'cash flow', 'income statement', 'consolidated', 'unaudited'
        ]
        return any(keyword in title_lower for keyword in financial_keywords)
    
    def is_balance_sheet_report(self, title):
        """Check if announcement specifically contains balance sheet data"""
        title_lower = title.lower()
        balance_sheet_terms = [
            'balance sheet', 'financial statements', 'annual report', 'half year',
            'full year', 'quarterly report', 'consolidated financial statements'
        ]
        return any(term in title_lower for term in balance_sheet_terms)
    
    def is_market_sensitive(self, title):
        """Check if announcement is market sensitive"""
        title_lower = title.lower()
        sensitive_terms = [
            'profit', 'loss', 'earnings', 'revenue', 'dividend', 'acquisition',
            'merger', 'restructure', 'ceo', 'cfo', 'resignation', 'appointment'
        ]
        return any(term in title_lower for term in sensitive_terms)
    
    def run_generation(self):
        """Run realistic data generation"""
        print("=" * 80)
        print("ASX REALISTIC DATA GENERATION SYSTEM")
        print("=" * 80)
        
        total_announcements = 0
        total_financial = 0
        total_balance_sheets = 0
        successful_companies = 0
        
        # Process each company
        for ticker, company_info in tqdm(self.companies.items(), desc="Generating Data"):
            try:
                announcements = self.generate_realistic_announcements(ticker, company_info)
                
                if announcements:
                    # Save to database
                    new_count = 0
                    financial_count = 0
                    balance_sheet_count = 0
                    
                    for announcement in announcements:
                        if self.db.insert_announcement(announcement):
                            new_count += 1
                            if announcement['is_financial_report']:
                                financial_count += 1
                            if announcement.get('is_balance_sheet', False):
                                balance_sheet_count += 1
                    
                    total_announcements += new_count
                    total_financial += financial_count
                    total_balance_sheets += balance_sheet_count
                    successful_companies += 1
                    
                    self.logger.info(f"SUCCESS: {ticker} - {new_count} announcements ({financial_count} financial)")
                else:
                    self.logger.warning(f"NO DATA: {ticker} - No announcements generated")
                
            except Exception as e:
                self.logger.error(f"Error processing {ticker}: {e}")
                continue
        
        # Final results
        print(f"\n" + "=" * 60)
        print("ASX REALISTIC GENERATION RESULTS")
        print("=" * 60)
        print(f"Companies processed: {successful_companies}/{len(self.companies)}")
        print(f"Total announcements: {total_announcements}")
        print(f"Financial reports: {total_financial}")
        print(f"Balance sheet reports: {total_balance_sheets}")
        
        # Database statistics
        db_stats = self.db.get_statistics()
        print(f"\nUpdated Database Statistics:")
        for key, value in db_stats.items():
            print(f"  {key}: {value}")
        
        return {
            'companies_processed': successful_companies,
            'total_announcements': total_announcements,
            'financial_reports': total_financial,
            'balance_sheet_reports': total_balance_sheets
        }

def main():
    """Main execution function"""
    generator = ASXRealisticGenerator()
    results = generator.run_generation()
    
    print(f"\n" + "=" * 80)
    print("ASX REALISTIC DATA GENERATION COMPLETE")
    print("=" * 80)
    print(f"Successfully generated ASX data:")
    print(f"  - {results['companies_processed']} companies processed")
    print(f"  - {results['total_announcements']} announcements")
    print(f"  - {results['financial_reports']} financial reports")
    print(f"  - {results['balance_sheet_reports']} balance sheet reports")

if __name__ == "__main__":
    main()
