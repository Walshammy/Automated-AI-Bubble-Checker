#!/usr/bin/env python3
"""
Enhanced Database Scale Report Generator
Creates a comprehensive PDF report with proper database schema handling
"""
import sqlite3
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
import json
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

class EnhancedDatabaseReporter:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.downloads_dir = Path.home() / "Downloads"
        
        # Database paths
        self.unified_db = self.base_dir / "consolidated_data" / "unified_financial_data.db"
        self.asx_db = self.base_dir / "asx_scraper" / "asx_data" / "asx_announcements.db"
        self.stock_db = self.base_dir / "data_collection" / "unified_stock_data.db"
        
        # PDF directories
        self.asx_pdfs = self.base_dir / "asx_scraper" / "asx_data" / "pdfs"
        self.nzx_pdfs = self.base_dir / "Balance_Sheet_Scraper" / "balance_sheet_data" / "pdfs"
        self.unified_pdfs = self.base_dir / "consolidated_data" / "pdfs"
        
        # Example companies
        self.companies = {
            'AIR': {'name': 'Air New Zealand', 'exchange': 'NZX', 'ticker': 'AIR'},
            'QAN': {'name': 'Qantas Airways', 'exchange': 'ASX', 'ticker': 'QAN'},
            'NVDA': {'name': 'NVIDIA Corporation', 'exchange': 'NASDAQ', 'ticker': 'NVDA'}
        }
        
        # Output file
        self.output_file = self.downloads_dir / f"Enhanced_Database_Scale_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"

    def get_database_schema_info(self, db_path):
        """Get database schema information"""
        if not db_path.exists():
            return {}
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            schema_info = {}
            for table in tables:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                schema_info[table] = [col[1] for col in columns]
            
            return schema_info
        except Exception as e:
            print(f"Error reading schema from {db_path}: {e}")
            return {}
        finally:
            conn.close()

    def get_comprehensive_stats(self):
        """Get comprehensive database statistics"""
        stats = {}
        
        # Unified database stats
        if self.unified_db.exists():
            conn = sqlite3.connect(self.unified_db)
            cursor = conn.cursor()
            
            try:
                cursor.execute('SELECT COUNT(*) FROM financial_announcements')
                stats['unified_total'] = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE is_financial_report = 1')
                stats['unified_financial'] = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE is_balance_sheet = 1')
                stats['unified_balance_sheets'] = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM financial_announcements WHERE download_status = "downloaded"')
                stats['unified_downloaded'] = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(DISTINCT ticker) FROM financial_announcements')
                stats['unified_companies'] = cursor.fetchone()[0]
                
                # Date range
                cursor.execute('SELECT MIN(announcement_date), MAX(announcement_date) FROM financial_announcements')
                date_range = cursor.fetchone()
                stats['unified_date_range'] = date_range
                
                # Top companies by balance sheets
                cursor.execute('''
                    SELECT ticker, COUNT(*) as balance_sheet_count
                    FROM financial_announcements 
                    WHERE is_balance_sheet = 1
                    GROUP BY ticker
                    ORDER BY balance_sheet_count DESC
                    LIMIT 10
                ''')
                stats['top_balance_sheet_companies'] = cursor.fetchall()
                
            except Exception as e:
                print(f"Error reading unified database: {e}")
            finally:
                conn.close()
        
        # Stock database stats
        if self.stock_db.exists():
            schema = self.get_database_schema_info(self.stock_db)
            conn = sqlite3.connect(self.stock_db)
            cursor = conn.cursor()
            
            try:
                # Try different possible table names
                possible_tables = ['stock_data', 'stocks', 'market_data', 'price_data']
                stock_table = None
                
                for table in possible_tables:
                    if table in schema:
                        stock_table = table
                        break
                
                if stock_table:
                    cursor.execute(f'SELECT COUNT(*) FROM {stock_table}')
                    stats['stock_total'] = cursor.fetchone()[0]
                    
                    cursor.execute(f'SELECT COUNT(DISTINCT ticker) FROM {stock_table}')
                    stats['stock_companies'] = cursor.fetchone()[0]
                    
                    cursor.execute(f'SELECT MIN(date), MAX(date) FROM {stock_table}')
                    date_range = cursor.fetchone()
                    stats['stock_date_range'] = date_range
                    
                    stats['stock_schema'] = schema[stock_table]
                
            except Exception as e:
                print(f"Error reading stock database: {e}")
            finally:
                conn.close()
        
        return stats

    def get_enhanced_company_data(self, ticker):
        """Get enhanced company data with proper schema handling"""
        company_data = {
            'ticker': ticker,
            'name': self.companies.get(ticker, {}).get('name', ticker),
            'exchange': self.companies.get(ticker, {}).get('exchange', 'Unknown'),
            'financial_announcements': [],
            'stock_data': [],
            'pdf_files': [],
            'summary': {}
        }
        
        # Financial announcements from unified database
        if self.unified_db.exists():
            conn = sqlite3.connect(self.unified_db)
            cursor = conn.cursor()
            
            try:
                cursor.execute('''
                    SELECT announcement_id, title, announcement_date, url, pdf_filename, 
                           download_status, is_financial_report, is_balance_sheet, exchange
                    FROM financial_announcements 
                    WHERE ticker = ?
                    ORDER BY announcement_date DESC
                ''', (ticker,))
                
                announcements = cursor.fetchall()
                company_data['financial_announcements'] = announcements
                
                # Summary stats
                company_data['summary']['total_announcements'] = len(announcements)
                company_data['summary']['financial_reports'] = sum(1 for a in announcements if a[6])
                company_data['summary']['balance_sheets'] = sum(1 for a in announcements if a[7])
                company_data['summary']['downloaded_pdfs'] = sum(1 for a in announcements if a[5] == 'downloaded')
                
                # Recent announcements (last 5)
                company_data['summary']['recent_announcements'] = announcements[:5]
                
            except Exception as e:
                print(f"Error reading financial data for {ticker}: {e}")
            finally:
                conn.close()
        
        # Stock data with schema detection
        if self.stock_db.exists():
            schema = self.get_database_schema_info(self.stock_db)
            conn = sqlite3.connect(self.stock_db)
            cursor = conn.cursor()
            
            try:
                # Find the correct table and columns
                possible_tables = ['stock_data', 'stocks', 'market_data', 'price_data']
                stock_table = None
                
                for table in possible_tables:
                    if table in schema:
                        stock_table = table
                        break
                
                if stock_table:
                    columns = schema[stock_table]
                    
                    # Build query based on available columns
                    if 'ticker' in columns and 'date' in columns:
                        query = f'''
                            SELECT {', '.join(columns)}
                            FROM {stock_table} 
                            WHERE ticker = ?
                            ORDER BY date DESC
                            LIMIT 20
                        '''
                        
                        cursor.execute(query, (ticker,))
                        stock_data = cursor.fetchall()
                        company_data['stock_data'] = stock_data
                        company_data['summary']['stock_data_points'] = len(stock_data)
                        
                        if stock_data:
                            # Try to extract latest price if available
                            price_columns = ['close', 'price', 'last_price', 'current_price']
                            for col in price_columns:
                                if col in columns:
                                    col_index = columns.index(col)
                                    company_data['summary']['latest_price'] = stock_data[0][col_index]
                                    break
                
            except Exception as e:
                print(f"Error reading stock data for {ticker}: {e}")
            finally:
                conn.close()
        
        # PDF files
        pdf_dirs = [self.asx_pdfs, self.nzx_pdfs, self.unified_pdfs]
        for pdf_dir in pdf_dirs:
            if pdf_dir.exists():
                company_pdf_dir = pdf_dir / ticker
                if company_pdf_dir.exists():
                    pdf_files = list(company_pdf_dir.glob('*.pdf'))
                    company_data['pdf_files'].extend([str(f) for f in pdf_files])
        
        company_data['summary']['total_pdfs'] = len(company_data['pdf_files'])
        
        return company_data

    def create_enhanced_pdf_report(self):
        """Create enhanced PDF report with comprehensive analysis"""
        doc = SimpleDocTemplate(str(self.output_file), pagesize=A4)
        story = []
        
        # Styles
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            alignment=TA_CENTER,
            textColor=colors.darkblue
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=16,
            spaceAfter=12,
            textColor=colors.darkblue
        )
        
        subheading_style = ParagraphStyle(
            'CustomSubHeading',
            parent=styles['Heading3'],
            fontSize=14,
            spaceAfter=8,
            textColor=colors.darkgreen
        )
        
        # Title
        story.append(Paragraph("Enhanced Financial Database Scale Report", title_style))
        story.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
        story.append(Spacer(1, 20))
        
        # Executive Summary
        story.append(Paragraph("Executive Summary", heading_style))
        stats = self.get_comprehensive_stats()
        
        summary_text = f"""
        This comprehensive report demonstrates the scale and integration of our financial data collection system,
        showcasing three representative companies across different exchanges and market segments.
        
        Our unified database contains {stats.get('unified_total', 0):,} financial announcements across 
        {stats.get('unified_companies', 0)} companies, with {stats.get('unified_balance_sheets', 0):,} balance sheet reports
        and {stats.get('unified_downloaded', 0):,} downloaded PDF documents.
        
        The system integrates data from multiple exchanges (ASX, NZX) and includes comprehensive financial reporting,
        stock market data, and valuation metrics. This report showcases three companies representing different
        market segments: Air New Zealand (NZX - Airlines), Qantas Airways (ASX - Airlines), and NVIDIA Corporation (NASDAQ - Technology).
        """
        
        story.append(Paragraph(summary_text, styles['Normal']))
        story.append(Spacer(1, 20))
        
        # Database Overview
        story.append(Paragraph("Database Overview", heading_style))
        
        overview_data = [
            ['Metric', 'Count', 'Description'],
            ['Total Financial Announcements', f"{stats.get('unified_total', 0):,}", 'All financial reports collected'],
            ['Financial Reports', f"{stats.get('unified_financial', 0):,}", 'Reports marked as financial'],
            ['Balance Sheet Reports', f"{stats.get('unified_balance_sheets', 0):,}", 'Specific balance sheet documents'],
            ['Downloaded PDFs', f"{stats.get('unified_downloaded', 0):,}", 'Successfully downloaded documents'],
            ['Companies Covered', f"{stats.get('unified_companies', 0):,}", 'Unique companies in database'],
            ['Stock Data Points', f"{stats.get('stock_total', 0):,}", 'Historical stock market data'],
            ['Stock Companies', f"{stats.get('stock_companies', 0):,}", 'Companies with stock data']
        ]
        
        if stats.get('unified_date_range'):
            start_date, end_date = stats['unified_date_range']
            overview_data.append(['Date Range', f"{start_date[:10]} to {end_date[:10]}" if start_date and end_date else 'N/A', 'Coverage period'])
        
        overview_table = Table(overview_data, colWidths=[2.5*inch, 1*inch, 2.5*inch])
        overview_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(overview_table)
        story.append(Spacer(1, 20))
        
        # Top Companies by Balance Sheets
        if stats.get('top_balance_sheet_companies'):
            story.append(Paragraph("Top Companies by Balance Sheet Reports", subheading_style))
            
            top_companies_data = [['Company', 'Balance Sheet Reports']]
            for ticker, count in stats['top_balance_sheet_companies']:
                top_companies_data.append([ticker, str(count)])
            
            top_companies_table = Table(top_companies_data, colWidths=[2*inch, 1*inch])
            top_companies_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            story.append(top_companies_table)
            story.append(Spacer(1, 20))
        
        # Company Analysis
        for ticker in ['AIR', 'QAN', 'NVDA']:
            story.append(PageBreak())
            company_data = self.get_enhanced_company_data(ticker)
            
            # Company Header
            story.append(Paragraph(f"{company_data['name']} ({ticker})", heading_style))
            story.append(Paragraph(f"Exchange: {company_data['exchange']}", styles['Normal']))
            story.append(Spacer(1, 12))
            
            # Company Summary
            story.append(Paragraph("Data Summary", subheading_style))
            
            summary_data = [
                ['Metric', 'Value'],
                ['Total Announcements', f"{company_data['summary'].get('total_announcements', 0):,}"],
                ['Financial Reports', f"{company_data['summary'].get('financial_reports', 0):,}"],
                ['Balance Sheet Reports', f"{company_data['summary'].get('balance_sheets', 0):,}"],
                ['Downloaded PDFs', f"{company_data['summary'].get('downloaded_pdfs', 0):,}"],
                ['Total PDF Files', f"{company_data['summary'].get('total_pdfs', 0):,}"],
                ['Stock Data Points', f"{company_data['summary'].get('stock_data_points', 0):,}"]
            ]
            
            if company_data['summary'].get('latest_price'):
                summary_data.append(['Latest Price', f"${company_data['summary']['latest_price']:.2f}"])
            
            summary_table = Table(summary_data, colWidths=[2*inch, 1.5*inch])
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            story.append(summary_table)
            story.append(Spacer(1, 12))
            
            # Recent Financial Announcements
            if company_data['summary'].get('recent_announcements'):
                story.append(Paragraph("Recent Financial Announcements", subheading_style))
                
                announcement_data = [['Date', 'Title', 'Type', 'Status']]
                
                for ann in company_data['summary']['recent_announcements']:
                    ann_id, title, date, url, pdf_filename, status, is_financial, is_balance_sheet, exchange = ann
                    
                    # Determine type
                    if is_balance_sheet:
                        ann_type = "Balance Sheet"
                    elif is_financial:
                        ann_type = "Financial Report"
                    else:
                        ann_type = "General"
                    
                    # Truncate title if too long
                    display_title = title[:40] + "..." if len(title) > 40 else title
                    
                    announcement_data.append([
                        date[:10] if date else 'N/A',
                        display_title,
                        ann_type,
                        status
                    ])
                
                announcement_table = Table(announcement_data, colWidths=[1*inch, 3*inch, 1*inch, 1*inch])
                announcement_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                
                story.append(announcement_table)
                story.append(Spacer(1, 12))
            
            # PDF Files Analysis
            if company_data['pdf_files']:
                story.append(Paragraph("PDF Document Analysis", subheading_style))
                
                pdf_analysis_text = f"""
                Total PDF files available: {len(company_data['pdf_files'])}
                
                These documents contain comprehensive financial information including:
                • Balance sheets and financial statements
                • Annual and interim reports
                • Management commentary and analysis
                • Regulatory filings and announcements
                
                All PDFs are organized by company ticker and are available for detailed analysis.
                """
                
                story.append(Paragraph(pdf_analysis_text, styles['Normal']))
                story.append(Spacer(1, 12))
        
        # Data Integration Analysis
        story.append(PageBreak())
        story.append(Paragraph("Data Integration Analysis", heading_style))
        
        integration_text = f"""
        The system demonstrates comprehensive data integration across multiple sources:
        
        1. Financial Announcements Database:
           • Unified schema across ASX and NZX exchanges
           • Comprehensive metadata including document types and download status
           • Cross-reference capabilities for analysis
        
        2. Stock Market Data Integration:
           • Historical price and volume data
           • Market capitalization tracking
           • Cross-reference with financial announcements
        
        3. Document Management:
           • Organized PDF storage by exchange and company
           • Download status tracking
           • Comprehensive document coverage
        
        4. Data Quality Metrics:
           • {stats.get('unified_downloaded', 0)/max(stats.get('unified_total', 1), 1)*100:.1f}% PDF download success rate
           • Comprehensive coverage across {stats.get('unified_companies', 0)} companies
           • Multi-year historical data collection
        
        This integrated approach enables comprehensive financial analysis, trend identification,
        and cross-company comparisons across different market segments and exchanges.
        """
        
        story.append(Paragraph(integration_text, styles['Normal']))
        story.append(Spacer(1, 20))
        
        # Conclusion
        story.append(Paragraph("Conclusion", heading_style))
        
        conclusion_text = f"""
        This enhanced report demonstrates the comprehensive scale and integration of our financial data collection system.
        
        Key Achievements:
        • Unified database containing {stats.get('unified_total', 0):,} financial announcements
        • {stats.get('unified_balance_sheets', 0):,} balance sheet reports across {stats.get('unified_companies', 0)} companies
        • {stats.get('unified_downloaded', 0):,} successfully downloaded PDF documents
        • Integration of multiple data sources: financial announcements, stock data, and document management
        • Cross-exchange coverage including ASX, NZX, and international markets
        • Comprehensive company coverage across different market segments
        
        The system provides comprehensive coverage of financial reporting, enabling detailed analysis
        of company performance, financial health, and market trends. The integration of multiple
        data sources creates a powerful platform for investment research and financial analysis.
        
        Data Quality: The system maintains high data quality with {stats.get('unified_downloaded', 0)/max(stats.get('unified_total', 1), 1)*100:.1f}% 
        successful PDF download rate and comprehensive coverage across multiple exchanges and market segments.
        
        The three example companies (Air New Zealand, Qantas, NVIDIA) demonstrate the system's
        ability to handle different market segments, exchanges, and company sizes effectively.
        """
        
        story.append(Paragraph(conclusion_text, styles['Normal']))
        
        # Build PDF
        doc.build(story)
        return str(self.output_file)

    def generate_enhanced_report(self):
        """Generate the enhanced report"""
        print("Generating enhanced database scale report...")
        print(f"Analyzing data for: {', '.join(self.companies.keys())}")
        
        try:
            output_file = self.create_enhanced_pdf_report()
            print(f"Enhanced report generated successfully: {output_file}")
            return output_file
        except Exception as e:
            print(f"Error generating enhanced report: {e}")
            return None

if __name__ == "__main__":
    reporter = EnhancedDatabaseReporter()
    output_file = reporter.generate_enhanced_report()
    
    if output_file:
        print(f"\nEnhanced report saved to: {output_file}")
        print("The report demonstrates the comprehensive scale and integration of all data sources.")
        print("Includes detailed analysis of AIR, QAN, and NVDA across all available data sources.")
    else:
        print("Failed to generate enhanced report.")
