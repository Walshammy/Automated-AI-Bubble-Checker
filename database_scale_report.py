#!/usr/bin/env python3
"""
Comprehensive Database Scale Report Generator
Creates a PDF report demonstrating the scale and integration of all data sources
using AIR New Zealand, Qantas, and NVIDIA as examples
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

class DatabaseScaleReporter:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.downloads_dir = Path.home() / "Downloads"
        
        # Database paths
        self.unified_db = self.base_dir / "consolidated_data" / "unified_financial_data.db"
        self.asx_db = self.base_dir / "asx_scraper" / "asx_data" / "asx_announcements.db"
        self.nzx_db = self.base_dir / "Balance_Sheet_Scraper" / "balance_sheet_data" / "nzx_financial_data.db"
        self.stock_db = self.base_dir / "data_collection" / "unified_stock_data.db"
        self.valuation_db = self.base_dir / "valuation_analysis" / "stock_valuation_data.db"
        
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
        self.output_file = self.downloads_dir / f"Database_Scale_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"

    def get_database_stats(self):
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
                
                # By exchange
                cursor.execute('''
                    SELECT exchange, COUNT(*) as total, 
                           SUM(CASE WHEN is_financial_report = 1 THEN 1 ELSE 0 END) as financial,
                           SUM(CASE WHEN is_balance_sheet = 1 THEN 1 ELSE 0 END) as balance_sheets,
                           SUM(CASE WHEN download_status = "downloaded" THEN 1 ELSE 0 END) as downloaded,
                           COUNT(DISTINCT ticker) as companies
                    FROM financial_announcements 
                    GROUP BY exchange
                ''')
                
                stats['by_exchange'] = {}
                for row in cursor.fetchall():
                    exchange, total, financial, balance_sheets, downloaded, companies = row
                    stats['by_exchange'][exchange] = {
                        'total': total, 'financial': financial, 'balance_sheets': balance_sheets,
                        'downloaded': downloaded, 'companies': companies
                    }
                
            except Exception as e:
                print(f"Error reading unified database: {e}")
            finally:
                conn.close()
        
        # Stock data stats
        if self.stock_db.exists():
            conn = sqlite3.connect(self.stock_db)
            cursor = conn.cursor()
            
            try:
                cursor.execute('SELECT COUNT(*) FROM stock_data')
                stats['stock_total'] = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(DISTINCT ticker) FROM stock_data')
                stats['stock_companies'] = cursor.fetchone()[0]
                
                cursor.execute('SELECT MIN(date), MAX(date) FROM stock_data')
                date_range = cursor.fetchone()
                stats['stock_date_range'] = date_range
                
            except Exception as e:
                print(f"Error reading stock database: {e}")
            finally:
                conn.close()
        
        # Valuation data stats
        if self.valuation_db.exists():
            conn = sqlite3.connect(self.valuation_db)
            cursor = conn.cursor()
            
            try:
                cursor.execute('SELECT COUNT(*) FROM valuation_data')
                stats['valuation_total'] = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(DISTINCT ticker) FROM valuation_data')
                stats['valuation_companies'] = cursor.fetchone()[0]
                
            except Exception as e:
                print(f"Error reading valuation database: {e}")
            finally:
                conn.close()
        
        return stats

    def get_company_data(self, ticker):
        """Get comprehensive data for a specific company"""
        company_data = {
            'ticker': ticker,
            'name': self.companies.get(ticker, {}).get('name', ticker),
            'exchange': self.companies.get(ticker, {}).get('exchange', 'Unknown'),
            'financial_announcements': [],
            'stock_data': [],
            'valuation_data': [],
            'pdf_files': [],
            'summary': {}
        }
        
        # Financial announcements
        if self.unified_db.exists():
            conn = sqlite3.connect(self.unified_db)
            cursor = conn.cursor()
            
            try:
                cursor.execute('''
                    SELECT announcement_id, title, announcement_date, url, pdf_filename, 
                           download_status, is_financial_report, is_balance_sheet
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
                
            except Exception as e:
                print(f"Error reading financial data for {ticker}: {e}")
            finally:
                conn.close()
        
        # Stock data
        if self.stock_db.exists():
            conn = sqlite3.connect(self.stock_db)
            cursor = conn.cursor()
            
            try:
                cursor.execute('''
                    SELECT date, open, high, low, close, volume, market_cap
                    FROM stock_data 
                    WHERE ticker = ?
                    ORDER BY date DESC
                    LIMIT 50
                ''', (ticker,))
                
                stock_data = cursor.fetchall()
                company_data['stock_data'] = stock_data
                
                if stock_data:
                    company_data['summary']['latest_price'] = stock_data[0][4]
                    company_data['summary']['latest_market_cap'] = stock_data[0][6]
                    company_data['summary']['stock_data_points'] = len(stock_data)
                
            except Exception as e:
                print(f"Error reading stock data for {ticker}: {e}")
            finally:
                conn.close()
        
        # Valuation data
        if self.valuation_db.exists():
            conn = sqlite3.connect(self.valuation_db)
            cursor = conn.cursor()
            
            try:
                cursor.execute('''
                    SELECT date, pe_ratio, pb_ratio, debt_to_equity, current_ratio, roe
                    FROM valuation_data 
                    WHERE ticker = ?
                    ORDER BY date DESC
                    LIMIT 20
                ''', (ticker,))
                
                valuation_data = cursor.fetchall()
                company_data['valuation_data'] = valuation_data
                
                if valuation_data:
                    company_data['summary']['latest_pe'] = valuation_data[0][1]
                    company_data['summary']['latest_pb'] = valuation_data[0][2]
                    company_data['summary']['valuation_data_points'] = len(valuation_data)
                
            except Exception as e:
                print(f"Error reading valuation data for {ticker}: {e}")
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

    def create_pdf_report(self):
        """Create comprehensive PDF report"""
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
        story.append(Paragraph("Financial Database Scale Report", title_style))
        story.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))
        story.append(Spacer(1, 20))
        
        # Executive Summary
        story.append(Paragraph("Executive Summary", heading_style))
        stats = self.get_database_stats()
        
        summary_text = f"""
        This report demonstrates the comprehensive scale and integration of our financial data collection system.
        Our unified database contains {stats.get('unified_total', 0):,} financial announcements across 
        {stats.get('unified_companies', 0)} companies, with {stats.get('unified_balance_sheets', 0):,} balance sheet reports
        and {stats.get('unified_downloaded', 0):,} downloaded PDF documents.
        
        The system integrates data from multiple exchanges (ASX, NZX) and includes stock market data,
        valuation metrics, and comprehensive financial reporting. This report showcases three representative
        companies: Air New Zealand (NZX), Qantas Airways (ASX), and NVIDIA Corporation (NASDAQ).
        """
        
        story.append(Paragraph(summary_text, styles['Normal']))
        story.append(Spacer(1, 20))
        
        # Database Overview
        story.append(Paragraph("Database Overview", heading_style))
        
        # Create overview table
        overview_data = [
            ['Metric', 'Count', 'Description'],
            ['Total Financial Announcements', f"{stats.get('unified_total', 0):,}", 'All financial reports collected'],
            ['Financial Reports', f"{stats.get('unified_financial', 0):,}", 'Reports marked as financial'],
            ['Balance Sheet Reports', f"{stats.get('unified_balance_sheets', 0):,}", 'Specific balance sheet documents'],
            ['Downloaded PDFs', f"{stats.get('unified_downloaded', 0):,}", 'Successfully downloaded documents'],
            ['Companies Covered', f"{stats.get('unified_companies', 0):,}", 'Unique companies in database'],
            ['Stock Data Points', f"{stats.get('stock_total', 0):,}", 'Historical stock market data'],
            ['Valuation Data Points', f"{stats.get('valuation_total', 0):,}", 'Financial ratio calculations']
        ]
        
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
        
        # Exchange Breakdown
        if 'by_exchange' in stats:
            story.append(Paragraph("Data by Exchange", subheading_style))
            
            exchange_data = [['Exchange', 'Total', 'Financial', 'Balance Sheets', 'Downloaded', 'Companies']]
            for exchange, data in stats['by_exchange'].items():
                exchange_data.append([
                    exchange,
                    f"{data['total']:,}",
                    f"{data['financial']:,}",
                    f"{data['balance_sheets']:,}",
                    f"{data['downloaded']:,}",
                    f"{data['companies']:,}"
                ])
            
            exchange_table = Table(exchange_data, colWidths=[1*inch, 0.8*inch, 0.8*inch, 0.8*inch, 0.8*inch, 0.8*inch])
            exchange_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            story.append(exchange_table)
            story.append(Spacer(1, 20))
        
        # Company Analysis
        for ticker in ['AIR', 'QAN', 'NVDA']:
            story.append(PageBreak())
            company_data = self.get_company_data(ticker)
            
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
                ['Stock Data Points', f"{company_data['summary'].get('stock_data_points', 0):,}"],
                ['Valuation Data Points', f"{company_data['summary'].get('valuation_data_points', 0):,}"]
            ]
            
            if company_data['summary'].get('latest_price'):
                summary_data.append(['Latest Price', f"${company_data['summary']['latest_price']:.2f}"])
            
            if company_data['summary'].get('latest_market_cap'):
                summary_data.append(['Latest Market Cap', f"${company_data['summary']['latest_market_cap']:,.0f}"])
            
            if company_data['summary'].get('latest_pe'):
                summary_data.append(['Latest P/E Ratio', f"{company_data['summary']['latest_pe']:.2f}"])
            
            if company_data['summary'].get('latest_pb'):
                summary_data.append(['Latest P/B Ratio', f"{company_data['summary']['latest_pb']:.2f}"])
            
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
            if company_data['financial_announcements']:
                story.append(Paragraph("Recent Financial Announcements", subheading_style))
                
                # Show first 10 announcements
                recent_announcements = company_data['financial_announcements'][:10]
                announcement_data = [['Date', 'Title', 'Type', 'Status']]
                
                for ann in recent_announcements:
                    ann_id, title, date, url, pdf_filename, status, is_financial, is_balance_sheet = ann
                    
                    # Determine type
                    if is_balance_sheet:
                        ann_type = "Balance Sheet"
                    elif is_financial:
                        ann_type = "Financial Report"
                    else:
                        ann_type = "General"
                    
                    # Truncate title if too long
                    display_title = title[:50] + "..." if len(title) > 50 else title
                    
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
            
            # Stock Data Sample
            if company_data['stock_data']:
                story.append(Paragraph("Recent Stock Data", subheading_style))
                
                stock_sample = company_data['stock_data'][:5]
                stock_data = [['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
                
                for stock in stock_sample:
                    date, open_price, high, low, close, volume, market_cap = stock
                    stock_data.append([
                        date[:10] if date else 'N/A',
                        f"${open_price:.2f}" if open_price else 'N/A',
                        f"${high:.2f}" if high else 'N/A',
                        f"${low:.2f}" if low else 'N/A',
                        f"${close:.2f}" if close else 'N/A',
                        f"{volume:,}" if volume else 'N/A'
                    ])
                
                stock_table = Table(stock_data, colWidths=[1*inch, 0.8*inch, 0.8*inch, 0.8*inch, 0.8*inch, 1*inch])
                stock_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                
                story.append(stock_table)
                story.append(Spacer(1, 12))
        
        # Conclusion
        story.append(PageBreak())
        story.append(Paragraph("Conclusion", heading_style))
        
        conclusion_text = f"""
        This report demonstrates the comprehensive scale and integration of our financial data collection system.
        
        Key Achievements:
        • Unified database containing {stats.get('unified_total', 0):,} financial announcements
        • {stats.get('unified_balance_sheets', 0):,} balance sheet reports across {stats.get('unified_companies', 0)} companies
        • {stats.get('unified_downloaded', 0):,} successfully downloaded PDF documents
        • Integration of multiple data sources: financial announcements, stock data, and valuation metrics
        • Cross-exchange coverage including ASX, NZX, and international markets
        
        The system provides comprehensive coverage of financial reporting, enabling detailed analysis
        of company performance, financial health, and market trends. The integration of multiple
        data sources creates a powerful platform for investment research and financial analysis.
        
        Data Quality: The system maintains high data quality with {stats.get('unified_downloaded', 0)/max(stats.get('unified_total', 1), 1)*100:.1f}% 
        successful PDF download rate and comprehensive coverage across multiple exchanges.
        """
        
        story.append(Paragraph(conclusion_text, styles['Normal']))
        
        # Build PDF
        doc.build(story)
        return str(self.output_file)

    def generate_report(self):
        """Generate the complete report"""
        print("Generating comprehensive database scale report...")
        print(f"Analyzing data for: {', '.join(self.companies.keys())}")
        
        try:
            output_file = self.create_pdf_report()
            print(f"Report generated successfully: {output_file}")
            return output_file
        except Exception as e:
            print(f"Error generating report: {e}")
            return None

if __name__ == "__main__":
    reporter = DatabaseScaleReporter()
    output_file = reporter.generate_report()
    
    if output_file:
        print(f"\nReport saved to: {output_file}")
        print("The report demonstrates the comprehensive scale and integration of all data sources.")
    else:
        print("Failed to generate report.")
