"""
Financial Statement Processor
Advanced PDF parsing and extraction of balance sheet and financial data
"""

import pdfplumber
import pandas as pd
import re
from datetime import datetime
import logging
from typing import Dict, List, Any, Optional
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class FinancialStatementProcessor:
    """Advanced processor for financial statement PDFs"""
    
    def __init__(self):
        # Comprehensive financial terms dictionary
        self.financial_terms = {
            # Balance Sheet - Assets
            'total_assets': [
                r'total\s+assets?',
                r'consolidated?\s+total\s+assets?',
                r'crown\'?s?\s+assets?'
            ],
            'current_assets': [
                r'current\s+assets?',
                r'consolidated?\s+current\s+assets?'
            ],
            'non_current_assets': [
                r'non[-\s]?current\s+assets?',
                r'fixed\s+assets?',
                r'consolidated?\s+non[-\s]?current\s+assets?'
            ],
            'cash_and_equivalents': [
                r'cash\s+and\s+cash\s+equivalents?',
                r'cash\s+and\s+equivalents?',
                r'cash',
                r'cash\s+(?:at\s+)?bank'
            ],
            'accounts_receivable': [
                r'trade\s+and\s+other\s+receivables?',
                r'accounts?\s+receivable',
                r'trade\s+receivables?',
                r'sundry\s+debtors?'
            ],
            'inventory': [
                r'inventories?',
                r'stock',
                r'goods?\s+for\s+sale'
            ],
            
            # Balance Sheet - Liabilities
            'total_liabilities': [
                r'total\s+liabilities?',
                r'consolidated?\s+total\s+liabilities?'
            ],
            'current_liabilities': [
                r'current\s+liabilities?',
                r'consolidated?\s+current\s+liabilities?'
            ],
            'non_current_liabilities': [
                r'non[-\s]?current\s+liabilities?',
                r'consolidated?\s+non[-\s]?current\s+liabilities?'
            ],
            'accounts_payable': [
                r'trade\s+and\s+other\s+payables?',
                r'accounts?\s+payable',
                r'trade\s+payables?',
                r'sundry\s+creditors?'
            ],
            'long_term_debt': [
                r'long[-\s]?term\s+borrowings?',
                r'long[-\s]?term\s+debt',
                r'term\s+loans?',
                r'bank\s+loans?(?:\s+payable)?'
            ],
            
            # Equity
            'total_equity': [
                r'total\s+equity',
                r'shareholders?\'?\s+equity',
                r'total\s+shareholders?\'?\s+(?:funds?|equity)',
                r'net\s+worth'
            ],
            'retained_earnings': [
                r'retained\s+(?:earnings?|surplus)',
                r'accrual\s+surplus',
                r'accumulated?\s+(?:earnings?|surplus)'
            ],
            
            # Profit & Loss / Income Statement
            'revenue': [
                r'(?:total\s+)?revenue',
                r'turnover',
                r'sales?\s+(?:revenue)?',
                r'consolidated?\s+revenue'
            ],
            'gross_profit': [
                r'gross\s+(?:profit|margin)',
                r'profit(?:\s+before)?(?:.*?\s*depreciation)'
            ],
            'operating_income': [
                r'operating\s+(?:profit|income|result)',
                r'operating\s+profit\s+before\s+(?:interest\s+and\s+)?tax'
            ],
            'ebitda': [
                r'ebitda',
                r'(?:earnings?\s+before\s+(?:interest,\s+)?tax.*?)?depreciation.*?amortisation',
                r'(?:earnings?\s+before\s+(?:interest,\s+)?tax.*?)?depreciation.*?amortization'
            ],
            'ebit': [
                r'ebit',
                r'(?:earnings?\s+)(?:before\s+(?:interest\s+and\s+)?tax|from\s+operations)',
                r'operating\s+profit\s+before\s+(?:interest\s+and\s+)?tax'
            ],
            'net_income': [
                r'net\s+(?:profit|income|earnings?|result)',
                r'(?:profit|income|earnings?)\s+(?:after\s+)?tax',
                r'(?:consolidated\s+)?net\s+(?:profit|income|earnings?)'
            ],
            
            # Cash Flow
            'operating_cash_flow': [
                r'(?:net\s+)?cash\s+(?:provided|generated)\s+by\s+operating\s+activities?',
                r'operating\s+cash\s+flow',
                r'cash\s+from\s+operations'
            ],
            'investing_cash_flow': [
                r'(?:net\s+)?cash\s+(?:provided|used)\s+by\s+investing\s+activities?',
                r'investing\s+cash\s+flow',
                r'cash\s+(?:used|invested)\s+in\s+investing'
            ],
            'financing_cash_flow': [
                r'(?:net\s+)?cash\s+(?:provided|used)\s+by\s+financing\s+activities?',
                r'financing\s+cash\s+flow',
                r'cash\s+(?:provided|received)\s+from\s+financing'
            ],
            'free_cash_flow': [
                r'free\s+cash\s+flow',
                r'fcf',
                r'operating\s+cash\s+flow\s+(?:less|minus)\s+(?:capital\s+)?expenditure'
            ]
        }
        
        # Number extraction patterns
        self.number_patterns = [
            r'[\$]?([\d,]+\.?\d*)\s*(?:million|m|billion|b|thousand|k)?',
            r'([\d,]+\.?\d*)\s*(?:million|m|billion|b|thousand|k)',
            r'[\$]([\d,]+\.?\d*)',
            r'\((?:\$)?([\d,]+\.?\d*)\)',  # Negative numbers in parentheses
            r'\-[\$]?([\d,]+\.?\d*)',      # Negative numbers with minus sign
        ]
    
    def extract_comprehensive_financial_data(self, pdf_path: str, ticker: str, 
                                            announcement_id: str, report_date: str,
                                            report_type: str) -> Optional[Dict[str, Any]]:
        """
        Extract comprehensive financial data from PDF
        """
        try:
            with pdfplumber.open(pdf_path, password=None) as pdf:
                all_text = ""
                all_tables = []
                
                # Extract text and tables from all pages
                for page in pdf.pages:
                    page_text = صفحة.extract_text() or ""
                    all_text += page_text + "\n"
                    
                    page_tables = page.extract_tables() or []
                    all_tables.extend(page_tables)
                
                # Extract key metrics
                extracted_metrics = self._extract_key_metrics(all_text)
                
                # Process tables for structured data
                table_data = self._process_financial_tables(all_tables)
                
                # Combine extracted data
                financial_data = {
                    'ticker': ticker,
                    'announcement_id': announcement_id,
                    'report_date': self._parse_date(report_date),
                    'report_type': report_type,
                    'statement_type': self._determine_statement_type(all_text),
                    'extraction_confidence': self._calculate_confidence(extracted_metrics),
                    'data_source': 'PDF_text_extraction'
                }
                
                # Add extracted metrics
                financial_data.update(extracted_metrics)
                
                # Add calculated ratios
                ratios = self._calculate_financial_ratios(financial_data)
                financial_data.update(ratios)
                
                # Validate extracted data
                if self._validate_financial_data(financial_data):
                    logging.info(f"Successfully extracted financial data for {ticker}")
                    return financial_data
                else:
                    logging.warning(f"Financial data validation failed for {ticker}")
                    return None
                
        except Exception as e:
            logging.error(f"Error processing PDF {pdf_path}: {e}")
            return None
    
    def _extract_key_metrics(self, text: str) -> Dict[str, float]:
        """
        Extract financial metrics using advanced pattern matching
        """
        extracted = {}
        text_lower = text.lower()
        
        # Clean text for better matching
        cleaned_text = re.sub(r'\s+', ' ', text_lower)
        
        for metric_name, patterns in self.financial_terms.items():
            best_match = None
            best_value = None
            
            for pattern in patterns:
                # Search for the financial term
                term_matches = re.finditer(pattern, cleaned_text)
                
                for match in term_matches:
                    # Get context around the find (100 chars before and after)
                    start = max(0, match.start() - 100)
                    end = min(len(cleaned_text), match.end() + 100)
                    context = cleaned_text[start:end]
                    
                    # Look for numbers in this context
                    for number_pattern in self.number_patterns:
                        number_matches = re.finditer(number_pattern, context)
                        
                        for number_match in number_matches:
                            try:
                                value_str = number_match.group(1).replace(',', '')
                                value = float(value_str)
                                
                                # Handle multipliers
                                context_after = context[number_match.end():number_match.end() + 20].lower()
                                if 'million' in context_after or 'm' in context_after:
                                    value *= 1_000_000
                                elif 'billion' in context_after or 'b' in context_after:
                                    value *= 1_000_000_000
                                elif 'thousand' in context_after or 'k' in context_after:
                                    value *= 1_000
                                
                                # Handle negative values
                                if '(' in context or 'negative' in context or '-' in context_after[:10]:
                                    value = -abs(value)
                                
                                if best_match is None or abs(value) > abs(best_value or 0):
                                    best_match = context
                                    best_value = value
                                    
                            except (ValueError, AttributeError):
                                continue
            
            if best_value is not None:
                extracted[metric_name] = best_value
                
        return extracted
    
    def _process_financial_tables(self, tables: List[List]) -> Dict[str, Any]:
        """
        Process extracted tables to find financial data
        """
        table_data = {}
        
        for table in tables:
            if not table or len(table) < 2:
                continue
            
            # Convert table to DataFrame for easier processing
            try:
                df = pd.DataFrame(table[1:], columns=table[0] if table[0] else None)
                
                # Look for financial data in the table
                for col in df.columns:
                    if isinstance(col, str):
                        col_lower = col.lower()
                        
                        # Check if column contains financial numbers
                        for metric_name, patterns in self.financial_terms.items():
                            if any(pattern.lower().replace(r'\s+', ' ') in col_lower for pattern in patterns[:1]):
                                # Extract values from this column
                                for value in df[col]:
                                    if isinstance(value, str) and re.search(r'\d', value):
                                        try:
                                            num_value = self._parse_financial_number(value)
                                            if num_value is not None:
                                                table_data[f"{metric_name}_table"] = num_value
                                                break
                                        except:
                                            continue
                                
            except Exception as e:
                continue
        
        return table_data
    
    def _parse_financial_number(self, text: str) -> Optional[float]:
        """
        Parse financial number from text
        """
        if not isinstance(text, str):
            return None
        
        # Clean the text
        clean_text = re.sub(r'[^\d,\.\$()-]', '', text)
        
        for pattern in self.number_patterns:
            match = re.search(pattern, clean_text)
            if match:
                try:
                    value_str = match.group(1).replace(',', '')
                    value = float(value_str)
                    
                    # Handle context for multipliers and negatives
                    context = clean_text[max(0, match.start()):min(len(clean_text), match.end() + 10)]
                    
                    if 'million' in context or '(m)' in context:
                        value *= 1_000_000
                    elif 'billion' in context or '(b)' in context:
                        value *= 1_000_000_000
                    elif 'thousand' in context or '(k)' in context or '(000)' in context:
                        value *= 1_000
                    
                    if '(' in context or '-' in context:
                        value = -abs(value)
                    
                    return value
                    
                except ValueError:
                    continue
        
        return None
    
    def _calculate_financial_ratios(self, data: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate financial ratios from extracted data
        """
        ratios = {}
        
        try:
            # Liquidity ratios
            if data.get('current_assets') and data.get('current_liabilities'):
                ratios['current_ratio'] = data['current_assets'] / data['current_liabilities']
            
            # Quick ratio (assuming cash + receivables are current assets)
            cash_recv = (data.get('cash_and_equivalents', 0) or 0) + (data.get('accounts_receivable', 0) or 0)
            if cash_recv and data.get('current_liabilities'):
                ratios['quick_ratio'] = cash_recv / data['current_liabilities']
            
            # Leverage ratios
            if data.get('total_liabilities') and data.get('total_equity'):
                ratios['debt_to_equity'] = data['total_liabilities'] / data['total_equity']
            
            # Profitability ratios
            if data.get('net_income') and data.get('total_assets'):
                ratios['return_on_assets'] = data['net_income'] / data['total_assets']
            
            if data.get('net_income') and data.get('total_equity'):
                ratios['return_on_equity'] = data['net_income'] / data['total_equity']
            
            # Margin ratios
            if data.get('revenue'):
                if data.get('gross_profit'):
                    ratios['gross_margin'] = data['gross_profit'] / data['revenue']
                if data.get('operating_income'):
                    ratios['operating_margin'] = data['operating_income'] / data['revenue']
                if data.get('net_income'):
                    ratios['net_margin'] = data['net_income'] / data['revenue']
            
        except (ZeroDivisionError, TypeError):
            pass
        
        return ratios
    
    def _determine_statement_type(self, text: str) -> str:
        """
        Determine the type of financial statement
        """
        text_lower = text.lower()
        
        if any(term in text_lower for term in ['annual report', 'audited', 'consolidated']):
            return 'annual_report'
        elif any(term in text_lower for term in ['interim', 'quarterly', 'q1', 'q2', 'q3', 'q4']):
            return 'interim_report'
        elif any(term in text_lower for term in ['results', 'financial statements']):
            return 'financial_statements'
        else:
            return 'other'
    
    def _parse_date(self, date_str: str) -> str:
        """
        Parse and clean date string
        """
        try:
            # Common date formats in NZX announcements
            date_formats = [
                '%d %b %Y',  # 01 Jan 2024
                '%d %B %Y',  # 01 January 2024
                '%d/%m/%Y',  # 01/01/2024
                '%Y-%m-%d',  # 2024-01-01
                '%d-%m-%Y'   # 01-01-2024
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            # If no format matches, return as-is after cleaning
            return re.sub(r'[^\d/\- ]', '', date_str).strip()
            
        except:
            return date_str
    
    def _calculate_confidence(self, extracted_metrics: Dict[str, float]) -> float:
        """
        Calculate extraction confidence based on number of successful extractions
        """
        expected_metrics = ['total_assets', 'total_liabilities', 'total_equity', 'revenue']
        extracted_count = sum(1 for metric in expected_metrics if metric in extracted_metrics)
        
        return extracted_count / len(expected_metrics)
    
    def _validate_financial_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate financial data for reasonableness
        """
        validation_checks = []
        
        # Check basic accounting equation (Assets = Liabilities + Equity)
        assets = data.get('total_assets')
        liabilities = data.get('total_liabilities')
        equity = data.get('total_equity')
        
        if all(x is not None and x != 0 for x in [assets, liabilities, equity]):
            equation_diff = abs(assets - (liabilities + equity)) / assets
            validation_checks.append(equation_diff < 0.1)  # Within 10% tolerance
        
        # Check that current assets <= total assets
        if data.get('current_assets') and assets:
            validation_checks.append(data['current_assets'] <= assets)
        
        # Check that current liabilities <= total liabilities
        if data.get('current_liabilities') and liabilities:
            validation_checks.append(data['current_liabilities'] <= liabilities)
        
        # Must have at least basic metrics
        basic_metrics = ['ticker', 'announcement_id', 'report_date']
        validation_checks.append(all(data.get(metric) for metric in basic_metrics))
        
        # At least 3 financial metrics extracted
        financial_metrics = [key for key in data.keys() if key in self.financial_terms]
        validation_checks.append(len(financial_metrics) >= 3)
        
        return all(validation_checks)

if __name__ == "__main__":
    # Test the processor
    processor = FinancialStatementProcessor()
    
    # Test financial term matching
    test_text = """
    CONSOLIDATED STATEMENT OF FINANCIAL POSITION
    Total Assets                    $1,234.5 million
    Current Assets                  $456.7 million  
    Cash and cash equivalents       $123.4 million
    Trade and other receivables     $89.6 million
    Total Liabilities              $789.1 million
    Current Liabilities            $234.5 million
    Trade and other payables       $156.7 million
    Total Equity                   $445.4 million
    """
    
    metrics = processor._extract_key_metrics(test_text)
    print("Extracted metrics:")
    for key, value in metrics.items():
        print(f"  {key}: {value}")
