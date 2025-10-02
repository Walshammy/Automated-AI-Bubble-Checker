import pandas as pd
import numpy as np
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import time
import os
from datetime import datetime, timedelta
import logging
import json
import re
from openpyxl import load_workbook, Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
import warnings
import functools
from typing import Dict, Optional, Tuple, List, Union
from tqdm import tqdm
from dataclasses import dataclass
from enum import Enum

warnings.filterwarnings('ignore')

class Sector(Enum):
    TECHNOLOGY = "Technology"
    FINANCIAL = "Financial"
    HEALTHCARE = "Healthcare"
    INDUSTRIAL = "Industrial"
    CONSUMER = "Consumer"
    ENERGY = "Energy"
    UTILITIES = "Utilities"
    REAL_ESTATE = "Real Estate"
    MATERIALS = "Materials"
    COMMUNICATION = "Communication"
    UNKNOWN = "Unknown"

@dataclass
class ValuationResult:
    """Structured result for each valuation method"""
    method: str
    intrinsic_value: float
    current_price: float
    margin_of_safety: float  # Percentage discount/premium
    confidence: float  # 0-1 scale
    assumptions: Dict[str, Union[float, str]]
    warnings: List[str]

@dataclass
class QualityMetrics:
    """Business quality assessment metrics"""
    earnings_quality: float  # 0-1 scale
    balance_sheet_strength: float  # 0-1 scale
    competitive_position: float  # 0-1 scale
    management_quality: float  # 0-1 scale (proxy metrics)
    overall_quality_score: float  # 0-1 scale

class DataValidator:
    """Comprehensive data validation and quality assessment"""
    
    @staticmethod
    def validate_earnings_data(data: Dict) -> Tuple[bool, List[str]]:
        """Validate earnings data for consistency and recency"""
        warnings = []
        
        # Check data recency
        if 'earnings_date' in data:
            earnings_date = pd.to_datetime(data['earnings_date'])
            days_old = (datetime.now() - earnings_date).days
            
            if days_old > 365:
                warnings.append(f"Earnings data is {days_old} days old")
            elif days_old > 180:
                warnings.append(f"Earnings data is {days_old} days old (stale)")
        
        # Check for one-time items
        if 'non_recurring_items' in data and data['non_recurring_items'] != 0:
            warnings.append("Non-recurring items detected in earnings")
        
        # Validate EPS consistency
        if 'eps' in data and 'eps_ttm' in data:
            if abs(data['eps'] - data['eps_ttm']) / abs(data['eps_ttm']) > 0.5:
                warnings.append("Large discrepancy between quarterly and TTM EPS")
        
        return len(warnings) == 0, warnings
    
    @staticmethod
    def validate_fcf_data(data: Dict) -> Tuple[bool, List[str]]:
        """Validate free cash flow data"""
        warnings = []
        
        # Check for negative FCF trends
        if 'fcf_ttm' in data and 'fcf_prev_year' in data:
            if data['fcf_ttm'] < 0 and data['fcf_prev_year'] < 0:
                warnings.append("Consistently negative FCF")
            elif data['fcf_ttm'] < data['fcf_prev_year'] * 0.5:
                warnings.append("FCF declined significantly year-over-year")
        
        # Check FCF vs earnings consistency
        if 'fcf_ttm' in data and 'net_income' in data:
            fcf_conversion = data['fcf_ttm'] / data['net_income'] if data['net_income'] > 0 else 0
            if fcf_conversion < 0.5:
                warnings.append("Low FCF conversion ratio")
        
        return len(warnings) == 0, warnings

class SectorSpecificValuation:
    """Sector-specific valuation adjustments and models"""
    
    @staticmethod
    def get_sector_multipliers(sector: Sector) -> Dict[str, float]:
        """Get sector-specific valuation multipliers"""
        multipliers = {
            Sector.TECHNOLOGY: {
                'dcf_growth_premium': 1.2,  # Tech companies can sustain higher growth
                'terminal_growth_rate': 0.03,  # Higher terminal growth
                'discount_rate_adjustment': -0.01,  # Lower discount rate for growth
                'lynch_applicable': False,  # Lynch method less applicable
            },
            Sector.FINANCIAL: {
                'dcf_growth_premium': 0.8,  # Banks have cyclical growth
                'terminal_growth_rate': 0.015,  # Lower terminal growth
                'discount_rate_adjustment': 0.01,  # Higher discount rate for risk
                'lynch_applicable': False,  # Use P/B instead
                'preferred_method': 'pbr',  # Price-to-book ratio
            },
            Sector.UTILITIES: {
                'dcf_growth_premium': 0.6,  # Low growth, stable
                'terminal_growth_rate': 0.01,  # Very low terminal growth
                'discount_rate_adjustment': 0.005,  # Slightly higher discount
                'lynch_applicable': False,  # Dividend yield model better
                'preferred_method': 'dividend_discount',
            },
            Sector.REAL_ESTATE: {
                'dcf_growth_premium': 0.7,  # Moderate growth
                'terminal_growth_rate': 0.02,  # Moderate terminal growth
                'discount_rate_adjustment': 0.005,
                'lynch_applicable': False,  # FFO-based models better
                'preferred_method': 'ffo_multiple',
            },
            Sector.HEALTHCARE: {
                'dcf_growth_premium': 1.1,  # Above average growth potential
                'terminal_growth_rate': 0.025,  # Moderate-high terminal growth
                'discount_rate_adjustment': -0.005,
                'lynch_applicable': True,
            },
            Sector.INDUSTRIAL: {
                'dcf_growth_premium': 0.9,  # Slightly below average
                'terminal_growth_rate': 0.02,  # Moderate terminal growth
                'discount_rate_adjustment': 0.0,
                'lynch_applicable': True,
            },
            Sector.CONSUMER: {
                'dcf_growth_premium': 0.95,  # Market-like growth
                'terminal_growth_rate': 0.02,  # Moderate terminal growth
                'discount_rate_adjustment': 0.0,
                'lynch_applicable': True,
            },
        }
        
        return multipliers.get(sector, {
            'dcf_growth_premium': 1.0,
            'terminal_growth_rate': 0.02,
            'discount_rate_adjustment': 0.0,
            'lynch_applicable': True,
        })

class ImprovedDCFValuation:
    """Properly implemented DCF with sector adjustments and validation"""
    
    def __init__(self, sector: Sector, risk_free_rate: float = 0.04):
        self.sector = sector
        self.risk_free_rate = risk_free_rate
        self.sector_multipliers = SectorSpecificValuation.get_sector_multipliers(sector)
    
    def calculate_wacc(self, beta: float, market_return: float = 0.10) -> float:
        """Calculate Weighted Average Cost of Capital"""
        # Simplified WACC calculation
        if beta is None or np.isnan(beta) or beta <= 0:
            beta = 1.0  # Default beta if invalid
        
        cost_of_equity = self.risk_free_rate + beta * (market_return - self.risk_free_rate)
        cost_of_equity += self.sector_multipliers['discount_rate_adjustment']
        return max(cost_of_equity, 0.05)  # Minimum 5% discount rate
    
    def estimate_fcf_growth(self, data: Dict) -> Tuple[float, List[str]]:
        """Properly estimate FCF growth based on business fundamentals"""
        warnings = []
        
        # Start with historical FCF growth if available
        historical_growth = 0.05  # Default 5% growth
        
        if 'fcf_growth_3y' in data and data['fcf_growth_3y'] is not None and not np.isnan(data['fcf_growth_3y']):
            historical_growth = data['fcf_growth_3y']
        elif 'fcf_growth_5y' in data and data['fcf_growth_5y'] is not None and not np.isnan(data['fcf_growth_5y']):
            historical_growth = data['fcf_growth_5y']
        elif 'eps_growth_5y' in data and data['eps_growth_5y'] is not None and not np.isnan(data['eps_growth_5y']):
            # Use EPS growth as proxy if FCF growth not available
            historical_growth = data['eps_growth_5y']
            warnings.append("Using EPS growth as FCF growth proxy")
        else:
            warnings.append("No historical growth data available, using default 5%")
        
        # Apply sector-specific adjustments
        sector_adjustment = self.sector_multipliers['dcf_growth_premium']
        adjusted_growth = historical_growth * sector_adjustment
        
        # Cap growth rates based on sector
        if self.sector == Sector.TECHNOLOGY:
            max_growth = 0.25  # 25% max for tech
        elif self.sector == Sector.UTILITIES:
            max_growth = 0.05  # 5% max for utilities
        elif self.sector == Sector.FINANCIAL:
            max_growth = 0.15  # 15% max for financials
        else:
            max_growth = 0.20  # 20% max for others
        
        final_growth = min(adjusted_growth, max_growth)
        
        # Ensure reasonable growth bounds
        final_growth = max(final_growth, 0.01)  # Minimum 1% growth
        final_growth = min(final_growth, 0.30)  # Maximum 30% growth
        
        return final_growth, warnings
    
    def calculate_dcf_value(self, data: Dict) -> ValuationResult:
        """Calculate DCF value with proper methodology"""
        warnings = []
        
        # Validate FCF data
        fcf_valid, fcf_warnings = DataValidator.validate_fcf_data(data)
        warnings.extend(fcf_warnings)
        
        if not fcf_valid or 'fcf_ttm' not in data or data['fcf_ttm'] <= 0:
            return ValuationResult(
                method="DCF",
                intrinsic_value=0,
                current_price=data.get('current_price', 0),
                margin_of_safety=0,
                confidence=0.0,
                assumptions={},
                warnings=["Invalid FCF data for DCF calculation"]
            )
        
        # Get inputs
        current_fcf = data['fcf_ttm']
        current_price = data.get('current_price', 0)
        beta = data.get('beta', 1.0)
        
        # Calculate WACC
        wacc = self.calculate_wacc(beta)
        
        # Estimate growth rate
        growth_rate, growth_warnings = self.estimate_fcf_growth(data)
        warnings.extend(growth_warnings)
        
        # Terminal growth rate
        terminal_growth = self.sector_multipliers['terminal_growth_rate']
        
        # Project FCF for 10 years
        projected_fcf = []
        for year in range(1, 11):
            year_fcf = current_fcf * ((1 + growth_rate) ** year)
            projected_fcf.append(year_fcf)
        
        # Calculate terminal value with safety checks
        terminal_fcf = projected_fcf[-1] * (1 + terminal_growth)
        
        # Ensure terminal growth is less than WACC
        if terminal_growth >= wacc:
            terminal_growth = wacc - 0.01  # Force terminal growth below WACC
            warnings.append("Adjusted terminal growth to be below WACC")
        
        terminal_value = terminal_fcf / (wacc - terminal_growth)
        
        # Sanity check: terminal value shouldn't be more than 50x current FCF
        max_reasonable_terminal = current_fcf * 50
        if terminal_value > max_reasonable_terminal:
            terminal_value = max_reasonable_terminal
            warnings.append("Capped terminal value at 50x current FCF")
        
        # Discount cash flows
        pv_cash_flows = []
        for i, fcf in enumerate(projected_fcf):
            pv = fcf / ((1 + wacc) ** (i + 1))
            pv_cash_flows.append(pv)
        
        pv_terminal = terminal_value / ((1 + wacc) ** 10)
        
        # Calculate intrinsic value
        total_pv = sum(pv_cash_flows) + pv_terminal
        
        # Check terminal value dominance
        terminal_ratio = pv_terminal / total_pv
        if terminal_ratio > 0.8:
            warnings.append(f"Terminal value dominates ({terminal_ratio:.1%}) - consider longer projection period")
        
        # Calculate margin of safety
        margin_of_safety = ((total_pv - current_price) / current_price) * 100 if current_price > 0 else 0
        
        # Confidence based on data quality and assumptions
        confidence = 0.8 if fcf_valid else 0.3
        if terminal_ratio > 0.7:
            confidence *= 0.8  # Reduce confidence for high terminal value dominance
        
        return ValuationResult(
            method="DCF",
            intrinsic_value=total_pv,
            current_price=current_price,
            margin_of_safety=margin_of_safety,
            confidence=confidence,
            assumptions={
                'wacc': wacc,
                'growth_rate': growth_rate,
                'terminal_growth': terminal_growth,
                'terminal_value_ratio': terminal_ratio,
                'projection_years': 10
            },
            warnings=warnings
        )

class ImprovedLynchValuation:
    """Properly implemented Peter Lynch valuation"""
    
    def __init__(self, sector: Sector):
        self.sector = sector
        self.sector_multipliers = SectorSpecificValuation.get_sector_multipliers(sector)
    
    def calculate_lynch_value(self, data: Dict) -> ValuationResult:
        """Calculate Peter Lynch value with proper methodology"""
        warnings = []
        
        # Check if Lynch method is applicable for this sector
        if not self.sector_multipliers.get('lynch_applicable', True):
            return ValuationResult(
                method="Peter Lynch",
                intrinsic_value=0,
                current_price=data.get('current_price', 0),
                margin_of_safety=0,
                confidence=0.0,
                assumptions={},
                warnings=[f"Lynch method not applicable for {self.sector.value} sector"]
            )
        
        # Validate required data
        required_fields = ['eps_ttm', 'pe_ratio', 'eps_growth_5y']
        missing_fields = [field for field in required_fields if field not in data or data[field] is None]
        
        if missing_fields:
            return ValuationResult(
                method="Peter Lynch",
                intrinsic_value=0,
                current_price=data.get('current_price', 0),
                margin_of_safety=0,
                confidence=0.0,
                assumptions={},
                warnings=[f"Missing required data: {missing_fields}"]
            )
        
        eps = data['eps_ttm']
        pe_ratio = data['pe_ratio']
        eps_growth = data['eps_growth_5y']
        
        # Validate data quality
        if eps <= 0:
            warnings.append("Negative or zero EPS")
        if pe_ratio <= 0:
            warnings.append("Negative or zero P/E ratio")
        if eps_growth <= 0:
            warnings.append("Negative EPS growth")
        
        # Calculate Lynch ratio (PEG ratio) with safety checks
        if eps_growth <= 0:
            return ValuationResult(
                method="Peter Lynch",
                intrinsic_value=0,
                current_price=data.get('current_price', 0),
                margin_of_safety=0,
                confidence=0.0,
                assumptions={},
                warnings=["Cannot calculate Lynch value with negative/zero growth"]
            )
        
        lynch_ratio = pe_ratio / (eps_growth * 100)  # Convert growth to decimal
        
        # Lynch's rule: P/E should equal growth rate (but cap at reasonable levels)
        fair_pe = min(eps_growth, 25)  # Cap fair P/E at 25
        intrinsic_value = eps * fair_pe
        
        current_price = data.get('current_price', 0)
        margin_of_safety = ((intrinsic_value - current_price) / current_price) * 100 if current_price > 0 else 0
        
        # Confidence based on data quality and growth consistency
        confidence = 0.7
        if eps_growth > 0.25:  # >25% growth
            confidence *= 0.8  # Reduce confidence for very high growth
        if pe_ratio > 30:
            confidence *= 0.9  # Reduce confidence for high P/E
        
        return ValuationResult(
            method="Peter Lynch",
            intrinsic_value=intrinsic_value,
            current_price=current_price,
            margin_of_safety=margin_of_safety,
            confidence=confidence,
            assumptions={
                'lynch_ratio': lynch_ratio,
                'fair_pe': fair_pe,
                'eps_growth': eps_growth,
                'current_pe': pe_ratio
            },
            warnings=warnings
        )

class QualityAssessment:
    """Comprehensive business quality assessment"""
    
    @staticmethod
    def assess_earnings_quality(data: Dict) -> float:
        """Assess earnings quality (0-1 scale)"""
        score = 0.5  # Start neutral
        
        # FCF conversion ratio
        if 'fcf_ttm' in data and 'net_income' in data and data['net_income'] > 0:
            fcf_conversion = data['fcf_ttm'] / data['net_income']
            if fcf_conversion > 0.8:
                score += 0.2
            elif fcf_conversion > 0.6:
                score += 0.1
            elif fcf_conversion < 0.3:
                score -= 0.2
        
        # Earnings consistency
        if 'eps_growth_5y' in data and 'eps_growth_3y' in data:
            growth_consistency = abs(data['eps_growth_5y'] - data['eps_growth_3y']) / max(abs(data['eps_growth_5y']), 0.01)
            if growth_consistency < 0.2:  # Consistent growth
                score += 0.1
            elif growth_consistency > 0.5:  # Inconsistent growth
                score -= 0.1
        
        # Revenue vs earnings growth alignment
        if 'revenue_growth_5y' in data and 'eps_growth_5y' in data:
            if data['eps_growth_5y'] > data['revenue_growth_5y'] * 1.5:
                score -= 0.1  # Earnings growing much faster than revenue (potential red flag)
        
        return max(0, min(1, score))
    
    @staticmethod
    def assess_balance_sheet_strength(data: Dict) -> float:
        """Assess balance sheet strength (0-1 scale)"""
        score = 0.5  # Start neutral
        
        # Debt-to-equity ratio
        if 'debt_to_equity' in data:
            if data['debt_to_equity'] < 0.3:
                score += 0.2
            elif data['debt_to_equity'] < 0.5:
                score += 0.1
            elif data['debt_to_equity'] > 1.0:
                score -= 0.2
        
        # Current ratio
        if 'current_ratio' in data:
            if data['current_ratio'] > 2.0:
                score += 0.1
            elif data['current_ratio'] < 1.0:
                score -= 0.1
        
        # Interest coverage
        if 'interest_coverage' in data:
            if data['interest_coverage'] > 5.0:
                score += 0.1
            elif data['interest_coverage'] < 2.0:
                score -= 0.1
        
        return max(0, min(1, score))
    
    @staticmethod
    def assess_competitive_position(data: Dict) -> float:
        """Assess competitive position (0-1 scale)"""
        score = 0.5  # Start neutral
        
        # ROE consistency
        if 'roe_5y' in data and 'roe_ttm' in data:
            roe_consistency = abs(data['roe_5y'] - data['roe_ttm']) / max(abs(data['roe_5y']), 0.01)
            if roe_consistency < 0.2:
                score += 0.1
            elif roe_consistency > 0.5:
                score -= 0.1
        
        # High ROE
        if 'roe_ttm' in data:
            if data['roe_ttm'] > 0.15:  # >15% ROE
                score += 0.1
            elif data['roe_ttm'] < 0.05:  # <5% ROE
                score -= 0.1
        
        # Gross margin stability
        if 'gross_margin_5y' in data and 'gross_margin_ttm' in data:
            margin_stability = abs(data['gross_margin_5y'] - data['gross_margin_ttm']) / max(abs(data['gross_margin_5y']), 0.01)
            if margin_stability < 0.1:
                score += 0.1
        
        return max(0, min(1, score))
    
    @staticmethod
    def calculate_overall_quality(data: Dict) -> QualityMetrics:
        """Calculate overall quality metrics"""
        earnings_quality = QualityAssessment.assess_earnings_quality(data)
        balance_sheet_strength = QualityAssessment.assess_balance_sheet_strength(data)
        competitive_position = QualityAssessment.assess_competitive_position(data)
        
        # Management quality proxy (based on capital allocation)
        management_quality = 0.5
        if 'roe_ttm' in data and 'roic_ttm' in data:
            if data['roe_ttm'] > data['roic_ttm'] * 0.8:  # Good capital allocation
                management_quality += 0.2
        
        overall_score = (earnings_quality + balance_sheet_strength + competitive_position + management_quality) / 4
        
        return QualityMetrics(
            earnings_quality=earnings_quality,
            balance_sheet_strength=balance_sheet_strength,
            competitive_position=competitive_position,
            management_quality=management_quality,
            overall_quality_score=overall_score
        )

class ImprovedStockValuationScraper:
    """Improved stock valuation scraper with proper methodology"""
    
    def __init__(self):
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Focus on high-quality stocks only
        self.focus_stocks = {
            # NZX Large Cap Quality Stocks
            'FPH.NZ': 'Fisher & Paykel Healthcare Corporation Limited',
            'MEL.NZ': 'Meridian Energy Limited',
            'AIA.NZ': 'Auckland International Airport Limited',
            'IFT.NZ': 'Infratil Limited',
            'MFT.NZ': 'Mainfreight Limited',
            'ATM.NZ': 'The a2 Milk Company Limited',
            'POT.NZ': 'Port of Tauranga Limited',
            'SPK.NZ': 'Spark New Zealand Limited',
            'VCT.NZ': 'Vector Limited',
            'CNU.NZ': 'Chorus Limited',
            
            # International Quality Stocks
            'BRK-B': 'Berkshire Hathaway Class B',
            'MSFT': 'Microsoft Corporation',
            'AAPL': 'Apple Inc.',
            'GOOGL': 'Alphabet Inc.',
            'JNJ': 'Johnson & Johnson',
            'PG': 'Procter & Gamble Company',
            'KO': 'Coca-Cola Company',
            'PEP': 'PepsiCo Inc.',
            'WMT': 'Walmart Inc.',
            'HD': 'Home Depot Inc.',
        }
        
        # Output directory
        self.output_dir = os.path.join(os.getcwd(), "valuation_results")
        os.makedirs(self.output_dir, exist_ok=True)
    
    def get_sector(self, ticker: str, data: Dict) -> Sector:
        """Determine sector based on ticker and data"""
        # Simple sector mapping - in practice, you'd use a more sophisticated method
        sector_mapping = {
            'FPH.NZ': Sector.HEALTHCARE,
            'MEL.NZ': Sector.UTILITIES,
            'AIA.NZ': Sector.INDUSTRIAL,
            'IFT.NZ': Sector.INDUSTRIAL,
            'MFT.NZ': Sector.INDUSTRIAL,
            'ATM.NZ': Sector.CONSUMER,
            'POT.NZ': Sector.INDUSTRIAL,
            'SPK.NZ': Sector.COMMUNICATION,
            'VCT.NZ': Sector.UTILITIES,
            'CNU.NZ': Sector.COMMUNICATION,
            'BRK-B': Sector.FINANCIAL,
            'MSFT': Sector.TECHNOLOGY,
            'AAPL': Sector.TECHNOLOGY,
            'GOOGL': Sector.TECHNOLOGY,
            'JNJ': Sector.HEALTHCARE,
            'PG': Sector.CONSUMER,
            'KO': Sector.CONSUMER,
            'PEP': Sector.CONSUMER,
            'WMT': Sector.CONSUMER,
            'HD': Sector.CONSUMER,
        }
        
        return sector_mapping.get(ticker, Sector.UNKNOWN)
    
    def get_stock_data(self, ticker: str) -> Dict:
        """Get comprehensive stock data"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Extract key metrics with proper null handling
            def safe_get(key, default=0):
                value = info.get(key, default)
                if value is None or np.isnan(value) if isinstance(value, (int, float)) else False:
                    return default
                return value
            
            data = {
                'ticker': ticker,
                'current_price': safe_get('currentPrice', 0),
                'eps_ttm': safe_get('trailingEps', 0),
                'eps_growth_5y': safe_get('earningsGrowth', 0),
                'pe_ratio': safe_get('trailingPE', 0),
                'fcf_ttm': safe_get('freeCashflow', 0),
                'net_income': safe_get('netIncomeToCommon', 0),
                'revenue_growth_5y': safe_get('revenueGrowth', 0),
                'debt_to_equity': safe_get('debtToEquity', 0),
                'current_ratio': safe_get('currentRatio', 0),
                'roe_ttm': safe_get('returnOnEquity', 0),
                'roic_ttm': safe_get('returnOnInvestedCapital', 0),
                'beta': safe_get('beta', 1.0),
                'gross_margin_ttm': safe_get('grossMargins', 0),
                'earnings_date': info.get('mostRecentQuarter', ''),
            }
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error getting data for {ticker}: {e}")
            return {}
    
    def calculate_valuations(self, data: Dict) -> List[ValuationResult]:
        """Calculate all applicable valuations"""
        ticker = data['ticker']
        sector = self.get_sector(ticker, data)
        
        valuations = []
        
        # DCF Valuation
        dcf = ImprovedDCFValuation(sector)
        dcf_result = dcf.calculate_dcf_value(data)
        valuations.append(dcf_result)
        
        # Peter Lynch Valuation
        lynch = ImprovedLynchValuation(sector)
        lynch_result = lynch.calculate_lynch_value(data)
        valuations.append(lynch_result)
        
        return valuations
    
    def analyze_stock(self, ticker: str) -> Dict:
        """Comprehensive stock analysis"""
        self.logger.info(f"Analyzing {ticker}")
        
        # Get data
        data = self.get_stock_data(ticker)
        if not data:
            return {'error': 'Failed to get data'}
        
        # Calculate valuations
        valuations = self.calculate_valuations(data)
        
        # Assess quality
        quality = QualityAssessment.calculate_overall_quality(data)
        
        # Calculate consensus
        valid_valuations = [v for v in valuations if v.intrinsic_value > 0]
        if valid_valuations:
            avg_intrinsic = np.mean([v.intrinsic_value for v in valid_valuations])
            avg_margin_of_safety = np.mean([v.margin_of_safety for v in valid_valuations])
            avg_confidence = np.mean([v.confidence for v in valid_valuations])
        else:
            avg_intrinsic = 0
            avg_margin_of_safety = 0
            avg_confidence = 0
        
        return {
            'ticker': ticker,
            'company_name': self.focus_stocks.get(ticker, ticker),
            'current_price': data.get('current_price', 0),
            'sector': self.get_sector(ticker, data).value,
            'valuations': valuations,
            'quality_metrics': quality,
            'consensus': {
                'intrinsic_value': avg_intrinsic,
                'margin_of_safety': avg_margin_of_safety,
                'confidence': avg_confidence
            },
            'raw_data': data
        }
    
    def run_analysis(self):
        """Run comprehensive analysis on all focus stocks"""
        self.logger.info("Starting improved stock valuation analysis")
        
        results = []
        for ticker in tqdm(self.focus_stocks.keys(), desc="Analyzing stocks"):
            try:
                result = self.analyze_stock(ticker)
                if 'error' not in result:
                    results.append(result)
                time.sleep(1)  # Rate limiting
            except Exception as e:
                self.logger.error(f"Error analyzing {ticker}: {e}")
        
        # Save results
        self.save_results(results)
        
        # Print summary
        self.print_summary(results)
    
    def save_results(self, results: List[Dict]):
        """Save results to Excel file"""
        # Create DataFrame
        summary_data = []
        for result in results:
            summary_data.append({
                'Ticker': result['ticker'],
                'Company': result['company_name'],
                'Current Price': result['current_price'],
                'Sector': result['sector'],
                'Intrinsic Value': result['consensus']['intrinsic_value'],
                'Margin of Safety': result['consensus']['margin_of_safety'],
                'Confidence': result['consensus']['confidence'],
                'Quality Score': result['quality_metrics'].overall_quality_score,
                'Earnings Quality': result['quality_metrics'].earnings_quality,
                'Balance Sheet': result['quality_metrics'].balance_sheet_strength,
                'Competitive Position': result['quality_metrics'].competitive_position,
            })
        
        df = pd.DataFrame(summary_data)
        
        # Save to Excel
        output_file = os.path.join(self.output_dir, f"valuation_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Format the sheet
            workbook = writer.book
            worksheet = writer.sheets['Summary']
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        self.logger.info(f"Results saved to {output_file}")
    
    def print_summary(self, results: List[Dict]):
        """Print analysis summary"""
        print("\n" + "="*80)
        print("STOCK VALUATION ANALYSIS SUMMARY")
        print("="*80)
        
        # Sort by margin of safety
        results.sort(key=lambda x: x['consensus']['margin_of_safety'], reverse=True)
        
        print(f"\n{'Ticker':<8} {'Company':<25} {'Price':<8} {'Intrinsic':<10} {'MoS%':<8} {'Quality':<8}")
        print("-" * 80)
        
        for result in results:
            print(f"{result['ticker']:<8} {result['company_name'][:24]:<25} "
                  f"${result['current_price']:<7.2f} ${result['consensus']['intrinsic_value']:<9.2f} "
                  f"{result['consensus']['margin_of_safety']:<7.1f}% {result['quality_metrics'].overall_quality_score:<7.2f}")
        
        # Top opportunities
        opportunities = [r for r in results if r['consensus']['margin_of_safety'] > 20 and r['quality_metrics'].overall_quality_score > 0.6]
        
        if opportunities:
            print(f"\nTOP OPPORTUNITIES (MoS > 20%, Quality > 0.6):")
            print("-" * 50)
            for opp in opportunities[:5]:
                print(f"• {opp['ticker']} ({opp['company_name']}) - {opp['consensus']['margin_of_safety']:.1f}% MoS, "
                      f"Quality: {opp['quality_metrics'].overall_quality_score:.2f}")

def main():
    """Main function"""
    scraper = ImprovedStockValuationScraper()
    scraper.run_analysis()

if __name__ == "__main__":
    main()
