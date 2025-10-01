import pandas as pd
import numpy as np
import yfinance as yf
import time
import os
from datetime import datetime
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
class ValuationSummary:
    """Simplified valuation summary focusing on key metrics"""
    ticker: str
    company_name: str
    current_price: float
    sector: str
    
    # Key valuation metrics
    pe_ratio: float
    pb_ratio: float
    peg_ratio: float
    dividend_yield: float
    
    # Quality metrics
    roe: float
    debt_to_equity: float
    current_ratio: float
    fcf_yield: float
    
    # Valuation assessment
    is_cheap: bool
    is_quality: bool
    margin_of_safety: float
    confidence: float
    
    # Warnings
    warnings: List[str]

class RobustStockAnalyzer:
    """Robust stock analyzer focusing on quality and value screening"""
    
    def __init__(self):
        # Expanded universe for value screening
        self.focus_stocks = {
            # NZX Stocks - Complete coverage
            'WBC.NZ': 'Westpac Banking Corporation',
            'ANZ.NZ': 'ANZ Group Holdings Limited',
            'FPH.NZ': 'Fisher & Paykel Healthcare Corporation Limited',
            'MEL.NZ': 'Meridian Energy Limited',
            'AIA.NZ': 'Auckland International Airport Limited',
            'IFT.NZ': 'Infratil Limited',
            'AFI.NZ': 'Australian Foundation Investment Company Limited',
            'MCY.NZ': 'Mercury NZ Limited',
            'EBO.NZ': 'EBOS Group Limited',
            'FCG.NZ': 'Fonterra Co-operative Group Limited',
            'CEN.NZ': 'Contact Energy Limited',
            'MFT.NZ': 'Mainfreight Limited',
            'ATM.NZ': 'The a2 Milk Company Limited',
            'POT.NZ': 'Port of Tauranga Limited',
            'SPK.NZ': 'Spark New Zealand Limited',
            'VNT.NZ': 'Ventia Services Group Limited',
            'VCT.NZ': 'Vector Limited',
            'CNU.NZ': 'Chorus Limited',
            'FBU.NZ': 'Fletcher Building Limited',
            'GMT.NZ': 'Goodman Property Trust',
            'SUM.NZ': 'Summerset Group Holdings Limited',
            'GNE.NZ': 'Genesis Energy Limited',
            'RYM.NZ': 'Ryman Healthcare Limited',
            'FRW.NZ': 'Freightways Group Limited',
            'MNW.NZ': 'Manawa Energy Limited',
            'PCT.NZ': 'Precinct Properties NZ Ltd',
            'AIR.NZ': 'Air New Zealand Limited',
            'KPG.NZ': 'Kiwi Property Group Limited',
            'GTK.NZ': 'Gentrack Group Limited',
            
            # International Value Candidates
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
            'JPM': 'JPMorgan Chase & Co.',
            'BAC': 'Bank of America Corporation',
            'WFC': 'Wells Fargo & Company',
            'CVX': 'Chevron Corporation',
            'XOM': 'Exxon Mobil Corporation',
            'IBM': 'International Business Machines Corporation',
            'INTC': 'Intel Corporation',
            'CSCO': 'Cisco Systems Inc.',
            'ORCL': 'Oracle Corporation',
            'ADBE': 'Adobe Inc.',
        }
        
        # Primary dataset path as specified
        self.primary_dataset_path = r"C:\Users\james\Downloads\Stock Valuation\stock_valuation_dataset.xlsx"
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.primary_dataset_path), exist_ok=True)
        
        # Secondary output directory
        self.output_dir = os.path.join(os.getcwd(), "valuation_results")
        os.makedirs(self.output_dir, exist_ok=True)
    
    def get_sector(self, ticker: str) -> Sector:
        """Simple sector mapping"""
        sector_map = {
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
            'WBC.NZ': Sector.FINANCIAL,
            'ANZ.NZ': Sector.FINANCIAL,
            'AFI.NZ': Sector.FINANCIAL,
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
            'JPM': Sector.FINANCIAL,
            'BAC': Sector.FINANCIAL,
            'WFC': Sector.FINANCIAL,
            'CVX': Sector.ENERGY,
            'XOM': Sector.ENERGY,
            'IBM': Sector.TECHNOLOGY,
            'INTC': Sector.TECHNOLOGY,
            'CSCO': Sector.TECHNOLOGY,
            'ORCL': Sector.TECHNOLOGY,
            'ADBE': Sector.TECHNOLOGY,
        }
        return sector_map.get(ticker, Sector.UNKNOWN)
    
    def safe_get(self, data: dict, key: str, default: float = 0.0) -> float:
        """Safely extract numeric values from data"""
        value = data.get(key, default)
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return default
        return float(value)
    
    def get_stock_data(self, ticker: str) -> Dict:
        """Get comprehensive stock data with historical context"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Get historical data for additional metrics
            hist_data = None
            try:
                # Get 5 years of monthly data for trend analysis
                hist_data = stock.history(period="5y", interval="1mo")
            except Exception as e:
                logger.warning(f"Could not get historical data for {ticker}: {e}")
            
            # Extract comprehensive metrics
            data = {
                'ticker': ticker,
                'company_name': self.focus_stocks.get(ticker, ticker),
                'current_price': self.safe_get(info, 'currentPrice'),
                'market_cap': self.safe_get(info, 'marketCap'),
                'pe_ratio': self.safe_get(info, 'trailingPE'),
                'pb_ratio': self.safe_get(info, 'priceToBook'),
                'ps_ratio': self.safe_get(info, 'priceToSalesTrailing12Months'),
                'peg_ratio': self.safe_get(info, 'pegRatio'),
                'dividend_yield': self.safe_get(info, 'dividendYield', 0) * 100,  # Convert to percentage
                'eps_ttm': self.safe_get(info, 'trailingEps'),
                'eps_growth_5y': self.safe_get(info, 'earningsGrowth', 0) * 100,  # Convert to percentage
                'revenue_growth_5y': self.safe_get(info, 'revenueGrowth', 0) * 100,  # Convert to percentage
                'roe': self.safe_get(info, 'returnOnEquity', 0) * 100,  # Convert to percentage
                'roa': self.safe_get(info, 'returnOnAssets', 0) * 100,  # Convert to percentage
                'roic': self.safe_get(info, 'returnOnInvestedCapital', 0) * 100,  # Convert to percentage
                'debt_to_equity': self.safe_get(info, 'debtToEquity'),
                'current_ratio': self.safe_get(info, 'currentRatio'),
                'quick_ratio': self.safe_get(info, 'quickRatio'),
                'fcf_ttm': self.safe_get(info, 'freeCashflow'),
                'net_income': self.safe_get(info, 'netIncomeToCommon'),
                'revenue_ttm': self.safe_get(info, 'totalRevenue'),
                'gross_margin': self.safe_get(info, 'grossMargins', 0) * 100,  # Convert to percentage
                'operating_margin': self.safe_get(info, 'operatingMargins', 0) * 100,  # Convert to percentage
                'net_margin': self.safe_get(info, 'profitMargins', 0) * 100,  # Convert to percentage
                'beta': self.safe_get(info, 'beta', 1.0),
                'shares_outstanding': self.safe_get(info, 'sharesOutstanding'),
                'book_value_per_share': self.safe_get(info, 'bookValue'),
                'cash_per_share': self.safe_get(info, 'totalCashPerShare'),
                'debt_per_share': self.safe_get(info, 'totalDebtPerShare'),
                'sector': self.get_sector(ticker).value,
                'industry': info.get('industry', 'Unknown'),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            
            # Add historical metrics if available
            if hist_data is not None and not hist_data.empty:
                # Calculate historical volatility
                if len(hist_data) > 12:  # At least 1 year of data
                    returns = hist_data['Close'].pct_change().dropna()
                    data['volatility_1y'] = returns.std() * np.sqrt(12) * 100  # Annualized volatility
                    data['max_drawdown_5y'] = self.calculate_max_drawdown(hist_data['Close'])
                
                # Price momentum
                if len(hist_data) >= 12:
                    data['price_change_1y'] = ((hist_data['Close'].iloc[-1] / hist_data['Close'].iloc[-12]) - 1) * 100
                if len(hist_data) >= 3:
                    data['price_change_3m'] = ((hist_data['Close'].iloc[-1] / hist_data['Close'].iloc[-3]) - 1) * 100
            
            return data
            
        except Exception as e:
            logger.error(f"Error getting data for {ticker}: {e}")
            return {}
    
    def calculate_max_drawdown(self, prices: pd.Series) -> float:
        """Calculate maximum drawdown from peak"""
        try:
            peak = prices.expanding().max()
            drawdown = (prices - peak) / peak
            return drawdown.min() * 100
        except:
            return 0.0
    
    def calculate_fcf_yield(self, data: Dict) -> float:
        """Calculate FCF yield"""
        fcf = data.get('fcf_ttm', 0)
        market_cap = data.get('market_cap', 0)
        
        if market_cap > 0 and fcf > 0:
            return (fcf / market_cap) * 100
        return 0.0
    
    def assess_quality(self, data: Dict) -> Tuple[bool, float, List[str]]:
        """Assess business quality with strict criteria"""
        warnings = []
        quality_score = 0.0
        
        # ROE assessment - STRICT: Must be > 10%
        roe = data.get('roe', 0)
        if roe > 15:
            quality_score += 0.3
        elif roe > 10:
            quality_score += 0.2
        elif roe < 5:
            warnings.append(f"Low ROE: {roe:.1f}%")
        
        # Debt assessment
        debt_to_equity = data.get('debt_to_equity', 0)
        if debt_to_equity < 0.3:
            quality_score += 0.2
        elif debt_to_equity < 0.5:
            quality_score += 0.1
        elif debt_to_equity > 1.0:
            warnings.append(f"High debt-to-equity: {debt_to_equity:.2f}")
        
        # Current ratio assessment
        current_ratio = data.get('current_ratio', 0)
        if current_ratio > 1.5:
            quality_score += 0.1
        elif current_ratio < 1.0:
            warnings.append(f"Low current ratio: {current_ratio:.2f}")
        
        # FCF yield assessment
        fcf_yield = self.calculate_fcf_yield(data)
        if fcf_yield > 5:
            quality_score += 0.2
        elif fcf_yield > 3:
            quality_score += 0.1
        elif fcf_yield < 1:
            warnings.append(f"Low FCF yield: {fcf_yield:.1f}%")
        
        # Growth consistency
        eps_growth = data.get('eps_growth_5y', 0)
        revenue_growth = data.get('revenue_growth_5y', 0)
        
        if eps_growth > 0 and revenue_growth > 0:
            if abs(eps_growth - revenue_growth) < 5:  # Growth alignment
                quality_score += 0.1
        
        # Margin stability
        gross_margin = data.get('gross_margin', 0)
        if gross_margin > 30:
            quality_score += 0.1
        
        is_quality = quality_score >= 0.6
        return is_quality, quality_score, warnings
    
    def assess_value(self, data: Dict, sector: Sector) -> Tuple[bool, float, List[str]]:
        """Assess valuation attractiveness with strict value criteria"""
        warnings = []
        value_score = 0.0
        
        # STRICT VALUE CRITERIA: P/E < 15
        pe_ratio = data.get('pe_ratio', 0)
        if pe_ratio > 0:
            if pe_ratio < 15:
                value_score += 0.4  # Higher weight for P/E < 15
            elif pe_ratio < 20:
                value_score += 0.2
            elif pe_ratio > 30:
                warnings.append(f"High P/E ratio: {pe_ratio:.1f}")
        
        # PEG ratio assessment (must be < 1.5 for value)
        peg_ratio = data.get('peg_ratio', 0)
        if peg_ratio > 0:
            if peg_ratio < 1.0:
                value_score += 0.3
            elif peg_ratio < 1.5:
                value_score += 0.1
            elif peg_ratio > 2.0:
                warnings.append(f"High PEG ratio: {peg_ratio:.2f}")
        
        # P/B ratio assessment (sector-specific, stricter for value)
        pb_ratio = data.get('pb_ratio', 0)
        if pb_ratio > 0:
            if sector == Sector.FINANCIAL:
                if pb_ratio < 1.2:  # Stricter for financials
                    value_score += 0.3
                elif pb_ratio < 1.5:
                    value_score += 0.1
                elif pb_ratio > 2.0:
                    warnings.append(f"High P/B for financial: {pb_ratio:.2f}")
            else:
                if pb_ratio < 1.5:  # Stricter for non-financials
                    value_score += 0.3
                elif pb_ratio < 2.0:
                    value_score += 0.1
                elif pb_ratio > 3.0:
                    warnings.append(f"High P/B ratio: {pb_ratio:.2f}")
        
        # Dividend yield assessment (bonus points)
        dividend_yield = data.get('dividend_yield', 0)
        if dividend_yield > 4:
            value_score += 0.2
        elif dividend_yield > 2:
            value_score += 0.1
        
        # FCF yield assessment (must be > 3% for value)
        fcf_yield = self.calculate_fcf_yield(data)
        if fcf_yield > 5:
            value_score += 0.3
        elif fcf_yield > 3:
            value_score += 0.2
        elif fcf_yield < 1:
            warnings.append(f"Low FCF yield: {fcf_yield:.1f}%")
        
        # STRICT VALUE CRITERIA: Must meet multiple criteria
        is_cheap = value_score >= 0.6  # Stricter threshold
        return is_cheap, value_score, warnings
    
    def calculate_margin_of_safety(self, data: Dict) -> float:
        """Calculate margin of safety using multiple methods"""
        current_price = data.get('current_price', 0)
        if current_price <= 0:
            return 0.0
        
        # Method 1: Graham's formula (simplified)
        eps = data.get('eps_ttm', 0)
        growth_rate = data.get('eps_growth_5y', 0) / 100  # Convert to decimal
        
        if eps > 0 and growth_rate > 0:
            # Graham's formula: Intrinsic Value = EPS × (8.5 + 2g) × 4.4 / Y
            # Simplified version: EPS × (8.5 + 2g)
            graham_value = eps * (8.5 + 2 * growth_rate)
            graham_mos = ((graham_value - current_price) / current_price) * 100
        else:
            graham_mos = 0
        
        # Method 2: P/E reversion to historical average
        pe_ratio = data.get('pe_ratio', 0)
        if pe_ratio > 0 and eps > 0:
            # Assume fair P/E of 15 for most stocks
            fair_pe = 15
            if pe_ratio > fair_pe:
                pe_mos = ((fair_pe - pe_ratio) / pe_ratio) * 100
            else:
                pe_mos = ((pe_ratio - fair_pe) / fair_pe) * 100
        else:
            pe_mos = 0
        
        # Average the methods
        if graham_mos != 0 and pe_mos != 0:
            return (graham_mos + pe_mos) / 2
        elif graham_mos != 0:
            return graham_mos
        elif pe_mos != 0:
            return pe_mos
        else:
            return 0.0
    
    def analyze_stock(self, ticker: str) -> Optional[ValuationSummary]:
        """Comprehensive stock analysis"""
        logger.info(f"Analyzing {ticker}")
        
        # Get data
        data = self.get_stock_data(ticker)
        if not data or data.get('current_price', 0) <= 0:
            logger.warning(f"No valid data for {ticker}")
            return None
        
        # Determine sector
        sector = self.get_sector(ticker)
        
        # Assess quality and value
        is_quality, quality_score, quality_warnings = self.assess_quality(data)
        is_cheap, value_score, value_warnings = self.assess_value(data, sector)
        
        # Calculate margin of safety
        margin_of_safety = self.calculate_margin_of_safety(data)
        
        # Calculate confidence
        confidence = (quality_score + value_score) / 2
        
        # Combine warnings
        all_warnings = quality_warnings + value_warnings
        
        # Add data quality warnings
        if data.get('eps_ttm', 0) <= 0:
            all_warnings.append("Negative or zero EPS")
        if data.get('fcf_ttm', 0) <= 0:
            all_warnings.append("Negative FCF")
        if data.get('revenue_ttm', 0) <= 0:
            all_warnings.append("Negative revenue")
        
        return ValuationSummary(
            ticker=ticker,
            company_name=self.focus_stocks.get(ticker, ticker),
            current_price=data.get('current_price', 0),
            sector=sector.value,
            pe_ratio=data.get('pe_ratio', 0),
            pb_ratio=data.get('pb_ratio', 0),
            peg_ratio=data.get('peg_ratio', 0),
            dividend_yield=data.get('dividend_yield', 0),
            roe=data.get('roe', 0),
            debt_to_equity=data.get('debt_to_equity', 0),
            current_ratio=data.get('current_ratio', 0),
            fcf_yield=self.calculate_fcf_yield(data),
            is_cheap=is_cheap,
            is_quality=is_quality,
            margin_of_safety=margin_of_safety,
            confidence=confidence,
            warnings=all_warnings
        )
    
    def save_to_primary_dataset(self, results: List[ValuationSummary]):
        """Save comprehensive data to primary dataset"""
        if not results:
            logger.warning("No results to save to primary dataset")
            return
        
        # Convert to comprehensive DataFrame
        data = []
        for result in results:
            # Get the raw data for this ticker
            raw_data = self.get_stock_data(result.ticker)
            
            data.append({
                'Ticker': result.ticker,
                'Company Name': result.company_name,
                'Current Price': result.current_price,
                'Sector': result.sector,
                'Industry': raw_data.get('industry', 'Unknown'),
                'Market Cap': raw_data.get('market_cap', 0),
                'P/E Ratio': result.pe_ratio,
                'P/B Ratio': result.pb_ratio,
                'PEG Ratio': result.peg_ratio,
                'P/S Ratio': raw_data.get('ps_ratio', 0),
                'Dividend Yield %': result.dividend_yield,
                'EPS TTM': raw_data.get('eps_ttm', 0),
                'EPS Growth 5Y %': raw_data.get('eps_growth_5y', 0),
                'Revenue Growth 5Y %': raw_data.get('revenue_growth_5y', 0),
                'ROE %': result.roe,
                'ROA %': raw_data.get('roa', 0),
                'ROIC %': raw_data.get('roic', 0),
                'Debt/Equity': result.debt_to_equity,
                'Current Ratio': result.current_ratio,
                'Quick Ratio': raw_data.get('quick_ratio', 0),
                'FCF TTM': raw_data.get('fcf_ttm', 0),
                'FCF Yield %': result.fcf_yield,
                'Net Income': raw_data.get('net_income', 0),
                'Revenue TTM': raw_data.get('revenue_ttm', 0),
                'Gross Margin %': raw_data.get('gross_margin', 0),
                'Operating Margin %': raw_data.get('operating_margin', 0),
                'Net Margin %': raw_data.get('net_margin', 0),
                'Beta': raw_data.get('beta', 1.0),
                'Shares Outstanding': raw_data.get('shares_outstanding', 0),
                'Book Value Per Share': raw_data.get('book_value_per_share', 0),
                'Cash Per Share': raw_data.get('cash_per_share', 0),
                'Debt Per Share': raw_data.get('debt_per_share', 0),
                'Volatility 1Y %': raw_data.get('volatility_1y', 0),
                'Max Drawdown 5Y %': raw_data.get('max_drawdown_5y', 0),
                'Price Change 1Y %': raw_data.get('price_change_1y', 0),
                'Price Change 3M %': raw_data.get('price_change_3m', 0),
                'Is Cheap': result.is_cheap,
                'Is Quality': result.is_quality,
                'Margin of Safety %': result.margin_of_safety,
                'Confidence': result.confidence,
                'Warnings': '; '.join(result.warnings) if result.warnings else '',
                'Timestamp': raw_data.get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            })
        
        df = pd.DataFrame(data)
        
        # Save to primary dataset
        try:
            df.to_excel(self.primary_dataset_path, index=False)
            logger.info(f"Primary dataset saved to {self.primary_dataset_path}")
        except Exception as e:
            logger.error(f"Error saving primary dataset: {e}")
    
    def run_analysis(self):
        """Run comprehensive analysis with value screening"""
        logger.info("Starting comprehensive stock valuation analysis with value screening")
        
        results = []
        value_candidates = []
        
        for ticker in self.focus_stocks.keys():
            try:
                result = self.analyze_stock(ticker)
                if result:
                    results.append(result)
                    
                    # Check if meets strict value criteria
                    if (result.pe_ratio < 15 and result.pe_ratio > 0 and 
                        result.roe > 10 and result.is_quality):
                        value_candidates.append(result)
                        logger.info(f"VALUE CANDIDATE: {ticker} - P/E: {result.pe_ratio:.1f}, ROE: {result.roe:.1f}%")
                
                time.sleep(1)  # Rate limiting
            except Exception as e:
                logger.error(f"Error analyzing {ticker}: {e}")
        
        # Save comprehensive data to primary dataset
        self.save_to_primary_dataset(results)
        
        # Save filtered results
        self.save_results(results)
        
        # Display results
        self.print_summary(results)
        
        # Print value candidates
        if value_candidates:
            print(f"\n🎯 VALUE CANDIDATES FOUND (P/E < 15, ROE > 10%, Quality):")
            print("=" * 80)
            for candidate in sorted(value_candidates, key=lambda x: x.pe_ratio):
                print(f"• {candidate.ticker} ({candidate.company_name})")
                print(f"  P/E: {candidate.pe_ratio:.1f} | ROE: {candidate.roe:.1f}% | "
                      f"P/B: {candidate.pb_ratio:.1f} | FCF Yield: {candidate.fcf_yield:.1f}%")
                print(f"  Margin of Safety: {candidate.margin_of_safety:.1f}% | "
                      f"Confidence: {candidate.confidence:.2f}")
                if candidate.warnings:
                    print(f"  Warnings: {'; '.join(candidate.warnings)}")
                print()
        else:
            print(f"\n❌ NO VALUE CANDIDATES FOUND")
            print("Current market conditions may not offer traditional value opportunities.")
            print("Consider expanding criteria or waiting for market correction.")
    
    def save_results(self, results: List[ValuationSummary]):
        """Save results to Excel"""
        if not results:
            logger.warning("No results to save")
            return
        
        # Convert to DataFrame
        data = []
        for result in results:
            data.append({
                'Ticker': result.ticker,
                'Company': result.company_name,
                'Current Price': result.current_price,
                'Sector': result.sector,
                'P/E Ratio': result.pe_ratio,
                'P/B Ratio': result.pb_ratio,
                'PEG Ratio': result.peg_ratio,
                'Dividend Yield %': result.dividend_yield,
                'ROE %': result.roe,
                'Debt/Equity': result.debt_to_equity,
                'Current Ratio': result.current_ratio,
                'FCF Yield %': result.fcf_yield,
                'Is Cheap': result.is_cheap,
                'Is Quality': result.is_quality,
                'Margin of Safety %': result.margin_of_safety,
                'Confidence': result.confidence,
                'Warnings': '; '.join(result.warnings) if result.warnings else ''
            })
        
        df = pd.DataFrame(data)
        
        # Save to Excel
        output_file = os.path.join(self.output_dir, f"valuation_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        df.to_excel(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
    
    def print_summary(self, results: List[ValuationSummary]):
        """Print analysis summary"""
        print("\n" + "="*100)
        print("COMPREHENSIVE STOCK VALUATION ANALYSIS WITH VALUE SCREENING")
        print("="*100)
        
        # Sort by margin of safety
        results.sort(key=lambda x: x.margin_of_safety, reverse=True)
        
        print(f"\n{'Ticker':<8} {'Company':<20} {'Price':<8} {'P/E':<6} {'P/B':<6} {'ROE%':<6} {'MoS%':<8} {'Quality':<8} {'Cheap':<6}")
        print("-" * 100)
        
        for result in results:
            print(f"{result.ticker:<8} {result.company_name[:19]:<20} "
                  f"${result.current_price:<7.2f} {result.pe_ratio:<5.1f} {result.pb_ratio:<5.1f} "
                  f"{result.roe:<5.1f} {result.margin_of_safety:<7.1f}% "
                  f"{'Yes' if result.is_quality else 'No':<8} {'Yes' if result.is_cheap else 'No':<6}")
        
        # Top opportunities
        opportunities = [r for r in results if r.is_cheap and r.is_quality and r.margin_of_safety > 10]
        
        if opportunities:
            print(f"\nTOP OPPORTUNITIES (Cheap + Quality + MoS > 10%):")
            print("-" * 60)
            for opp in opportunities[:5]:
                print(f"• {opp.ticker} ({opp.company_name}) - {opp.margin_of_safety:.1f}% MoS, "
                      f"P/E: {opp.pe_ratio:.1f}, ROE: {opp.roe:.1f}%")
        
        # Quality stocks trading at fair value
        quality_fair = [r for r in results if r.is_quality and not r.is_cheap and r.margin_of_safety > -20]
        
        if quality_fair:
            print(f"\nQUALITY STOCKS AT FAIR VALUE:")
            print("-" * 40)
            for stock in quality_fair[:3]:
                print(f"• {stock.ticker} ({stock.company_name}) - {stock.margin_of_safety:.1f}% MoS, "
                      f"ROE: {stock.roe:.1f}%")

def main():
    """Main function"""
    analyzer = RobustStockAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()