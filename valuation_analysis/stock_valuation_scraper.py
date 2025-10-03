#!/usr/bin/env python3
"""
Deep Stock Investigation Tool - Stage 2 Analysis
===============================================

Comprehensive deep-dive analysis for individual stocks using unified_stock_data.db
Performs 8-section investment thesis analysis with zero external data collection.

Author: AI Assistant
Date: 2025-01-02
"""

import pandas as pd
import numpy as np
import sqlite3
import os
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Any
import logging
from dataclasses import dataclass

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class InvestmentThesis:
    """Container for complete investment thesis"""
    ticker: str
    generated_date: datetime
    section_1: Dict[str, Any]
    section_2: Dict[str, Any]
    section_3: Dict[str, Any]
    section_4: Dict[str, Any]
    section_5: Dict[str, Any]
    section_6: Dict[str, Any]
    section_7: Dict[str, Any]
    section_8: Dict[str, Any]

class DeepStockInvestigation:
    """
    Deep stock investigation tool for Stage 2 analysis.
    Works entirely with unified_stock_data.db - no external data collection.
    """
    
    def __init__(self, db_path: str, ticker: str):
        """
        Initialize deep investigation for a single stock.
        
        Args:
            db_path: Path to unified_stock_data.db
            ticker: Stock ticker to analyze (e.g., 'AIR.NZ', 'AAPL')
        """
        self.db_path = db_path
        self.ticker = ticker.upper()
        
        # Validate database exists
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database not found: {db_path}")
        
        # Load all data for this ticker
        self._load_stock_data()
        
        # Validate ticker exists in database
        if self.fundamentals is None:
            raise ValueError(f"Ticker {ticker} not found in database")
        
        logger.info(f"Initialized deep investigation for {self.ticker}")
    
    def _load_stock_data(self):
        """Load all available data for the ticker from database"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Load current fundamentals
            count_query = pd.read_sql_query(
                "SELECT COUNT(*) as count FROM current_fundamentals WHERE ticker = ?", 
                conn, params=(self.ticker,)
            )
            
            if count_query.iloc[0]['count'] > 0:
                self.fundamentals = pd.read_sql_query(
                    "SELECT * FROM current_fundamentals WHERE ticker = ?", 
                    conn, params=(self.ticker,)
                ).iloc[0]
            else:
                self.fundamentals = None
            
            # Load historical prices
            self.historical_prices = pd.read_sql_query(
                "SELECT * FROM historical_prices WHERE ticker = ? ORDER BY date", 
                conn, params=(self.ticker,)
            )
            
            # Load analyst ratings
            self.analyst_ratings = pd.read_sql_query(
                "SELECT * FROM analyst_ratings WHERE ticker = ? ORDER BY rating_date DESC", 
                conn, params=(self.ticker,)
            )
            
            # Load earnings history
            self.earnings_history = pd.read_sql_query(
                "SELECT * FROM earnings_history WHERE ticker = ? ORDER BY quarter_date DESC", 
                conn, params=(self.ticker,)
            )
            
            # Load corporate actions
            self.corporate_actions = pd.read_sql_query(
                "SELECT * FROM corporate_actions WHERE ticker = ? ORDER BY action_date DESC", 
                conn, params=(self.ticker,)
            )
            
            # Load institutional holdings
            self.institutional_holdings = pd.read_sql_query(
                "SELECT * FROM institutional_holdings WHERE ticker = ? ORDER BY snapshot_date DESC", 
                conn, params=(self.ticker,)
            )
            
            # Load extended price data
            self.extended_price_data = pd.read_sql_query(
                "SELECT * FROM extended_price_data WHERE ticker = ? ORDER BY snapshot_date DESC", 
                conn, params=(self.ticker,)
            )
            
        finally:
            conn.close()
    
    def _safe_get(self, data: pd.Series, key: str, default: Any = None) -> Any:
        """Safely extract value from pandas Series"""
        try:
            value = data.get(key, default)
            if pd.isna(value):
                return default
            return value
        except:
            return default
    
    def _get_sector_averages(self, sector: str) -> Dict[str, float]:
        """Calculate sector averages from database"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            query = '''
                SELECT 
                    AVG(pe_ratio) as avg_pe,
                    AVG(pb_ratio) as avg_pb,
                    AVG(ps_ratio) as avg_ps,
                    AVG(peg_ratio) as avg_peg,
                    AVG(operating_margin) as avg_operating_margin,
                    AVG(roe) as avg_roe,
                    AVG(debt_to_equity) as avg_debt_equity,
                    COUNT(*) as count
                FROM current_fundamentals 
                WHERE sector = ? 
                AND pe_ratio > 0 AND pe_ratio < 100
                AND pb_ratio > 0 AND pb_ratio < 50
            '''
            
            result = pd.read_sql_query(query, conn, params=(sector,)).iloc[0]
            
            return {
                'avg_pe': result['avg_pe'] or 15.0,
                'avg_pb': result['avg_pb'] or 2.0,
                'avg_ps': result['avg_ps'] or 3.0,
                'avg_peg': result['avg_peg'] or 1.5,
                'avg_operating_margin': result['avg_operating_margin'] or 10.0,
                'avg_roe': result['avg_roe'] or 12.0,
                'avg_debt_equity': result['avg_debt_equity'] or 0.5,
                'sector_count': result['count'] or 1
            }
        finally:
            conn.close()
    
    def section_1_company_overview(self) -> Dict:
        """Section 1: Company Overview"""
        if self.fundamentals is None:
            return {'error': 'No fundamental data available'}
        
        # Get current price position vs 52-week range
        current_price = self._safe_get(self.fundamentals, 'market_cap', 0)  # Using market cap as proxy
        position_vs_high = 0.0
        
        if not self.extended_price_data.empty:
            latest_extended = self.extended_price_data.iloc[0]
            high_52w = self._safe_get(latest_extended, 'fifty_two_week_high', 0)
            low_52w = self._safe_get(latest_extended, 'fifty_two_week_low', 0)
            
            if high_52w > 0 and low_52w > 0:
                # Estimate current price from recent historical data
                if not self.historical_prices.empty:
                    current_price = self.historical_prices.iloc[-1]['close_price']
                    position_vs_high = ((current_price - low_52w) / (high_52w - low_52w)) * 100
        
        return {
            'ticker': self.ticker,
            'sector': self._safe_get(self.fundamentals, 'sector', 'Unknown'),
            'industry': self._safe_get(self.fundamentals, 'industry', 'Unknown'),
            'exchange': self._safe_get(self.fundamentals, 'exchange', 'Unknown'),
            'market_cap': self._safe_get(self.fundamentals, 'market_cap', 0),
            'current_price': current_price,
            'position_vs_52w_range': position_vs_high,
            'data_completeness': self._assess_data_completeness()
        }
    
    def _assess_data_completeness(self) -> Dict[str, bool]:
        """Assess completeness of available data"""
        return {
            'has_fundamentals': self.fundamentals is not None,
            'has_price_history': len(self.historical_prices) > 0,
            'has_analyst_ratings': len(self.analyst_ratings) > 0,
            'has_earnings_history': len(self.earnings_history) > 0,
            'has_corporate_actions': len(self.corporate_actions) > 0,
            'has_institutional_holdings': len(self.institutional_holdings) > 0,
            'has_extended_price_data': len(self.extended_price_data) > 0
        }
    
    def section_2_financial_health(self) -> Dict:
        """Section 2: Financial Health Assessment (0-100 score)"""
        if self.fundamentals is None:
            return {'score': 0, 'breakdown': {}, 'red_flags': ['No fundamental data']}
        
        score = 0.0
        breakdown = {}
        red_flags = []
        
        # Liquidity assessment (25 points max)
        current_ratio = self._safe_get(self.fundamentals, 'current_ratio', 0)
        if current_ratio > 2.0:
            liquidity_score = 25
        elif current_ratio > 1.5:
            liquidity_score = 15
        elif current_ratio > 1.0:
            liquidity_score = 5
        else:
            liquidity_score = 0
            red_flags.append(f"Low current ratio: {current_ratio:.2f}")
        
        score += liquidity_score
        breakdown['liquidity'] = liquidity_score
        
        # Leverage assessment (25 points max)
        debt_to_equity = self._safe_get(self.fundamentals, 'debt_to_equity', 0)
        if debt_to_equity < 0.5:
            leverage_score = 25
        elif debt_to_equity < 1.0:
            leverage_score = 15
        elif debt_to_equity < 2.0:
            leverage_score = 5
        else:
            leverage_score = -10  # Penalty for high debt
            red_flags.append(f"High debt-to-equity: {debt_to_equity:.2f}")
        
        score += leverage_score
        breakdown['leverage'] = leverage_score
        
        # Profitability assessment (25 points max)
        operating_margin = self._safe_get(self.fundamentals, 'operating_margin', 0)
        if operating_margin > 15:
            profitability_score = 25
        elif operating_margin > 10:
            profitability_score = 15
        elif operating_margin > 5:
            profitability_score = 5
        else:
            profitability_score = 0
            if operating_margin < 0:
                red_flags.append(f"Negative operating margin: {operating_margin:.1f}%")
        
        score += profitability_score
        breakdown['profitability'] = profitability_score
        
        # Cash generation assessment (25 points max)
        fcf_yield = self._safe_get(self.fundamentals, 'fcf_yield', 0)
        if fcf_yield > 8:
            cash_score = 25
        elif fcf_yield > 5:
            cash_score = 15
        elif fcf_yield > 2:
            cash_score = 5
        else:
            cash_score = 0
            if fcf_yield < 0:
                red_flags.append(f"Negative FCF yield: {fcf_yield:.1f}%")
        
        score += cash_score
        breakdown['cash_generation'] = cash_score
        
        return {
            'score': max(0, min(100, score)),  # Clamp between 0-100
            'breakdown': breakdown,
            'red_flags': red_flags
        }
    
    def section_3_valuation_analysis(self) -> Dict:
        """Section 3: Valuation Analysis with Sector Comparison"""
        if self.fundamentals is None:
            return {'error': 'No fundamental data available'}
        
        sector = self._safe_get(self.fundamentals, 'sector', 'Unknown')
        sector_averages = self._get_sector_averages(sector)
        
        # Get current metrics
        pe_ratio = self._safe_get(self.fundamentals, 'pe_ratio', 0)
        pb_ratio = self._safe_get(self.fundamentals, 'pb_ratio', 0)
        ps_ratio = self._safe_get(self.fundamentals, 'ps_ratio', 0)
        eps_ttm = self._safe_get(self.fundamentals, 'eps_ttm', 0)
        fcf_yield = self._safe_get(self.fundamentals, 'fcf_yield', 0)
        
        # Estimate current price from historical data
        current_price = 0
        if not self.historical_prices.empty:
            current_price = self.historical_prices.iloc[-1]['close_price']
        
        # Calculate intrinsic value using multiple methods
        intrinsic_values = {}
        
        # Method 1: PE-based valuation
        if pe_ratio > 0 and eps_ttm > 0:
            fair_pe = sector_averages['avg_pe']
            intrinsic_values['pe_based'] = fair_pe * eps_ttm
        
        # Method 2: PB-based valuation (simplified)
        if pb_ratio > 0 and current_price > 0:
            fair_pb = sector_averages['avg_pb']
            book_value_per_share = current_price / pb_ratio
            intrinsic_values['pb_based'] = fair_pb * book_value_per_share
        
        # Method 3: DCF-simple (using FCF yield)
        if fcf_yield > 0 and current_price > 0:
            # Assume 5% growth rate and 10% discount rate
            growth_rate = 0.05
            discount_rate = 0.10
            fcf_per_share = current_price * (fcf_yield / 100)
            intrinsic_values['dcf_simple'] = fcf_per_share * (1 + growth_rate) / (discount_rate - growth_rate)
        
        # Method 4: Graham's formula
        eps_growth_5y = self._safe_get(self.fundamentals, 'eps_growth_5y', 0) / 100
        if eps_ttm > 0 and eps_growth_5y > 0:
            intrinsic_values['graham'] = eps_ttm * (8.5 + 2 * eps_growth_5y)
        
        # Calculate average intrinsic value
        valid_values = [v for v in intrinsic_values.values() if v > 0]
        if valid_values:
            avg_intrinsic_value = sum(valid_values) / len(valid_values)
            discount_pct = ((avg_intrinsic_value - current_price) / current_price) * 100
            
            if discount_pct > 20:
                verdict = "Undervalued"
            elif discount_pct > -10:
                verdict = "Fair"
            else:
                verdict = "Overvalued"
        else:
            avg_intrinsic_value = current_price
            discount_pct = 0
            verdict = "Cannot determine"
        
        return {
            'intrinsic_value': avg_intrinsic_value,
            'current_price': current_price,
            'discount_pct': discount_pct,
            'verdict': verdict,
            'methods_used': intrinsic_values,
            'sector_comparison': {
                'sector': sector,
                'pe_vs_sector': pe_ratio / sector_averages['avg_pe'] if sector_averages['avg_pe'] > 0 else 1,
                'pb_vs_sector': pb_ratio / sector_averages['avg_pb'] if sector_averages['avg_pb'] > 0 else 1,
                'sector_avg_pe': sector_averages['avg_pe'],
                'sector_avg_pb': sector_averages['avg_pb']
            }
        }
    
    def generate_investment_thesis(self) -> InvestmentThesis:
        """Generate complete investment thesis"""
        logger.info(f"Generating investment thesis for {self.ticker}")
        
        return InvestmentThesis(
            ticker=self.ticker,
            generated_date=datetime.now(),
            section_1=self.section_1_company_overview(),
            section_2=self.section_2_financial_health(),
            section_3=self.section_3_valuation_analysis(),
            section_4=self.section_4_quality_and_moat(),
            section_5=self.section_5_historical_performance(),
            section_6=self.section_6_risk_assessment(),
            section_7=self.section_7_catalysts_and_opportunities(),
            section_8=self.section_8_investment_recommendation()
        )
    
    def print_summary(self):
        """Print formatted investment thesis summary"""
        thesis = self.generate_investment_thesis()
        
        print(f"\n{'='*80}")
        print(f"DEEP STOCK INVESTIGATION: {self.ticker}")
        print(f"{'='*80}")
        print(f"Generated: {thesis.generated_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Section 1: Company Overview
        s1 = thesis.section_1
        print(f"\n1. COMPANY OVERVIEW")
        print(f"   Sector: {s1.get('sector', 'Unknown')}")
        print(f"   Industry: {s1.get('industry', 'Unknown')}")
        print(f"   Exchange: {s1.get('exchange', 'Unknown')}")
        print(f"   Market Cap: ${s1.get('market_cap', 0):,.0f}")
        
        # Section 2: Financial Health
        s2 = thesis.section_2
        print(f"\n2. FINANCIAL HEALTH")
        print(f"   Score: {s2.get('score', 0)}/100")
        if s2.get('red_flags'):
            print(f"   Red Flags: {', '.join(s2['red_flags'])}")
        
        # Section 3: Valuation
        s3 = thesis.section_3
        print(f"\n3. VALUATION ANALYSIS")
        print(f"   Current Price: ${s3.get('current_price', 0):.2f}")
        print(f"   Intrinsic Value: ${s3.get('intrinsic_value', 0):.2f}")
        print(f"   Discount/Premium: {s3.get('discount_pct', 0):+.1f}%")
        print(f"   Verdict: {s3.get('verdict', 'Unknown')}")
        
        # Section 4: Quality and Moat
        s4 = thesis.section_4
        print(f"\n4. QUALITY AND MOAT")
        print(f"   Score: {s4.get('score', 0)}/100")
        print(f"   Moat Type: {s4.get('moat_type', 'None')}")
        if s4.get('competitive_advantages'):
            print(f"   Advantages: {', '.join(s4['competitive_advantages'])}")
        
        # Section 5: Historical Performance
        s5 = thesis.section_5
        print(f"\n5. HISTORICAL PERFORMANCE")
        returns = s5.get('returns', {})
        for period, ret in returns.items():
            print(f"   {period.upper()}: {ret:+.1f}%")
        print(f"   Volatility: {s5.get('volatility', 0):.1f}%")
        print(f"   Trend: {s5.get('trend', 'Unknown')}")
        
        # Section 6: Risk Assessment
        s6 = thesis.section_6
        print(f"\n6. RISK ASSESSMENT")
        print(f"   Overall Risk: {s6.get('overall_risk', 'Unknown')}")
        print(f"   Risk Score: {s6.get('risk_score', 0)}/100")
        if s6.get('risk_factors'):
            print(f"   Risk Factors: {', '.join(s6['risk_factors'])}")
        
        # Section 7: Catalysts
        s7 = thesis.section_7
        print(f"\n7. CATALYSTS AND OPPORTUNITIES")
        print(f"   Catalyst Score: {s7.get('catalyst_score', 0)}/100")
        if s7.get('catalysts'):
            print(f"   Catalysts: {', '.join(s7['catalysts'])}")
        
        # Section 8: Investment Recommendation
        s8 = thesis.section_8
        print(f"\n8. INVESTMENT RECOMMENDATION")
        print(f"   Action: {s8.get('action', 'HOLD')}")
        print(f"   Composite Score: {s8.get('composite_score', 0):.0f}/100")
        print(f"   Confidence: {s8.get('confidence', 0):.0%}")
        print(f"   Entry Price: ${s8.get('entry_price', 0):.2f}")
        if s8.get('key_reasons'):
            print(f"   Key Reasons: {', '.join(s8['key_reasons'])}")
        
        print(f"\n{'='*80}")
        print(f"INVESTMENT THESIS SUMMARY:")
        print(f"{s8.get('investment_thesis_summary', 'No summary available')}")
        print(f"{'='*80}")
    
    def export_to_excel(self, filename: str):
        """Export investment thesis to multi-sheet Excel file"""
        thesis = self.generate_investment_thesis()
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Sheet 1: Executive Summary
            summary_data = {
                'Metric': [
                    'Ticker', 'Generated Date', 'Action', 'Composite Score', 'Confidence',
                    'Current Price', 'Intrinsic Value', 'Discount/Premium %',
                    'Financial Health Score', 'Quality Score', 'Risk Score', 'Catalyst Score',
                    'Sector', 'Industry', 'Exchange', 'Market Cap'
                ],
                'Value': [
                    thesis.ticker,
                    thesis.generated_date.strftime('%Y-%m-%d %H:%M:%S'),
                    thesis.section_8.get('action', 'HOLD'),
                    f"{thesis.section_8.get('composite_score', 0):.0f}/100",
                    f"{thesis.section_8.get('confidence', 0):.0%}",
                    f"${thesis.section_3.get('current_price', 0):.2f}",
                    f"${thesis.section_3.get('intrinsic_value', 0):.2f}",
                    f"{thesis.section_3.get('discount_pct', 0):+.1f}%",
                    f"{thesis.section_2.get('score', 0)}/100",
                    f"{thesis.section_4.get('score', 0)}/100",
                    f"{thesis.section_6.get('risk_score', 0)}/100",
                    f"{thesis.section_7.get('catalyst_score', 0)}/100",
                    thesis.section_1.get('sector', 'Unknown'),
                    thesis.section_1.get('industry', 'Unknown'),
                    thesis.section_1.get('exchange', 'Unknown'),
                    f"${thesis.section_1.get('market_cap', 0):,.0f}"
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Executive Summary', index=False)
            
            # Sheet 2: Detailed Analysis
            detailed_data = []
            for section_num, section_name in [
                (1, 'Company Overview'), (2, 'Financial Health'), (3, 'Valuation Analysis'),
                (4, 'Quality and Moat'), (5, 'Historical Performance'), (6, 'Risk Assessment'),
                (7, 'Catalysts'), (8, 'Investment Recommendation')
            ]:
                section_data = getattr(thesis, f'section_{section_num}', {})
                detailed_data.append({
                    'Section': section_name,
                    'Data': str(section_data)
                })
            
            pd.DataFrame(detailed_data).to_excel(writer, sheet_name='Detailed Analysis', index=False)
            
            # Sheet 3: Historical Price Data
            if not self.historical_prices.empty:
                self.historical_prices.to_excel(writer, sheet_name='Price History', index=False)
            
            # Sheet 4: Sector Comparison
            sector = thesis.section_1.get('sector', 'Unknown')
            sector_averages = self._get_sector_averages(sector)
            
            sector_data = {
                'Metric': ['Sector', 'Average PE', 'Average PB', 'Average PS', 'Average PEG',
                          'Average Operating Margin', 'Average ROE', 'Average Debt/Equity', 'Stock Count'],
                'Value': [
                    sector,
                    f"{sector_averages['avg_pe']:.2f}",
                    f"{sector_averages['avg_pb']:.2f}",
                    f"{sector_averages['avg_ps']:.2f}",
                    f"{sector_averages['avg_peg']:.2f}",
                    f"{sector_averages['avg_operating_margin']:.1f}%",
                    f"{sector_averages['avg_roe']:.1f}%",
                    f"{sector_averages['avg_debt_equity']:.2f}",
                    f"{sector_averages['sector_count']}"
                ]
            }
            pd.DataFrame(sector_data).to_excel(writer, sheet_name='Sector Comparison', index=False)
        
        logger.info(f"Investment thesis exported to {filename}")
    
    def export_to_notion_markdown(self) -> str:
        """Generate Notion-compatible markdown for easy copying"""
        thesis = self.generate_investment_thesis()
        
        markdown = f"""# Deep Stock Investigation: {thesis.ticker}

> **Generated:** {thesis.generated_date.strftime('%Y-%m-%d %H:%M:%S')}

## 📊 Executive Summary

| Metric | Value |
|--------|-------|
| **Action** | `{thesis.section_8.get('action', 'HOLD')}` |
| **Composite Score** | {thesis.section_8.get('composite_score', 0):.0f}/100 |
| **Confidence** | {thesis.section_8.get('confidence', 0):.0%} |
| **Current Price** | ${thesis.section_3.get('current_price', 0):.2f} |
| **Intrinsic Value** | ${thesis.section_3.get('intrinsic_value', 0):.2f} |
| **Discount/Premium** | {thesis.section_3.get('discount_pct', 0):+.1f}% |

## 🏢 Company Overview

- **Sector:** {thesis.section_1.get('sector', 'Unknown')}
- **Industry:** {thesis.section_1.get('industry', 'Unknown')}
- **Exchange:** {thesis.section_1.get('exchange', 'Unknown')}
- **Market Cap:** ${thesis.section_1.get('market_cap', 0):,.0f}

## 💰 Financial Health

**Score:** {thesis.section_2.get('score', 0)}/100

"""
        
        if thesis.section_2.get('red_flags'):
            markdown += f"⚠️ **Red Flags:** {', '.join(thesis.section_2['red_flags'])}\n\n"
        
        markdown += f"""## 📈 Valuation Analysis

**Verdict:** {thesis.section_3.get('verdict', 'Unknown')}

- **Current Price:** ${thesis.section_3.get('current_price', 0):.2f}
- **Intrinsic Value:** ${thesis.section_3.get('intrinsic_value', 0):.2f}
- **Discount/Premium:** {thesis.section_3.get('discount_pct', 0):+.1f}%

## 🛡️ Quality and Moat

**Score:** {thesis.section_4.get('score', 0)}/100  
**Moat Type:** {thesis.section_4.get('moat_type', 'None')}

"""
        
        if thesis.section_4.get('competitive_advantages'):
            markdown += f"**Competitive Advantages:**\n"
            for advantage in thesis.section_4['competitive_advantages']:
                markdown += f"- {advantage}\n"
            markdown += "\n"
        
        markdown += f"""## 📊 Historical Performance

"""
        returns = thesis.section_5.get('returns', {})
        for period, ret in returns.items():
            markdown += f"- **{period.upper()}:** {ret:+.1f}%\n"
        
        markdown += f"""
- **Volatility:** {thesis.section_5.get('volatility', 0):.1f}%
- **Trend:** {thesis.section_5.get('trend', 'Unknown')}

## ⚠️ Risk Assessment

**Overall Risk:** {thesis.section_6.get('overall_risk', 'Unknown')}  
**Risk Score:** {thesis.section_6.get('risk_score', 0)}/100

"""
        
        if thesis.section_6.get('risk_factors'):
            markdown += "**Risk Factors:**\n"
            for risk in thesis.section_6['risk_factors']:
                markdown += f"- {risk}\n"
            markdown += "\n"
        
        markdown += f"""## 🚀 Catalysts and Opportunities

**Catalyst Score:** {thesis.section_7.get('catalyst_score', 0)}/100

"""
        
        if thesis.section_7.get('catalysts'):
            markdown += "**Catalysts:**\n"
            for catalyst in thesis.section_7['catalysts']:
                markdown += f"- {catalyst}\n"
            markdown += "\n"
        
        markdown += f"""## 🎯 Investment Recommendation

**Action:** `{thesis.section_8.get('action', 'HOLD')}`  
**Entry Price:** ${thesis.section_8.get('entry_price', 0):.2f}

**Key Reasons:**
"""
        
        if thesis.section_8.get('key_reasons'):
            for reason in thesis.section_8['key_reasons']:
                markdown += f"- {reason}\n"
        
        markdown += f"""
---

**Investment Thesis:** {thesis.section_8.get('investment_thesis_summary', 'No summary available')}
"""
        
        return markdown

    def section_4_quality_and_moat(self) -> Dict:
        """Section 4: Quality and Moat Assessment (0-100 score)"""
        if self.fundamentals is None:
            return {'score': 0, 'moat_type': 'None', 'competitive_advantages': []}
        
        score = 0.0
        competitive_advantages = []
        
        # ROE assessment (25 points max)
        roe = self._safe_get(self.fundamentals, 'roe', 0)
        if roe > 20:
            roe_score = 25
            competitive_advantages.append(f"Excellent ROE: {roe:.1f}%")
        elif roe > 15:
            roe_score = 15
        elif roe > 10:
            roe_score = 5
        else:
            roe_score = 0
        
        score += roe_score
        
        # Margin consistency vs sector (25 points max)
        sector = self._safe_get(self.fundamentals, 'sector', 'Unknown')
        sector_averages = self._get_sector_averages(sector)
        operating_margin = self._safe_get(self.fundamentals, 'operating_margin', 0)
        
        if sector_averages['avg_operating_margin'] > 0:
            margin_vs_sector = operating_margin / sector_averages['avg_operating_margin']
            if margin_vs_sector > 1.5:
                margin_score = 25
                competitive_advantages.append(f"Superior margins vs sector: {margin_vs_sector:.1f}x")
            elif margin_vs_sector > 1.2:
                margin_score = 15
            elif margin_vs_sector > 1.0:
                margin_score = 5
            else:
                margin_score = 0
        else:
            margin_score = 10 if operating_margin > 15 else 0
        
        score += margin_score
        
        # Growth quality (25 points max)
        eps_growth = self._safe_get(self.fundamentals, 'eps_growth_5y', 0)
        revenue_growth = self._safe_get(self.fundamentals, 'revenue_growth_5y', 0)
        
        avg_growth = (eps_growth + revenue_growth) / 2
        if avg_growth > 15:
            growth_score = 25
            competitive_advantages.append(f"Strong growth: {avg_growth:.1f}% avg")
        elif avg_growth > 10:
            growth_score = 15
        elif avg_growth > 5:
            growth_score = 5
        else:
            growth_score = 0
        
        score += growth_score
        
        # Moat indicators (25 points max)
        moat_score = 0
        
        # High margins = pricing power
        if operating_margin > 20:
            moat_score += 10
            competitive_advantages.append("Pricing power (high margins)")
        
        # Low debt = financial moat
        debt_to_equity = self._safe_get(self.fundamentals, 'debt_to_equity', 0)
        if debt_to_equity < 0.3:
            moat_score += 10
            competitive_advantages.append("Financial moat (low debt)")
        
        # High ROIC = competitive advantage
        roic = self._safe_get(self.fundamentals, 'roic', 0)
        if roic > 15:
            moat_score += 5
            competitive_advantages.append(f"Competitive advantage (ROIC: {roic:.1f}%)")
        
        score += moat_score
        
        # Classify moat
        if score > 80:
            moat_type = "Wide"
        elif score > 60:
            moat_type = "Narrow"
        else:
            moat_type = "None"
        
        return {
            'score': max(0, min(100, score)),
            'moat_type': moat_type,
            'competitive_advantages': competitive_advantages,
            'breakdown': {
                'roe_score': roe_score,
                'margin_score': margin_score,
                'growth_score': growth_score,
                'moat_score': moat_score
            }
        }
    
    def section_5_historical_performance(self) -> Dict:
        """Section 5: Historical Performance Analysis"""
        if self.historical_prices.empty:
            return {'returns': {}, 'volatility': 0, 'trend': 'Unknown', 'position_vs_high': 0}
        
        # Calculate returns for different periods
        prices = self.historical_prices['close_price']
        returns = {}
        
        # Calculate returns for available periods
        if len(prices) >= 1:
            returns['1m'] = ((prices.iloc[-1] / prices.iloc[-1]) - 1) * 100 if len(prices) >= 1 else 0
        
        if len(prices) >= 3:
            returns['3m'] = ((prices.iloc[-1] / prices.iloc[-3]) - 1) * 100
        
        if len(prices) >= 6:
            returns['6m'] = ((prices.iloc[-1] / prices.iloc[-6]) - 1) * 100
        
        if len(prices) >= 12:
            returns['1y'] = ((prices.iloc[-1] / prices.iloc[-12]) - 1) * 100
        
        if len(prices) >= 60:
            returns['5y'] = ((prices.iloc[-1] / prices.iloc[-60]) - 1) * 100
        
        # Calculate volatility
        if len(prices) > 1:
            price_changes = prices.pct_change().dropna()
            volatility = price_changes.std() * np.sqrt(252) * 100  # Annualized
        else:
            volatility = 0
        
        # Determine trend (50-day MA vs 200-day MA)
        trend = "Unknown"
        if len(prices) >= 200:
            ma_50 = prices.tail(50).mean()
            ma_200 = prices.tail(200).mean()
            if ma_50 > ma_200 * 1.05:
                trend = "Bullish"
            elif ma_50 < ma_200 * 0.95:
                trend = "Bearish"
            else:
                trend = "Sideways"
        
        # Current position vs 52-week high
        position_vs_high = 0
        if not self.extended_price_data.empty:
            latest_extended = self.extended_price_data.iloc[0]
            high_52w = self._safe_get(latest_extended, 'fifty_two_week_high', 0)
            current_price = prices.iloc[-1]
            
            if high_52w > 0:
                position_vs_high = ((current_price - high_52w) / high_52w) * 100
        
        return {
            'returns': returns,
            'volatility': volatility,
            'trend': trend,
            'position_vs_high': position_vs_high,
            'data_points': len(prices)
        }
    
    def section_6_risk_assessment(self) -> Dict:
        """Section 6: Risk Assessment"""
        if self.fundamentals is None:
            return {'overall_risk': 'High', 'risk_factors': ['No fundamental data'], 'risk_score': 100}
        
        risk_factors = []
        risk_score = 0
        
        # Financial risks
        debt_to_equity = self._safe_get(self.fundamentals, 'debt_to_equity', 0)
        if debt_to_equity > 1.5:
            risk_factors.append(f"High financial risk (D/E: {debt_to_equity:.2f})")
            risk_score += 30
        
        current_ratio = self._safe_get(self.fundamentals, 'current_ratio', 0)
        if current_ratio < 1.0:
            risk_factors.append(f"Liquidity risk (Current ratio: {current_ratio:.2f})")
            risk_score += 25
        
        fcf_yield = self._safe_get(self.fundamentals, 'fcf_yield', 0)
        if fcf_yield < 0:
            risk_factors.append(f"Cash burn risk (Negative FCF yield: {fcf_yield:.1f}%)")
            risk_score += 20
        
        # Market risks
        beta = self._safe_get(self.fundamentals, 'beta', 1.0)
        if beta > 1.3:
            risk_factors.append(f"High volatility risk (Beta: {beta:.2f})")
            risk_score += 15
        
        max_drawdown = self._safe_get(self.fundamentals, 'max_drawdown_5y', 0)
        if max_drawdown < -50:
            risk_factors.append(f"Severe historical risk (Max drawdown: {max_drawdown:.1f}%)")
            risk_score += 20
        
        # Valuation risk
        pe_ratio = self._safe_get(self.fundamentals, 'pe_ratio', 0)
        sector = self._safe_get(self.fundamentals, 'sector', 'Unknown')
        sector_averages = self._get_sector_averages(sector)
        
        if pe_ratio > 0 and sector_averages['avg_pe'] > 0:
            pe_vs_sector = pe_ratio / sector_averages['avg_pe']
            if pe_vs_sector > 1.8:
                risk_factors.append(f"Overvaluation risk (PE {pe_vs_sector:.1f}x sector avg)")
                risk_score += 15
        
        # Determine overall risk
        if risk_score >= 70:
            overall_risk = "High"
        elif risk_score >= 40:
            overall_risk = "Medium"
        else:
            overall_risk = "Low"
        
        return {
            'overall_risk': overall_risk,
            'risk_factors': risk_factors,
            'risk_score': min(100, risk_score)
        }
    
    def section_7_catalysts_and_opportunities(self) -> Dict:
        """Section 7: Catalysts and Opportunities"""
        catalysts = []
        catalyst_score = 0
        
        # Analyst rating upgrades (last 90 days)
        if not self.analyst_ratings.empty:
            recent_ratings = self.analyst_ratings[
                pd.to_datetime(self.analyst_ratings['rating_date']) >= 
                datetime.now() - timedelta(days=90)
            ]
            
            upgrades = recent_ratings[recent_ratings['action'].str.contains('upgrade', case=False, na=False)]
            if not upgrades.empty:
                catalysts.append(f"Recent analyst upgrades: {len(upgrades)}")
                catalyst_score += 20
        
        # Positive earnings surprises
        if not self.earnings_history.empty:
            recent_earnings = self.earnings_history.head(4)  # Last 4 quarters
            positive_surprises = recent_earnings[recent_earnings['surprise_pct'] > 0]
            
            if not positive_surprises.empty:
                avg_surprise = positive_surprises['surprise_pct'].mean()
                catalysts.append(f"Positive earnings surprises: {avg_surprise:.1f}% avg")
                catalyst_score += 15
        
        # Technical signals
        if not self.historical_prices.empty:
            current_price = self.historical_prices.iloc[-1]['close_price']
            
            # Check if oversold (price < -20% from 52w high)
            if not self.extended_price_data.empty:
                high_52w = self._safe_get(self.extended_price_data.iloc[0], 'fifty_two_week_high', 0)
                if high_52w > 0:
                    decline_from_high = ((current_price - high_52w) / high_52w) * 100
                    if decline_from_high < -20:
                        catalysts.append(f"Oversold condition: {decline_from_high:.1f}% from high")
                        catalyst_score += 10
        
        # Dividend increases
        if not self.corporate_actions.empty:
            dividend_actions = self.corporate_actions[
                self.corporate_actions['action_type'].str.contains('dividend', case=False, na=False)
            ]
            if not dividend_actions.empty:
                catalysts.append("Recent dividend activity")
                catalyst_score += 5
        
        return {
            'catalysts': catalysts,
            'catalyst_score': min(100, catalyst_score)
        }
    
    def section_8_investment_recommendation(self) -> Dict:
        """Section 8: Investment Recommendation"""
        # Get scores from previous sections
        financial_health = self.section_2_financial_health()
        quality_moat = self.section_4_quality_and_moat()
        valuation = self.section_3_valuation_analysis()
        catalysts = self.section_7_catalysts_and_opportunities()
        
        financial_score = financial_health.get('score', 0)
        quality_score = quality_moat.get('score', 0)
        valuation_discount = valuation.get('discount_pct', 0)
        catalyst_score = catalysts.get('catalyst_score', 0)
        
        # Convert valuation discount to 0-100 scale
        valuation_score = max(0, min(100, 50 + valuation_discount))
        
        # Composite score calculation
        composite = (
            financial_score * 0.25 +
            quality_score * 0.25 +
            valuation_score * 0.30 +
            catalyst_score * 0.20
        )
        
        # Determine action
        if composite > 80:
            action = "STRONG BUY"
        elif composite > 70:
            action = "BUY"
        elif composite > 55:
            action = "HOLD"
        else:
            action = "PASS"
        
        # Calculate confidence based on data completeness
        completeness = self._assess_data_completeness()
        confidence = 0.0
        
        if completeness['has_fundamentals']:
            confidence += 0.2
        if completeness['has_price_history'] and len(self.historical_prices) > 60:
            confidence += 0.2
        if completeness['has_analyst_ratings']:
            confidence += 0.2
        if completeness['has_institutional_holdings']:
            confidence += 0.2
        if completeness['has_earnings_history']:
            confidence += 0.2
        
        # Entry strategy
        current_price = valuation.get('current_price', 0)
        entry_price = current_price
        
        if action in ["BUY", "STRONG BUY"]:
            margin_of_safety = max(0.1, abs(valuation_discount) / 100)
            entry_price = current_price * (1 - margin_of_safety * 0.5)
        
        # Key reasons
        key_reasons = []
        if financial_score > 70:
            key_reasons.append("Strong financial health")
        if quality_score > 70:
            key_reasons.append("Quality business with competitive moat")
        if valuation_discount > 20:
            key_reasons.append("Significantly undervalued")
        if catalyst_score > 50:
            key_reasons.append("Positive catalysts present")
        
        # Investment thesis summary
        thesis_summary = f"{action} {self.ticker} - {composite:.0f}/100 composite score. "
        thesis_summary += f"Financial health: {financial_score:.0f}/100, "
        thesis_summary += f"Quality: {quality_score:.0f}/100, "
        thesis_summary += f"Valuation: {valuation_discount:+.1f}% discount, "
        thesis_summary += f"Confidence: {confidence:.0%}"
        
        return {
            'action': action,
            'composite_score': composite,
            'confidence': confidence,
            'entry_price': entry_price,
            'key_reasons': key_reasons,
            'investment_thesis_summary': thesis_summary,
            'score_breakdown': {
                'financial_health': financial_score,
                'quality_moat': quality_score,
                'valuation': valuation_score,
                'catalysts': catalyst_score
            }
        }

def main():
    """Example usage of Deep Stock Investigation"""
    try:
        print("="*80)
        print("DEEP STOCK INVESTIGATION TOOL - STAGE 2 ANALYSIS")
        print("="*80)
        print("This tool performs comprehensive deep-dive analysis using unified_stock_data.db")
        print("No external data collection - works entirely with your database.")
        print("="*80)
        
        # Example with NZX stock
        ticker = 'AIR.NZ'  # Air New Zealand
        print(f"\nAnalyzing: {ticker}")
        
        investigator = DeepStockInvestigation(
            'data_collection/unified_stock_data.db', 
            ticker
        )
        
        # Print comprehensive summary
        investigator.print_summary()
        
        # Export to Excel
        excel_filename = f"{ticker.replace('.', '_')}_investment_thesis.xlsx"
        investigator.export_to_excel(excel_filename)
        print(f"\nExcel report exported to: {excel_filename}")
        
        # Generate Notion markdown
        markdown = investigator.export_to_notion_markdown()
        print(f"\nNotion markdown generated (copy to clipboard):")
        print("-" * 50)
        print(markdown[:500] + "..." if len(markdown) > 500 else markdown)
        print("-" * 50)
        
        print(f"\n{'='*80}")
        print("ANALYSIS COMPLETE!")
        print("="*80)
        print("Key Features:")
        print("✓ 8-section investment thesis analysis")
        print("✓ Database-only approach (no external API calls)")
        print("✓ Sector comparison from your data")
        print("✓ Multiple valuation methods")
        print("✓ Risk assessment and catalyst identification")
        print("✓ Excel export with multiple sheets")
        print("✓ Notion-compatible markdown")
        print("="*80)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        print(f"Error: {e}")
        print("\nMake sure:")
        print("1. Database exists at: data_collection/unified_stock_data.db")
        print("2. Ticker exists in the database")
        print("3. Database has been populated with stock data")

if __name__ == "__main__":
    main()