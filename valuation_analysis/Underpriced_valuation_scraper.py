#!/usr/bin/env python3
"""
Undervalued Stock Screener with Data Validation
================================================

Screens all stocks in database to find top 5 undervalued opportunities.
Validates data completeness before proceeding.

Author: AI Assistant  
Date: 2025-01-02
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class UndervaluedStockScreener:
    """
    Screens database for undervalued stock opportunities.
    Validates data quality before analysis.
    """

    def __init__(self, db_path: str = 'data_collection/unified_stock_data.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.data_quality = None

    def validate_database(self) -> Dict:
        """
        Validate database has sufficient data for screening.
        Returns data quality report.
        """
        logger.info("Validating database structure and data quality...")

        validation = {
            'is_valid': False,
            'stock_count': 0,
            'missing_fields': [],
            'data_coverage': {},
            'recommendations': []
        }

        # Check if current_fundamentals table exists and has data
        try:
            fundamentals = pd.read_sql_query(
                "SELECT * FROM current_fundamentals LIMIT 5",
                self.conn
            )

            validation['stock_count'] = pd.read_sql_query(
                "SELECT COUNT(DISTINCT ticker) as count FROM current_fundamentals",
                self.conn
            ).iloc[0]['count']

            # Check critical fields for valuation
            critical_fields = [
                'pe_ratio', 'pb_ratio', 'market_cap', 'roe',
                'net_margin', 'debt_to_equity', 'sector'
            ]

            existing_fields = fundamentals.columns.tolist()

            for field in critical_fields:
                if field not in existing_fields:
                    validation['missing_fields'].append(field)
                else:
                    # Check data coverage
                    coverage = pd.read_sql_query(
                        f"SELECT COUNT(*) as total, "
                        f"SUM(CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END) as has_data "
                        f"FROM current_fundamentals",
                        self.conn
                    ).iloc[0]

                    pct_coverage = (coverage['has_data'] / coverage['total'] * 100) if coverage['total'] > 0 else 0
                    validation['data_coverage'][field] = pct_coverage

            # Check historical prices
            price_count = pd.read_sql_query(
                "SELECT COUNT(DISTINCT ticker) as count FROM historical_prices",
                self.conn
            ).iloc[0]['count']

            validation['data_coverage']['historical_prices'] = (
                price_count / validation['stock_count'] * 100
                if validation['stock_count'] > 0 else 0
            )

            # Determine if valid for screening
            min_coverage = 60  # Need at least 60% data coverage
            avg_coverage = np.mean(list(validation['data_coverage'].values()))

            if validation['stock_count'] < 10:
                validation['recommendations'].append(
                    f"Only {validation['stock_count']} stocks found. Need at least 10 for meaningful screening."
                )

            if validation['missing_fields']:
                validation['recommendations'].append(
                    f"Missing critical fields: {', '.join(validation['missing_fields'])}"
                )

            if avg_coverage < min_coverage:
                validation['recommendations'].append(
                    f"Data coverage too low: {avg_coverage:.1f}%. Need at least {min_coverage}%."
                )
                validation['recommendations'].append(
                    "Recommend running data collection scripts to populate database."
                )

            # Mark as valid if we can proceed
            validation['is_valid'] = (
                validation['stock_count'] >= 10 and
                not validation['missing_fields'] and
                avg_coverage >= min_coverage
            )

        except Exception as e:
            validation['recommendations'].append(f"Database error: {str(e)}")
            validation['recommendations'].append(
                "Check that unified_stock_data.db exists and has correct schema."
            )

        self.data_quality = validation
        return validation

    def print_validation_report(self):
        """Print human-readable validation report"""
        if not self.data_quality:
            self.validate_database()

        v = self.data_quality

        print("\n" + "=" * 80)
        print("DATABASE VALIDATION REPORT")
        print("=" * 80)
        print(f"Total Stocks: {v['stock_count']}")
        print(f"Status: {'[READY] READY FOR SCREENING' if v['is_valid'] else '[NOT READY]'}")

        if v['missing_fields']:
            print(f"\n[ERROR] Missing Critical Fields:")
            for field in v['missing_fields']:
                print(f"   - {field}")

        print(f"\nData Coverage:")
        for field, coverage in v['data_coverage'].items():
            status = "[OK]" if coverage >= 60 else "[WARN]" if coverage >= 30 else "[LOW]"
            print(f"   {status} {field:<25} {coverage:>6.1f}%")

        if v['recommendations']:
            print(f"\nRecommendations:")
            for i, rec in enumerate(v['recommendations'], 1):
                print(f"   {i}. {rec}")

        print("=" * 80 + "\n")

    def calculate_undervaluation_score(self) -> pd.DataFrame:
        """
        Calculate undervaluation score for all stocks.
        Higher score = more undervalued.
        """
        if not self.data_quality or not self.data_quality['is_valid']:
            raise ValueError(
                "Database validation failed. Run validate_database() first."
            )

        logger.info("Calculating undervaluation scores...")

        # Load fundamental data
        query = """
        SELECT 
            ticker,
            exchange as market,
            sector,
            pe_ratio,
            pb_ratio,
            peg_ratio,
            ps_ratio,
            dividend_yield,
            fcf_yield as free_cash_flow_yield,
            roe,
            net_margin as profit_margin,
            operating_margin,
            revenue_growth_5y as revenue_growth,
            eps_growth_5y as earnings_growth,
            debt_to_equity,
            current_ratio,
            market_cap
        FROM current_fundamentals
        WHERE pe_ratio IS NOT NULL 
        AND market_cap IS NOT NULL
        """

        df = pd.read_sql_query(query, self.conn)

        logger.info(f"Analyzing {len(df)} stocks with fundamental data...")

        # Clean up data - convert 'Infinity' strings to NaN and ensure numeric types
        numeric_columns = ['pe_ratio', 'pb_ratio', 'peg_ratio', 'ps_ratio', 'dividend_yield', 
                          'free_cash_flow_yield', 'roe', 'profit_margin', 'operating_margin', 
                          'revenue_growth', 'earnings_growth', 'debt_to_equity', 'current_ratio', 'market_cap']
        
        for col in numeric_columns:
            if col in df.columns:
                # Replace infinity strings and convert to numeric
                df[col] = df[col].replace(['Infinity', '-Infinity', 'inf', '-inf'], np.nan)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Handle extreme outliers by capping them at reasonable values
        df['pe_ratio'] = df['pe_ratio'].clip(upper=1000)  # Cap P/E at 1000
        df['pb_ratio'] = df['pb_ratio'].clip(upper=50)    # Cap P/B at 50
        df['roe'] = df['roe'].clip(-100, 100)             # Cap ROE between -100% and 100%

        # Calculate sector averages for comparison with minimum data requirements
        # Find sectors with sufficient data points (at least 5)
        sector_counts = df.groupby('sector').size()
        valid_sectors = sector_counts[sector_counts >= 5].index
        
        # Calculate sector medians only for sectors with sufficient data
        df['sector_avg_pe'] = df.groupby('sector')['pe_ratio'].transform('median')
        df['sector_avg_pb'] = df.groupby('sector')['pb_ratio'].transform('median')
        df['sector_avg_roe'] = df.groupby('sector')['roe'].transform('median')
        
        # Fill missing sector averages with overall medians for sectors with <5 stocks
        global_pe_median = df['pe_ratio'].median()
        global_pb_median = df['pb_ratio'].median()
        global_roe_median = df['roe'].median()
        
        sectors_with_few_data = sector_counts[sector_counts < 5].index
        for sector in sectors_with_few_data:
            df.loc[df['sector'] == sector, 'sector_avg_pe'] = global_pe_median
            df.loc[df['sector'] == sector, 'sector_avg_pb'] = global_pb_median
            df.loc[df['sector'] == sector, 'sector_avg_roe'] = global_roe_median
        
        logger.info(f"Sector analysis: {len(valid_sectors)} sectors with ≥5 stocks, {len(sectors_with_few_data)} sectors with <5 stocks")

        # Data validation filters - remove unrealistic values
        initial_count = len(df)
        
        # Filter unrealistic valuations
        df = df[
            (df['free_cash_flow_yield'].fillna(0) <= 50) &  # Max 50% FCF yield 
            (df['pe_ratio'].fillna(0) <= 100) &              # Max P/E of 100
            (df['pe_ratio'].fillna(0) > 0) &                 # Positive P/E only
            (df['pb_ratio'].fillna(0) <= 30) &               # Max P/B of 30
            (df['market_cap'].fillna(0) > 0)                 # No zero market cap
        ]
        
        logger.info(f"Data validation: Removed {initial_count - len(df)} stocks with unrealistic values")

        # Score calculation (0-100 scale) - more granular scoring
        df['undervaluation_score'] = 0.0

        # 1. P/E Valuation (0-25 points) - More granular scoring
        df['pe_vs_sector'] = df['pe_ratio'] / df['sector_avg_pe']
        
        # Calculate P/E score more granularly using percentiles
        df['pe_percentile'] = df['pe_vs_sector'].rank(pct=True, ascending=False)  # Lower = better
        df['pe_score'] = np.where(
            df['pe_vs_sector'] < 0.5, 25,
            np.where(df['pe_vs_sector'] < 0.7, 22,
            np.where(df['pe_vs_sector'] < 0.85, 18,
            np.where(df['pe_vs_sector'] < 1.0, 15,
            np.where(df['pe_vs_sector'] < 1.2, 10,
            np.where(df['pe_vs_sector'] < 1.5, 5, 0)))))
        )

        # 2. P/B Valuation (0-20 points) - More granular
        df['pb_vs_sector'] = df['pb_ratio'] / df['sector_avg_pb']
        df['pb_score'] = np.where(
            (df['pb_ratio'] > 0) & (df['pb_ratio'] < 0.5), 20,
            np.where((df['pb_ratio'] < 1.0), 18,
            np.where((df['pb_vs_sector'] < 0.7), 15,
            np.where((df['pb_vs_sector'] < 0.8), 12,
            np.where((df['pb_vs_sector'] < 1.0), 8,
            np.where((df['pb_vs_sector'] < 1.2), 4, 0)))))
        )

        # 3. PEG Ratio (0-20 points)
        # PEG < 1 = undervalued growth
        df['peg_score'] = np.where(
            (df['peg_ratio'] > 0) & (df['peg_ratio'] < 0.7), 20,
            np.where((df['peg_ratio'] < 1.0), 15,
            np.where((df['peg_ratio'] < 1.5), 10, 0))
        )

        # 4. Free Cash Flow Yield (0-15 points)
        df['fcf_score'] = np.where(
            df['free_cash_flow_yield'] > 10, 15,
            np.where(df['free_cash_flow_yield'] > 7, 12,
            np.where(df['free_cash_flow_yield'] > 5, 8, 0))
        )

        # 5. Quality Check - ROE vs sector (0-10 points)
        # High ROE but low valuation = quality at discount
        df['roe_vs_sector'] = df['roe'] / df['sector_avg_roe']
        df['quality_score'] = np.where(
            (df['roe'] > 15) & (df['roe_vs_sector'] > 1.0), 10,
            np.where((df['roe'] > 10), 5, 0)
        )

        # 6. Financial Safety (0-10 points)
        # Low debt is good
        df['safety_score'] = np.where(
            df['debt_to_equity'] < 0.3, 10,
            np.where(df['debt_to_equity'] < 0.7, 7,
            np.where(df['debt_to_equity'] < 1.5, 3, 0))
        )

        # Total undervaluation score
        df['undervaluation_score'] = (
            df['pe_score'] +
            df['pb_score'] +
            df['peg_score'] +
            df['fcf_score'] +
            df['quality_score'] +
            df['safety_score']
        )

        # Add technical score if price data available
        df = self._add_technical_score(df)

        return df

    def _add_technical_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add technical analysis score (mean reversion signals)"""
        logger.info("Adding technical analysis scores...")

        # Get recent price data
        price_query = """
        SELECT 
            ticker,
            date,
            close_price as close
        FROM historical_prices
        WHERE date >= date('now', '-300 days')
        ORDER BY ticker, date
        """

        try:
            price_df = pd.read_sql_query(price_query, self.conn)

            if price_df.empty:
                logger.warning("No price data available for technical analysis")
                df['technical_score'] = 0
                return df

            technical_scores = []

            for ticker in df['ticker'].unique():
                ticker_prices = price_df[price_df['ticker'] == ticker].sort_values('date')

                if len(ticker_prices) < 60:
                    technical_scores.append({'ticker': ticker, 'technical_score': 0})
                    continue

                # Calculate technical indicators
                prices = ticker_prices['close']

                # 200-day MA
                ma_200 = prices.tail(200).mean() if len(prices) >= 200 else prices.mean()
                current_price = prices.iloc[-1]

                # Distance from MA (negative = below MA = potential value)
                pct_from_ma = ((current_price - ma_200) / ma_200) * 100

                # 52-week high/low
                high_52w = prices.tail(252).max() if len(prices) >= 252 else prices.max()
                low_52w = prices.tail(252).min() if len(prices) >= 252 else prices.min()

                # Position in range (0-100%)
                range_position = ((current_price - low_52w) / (high_52w - low_52w)) * 100 if (high_52w - low_52w) > 0 else 50

                # Technical score (0-10 points)
                tech_score = 0

                # Below 200-day MA
                if pct_from_ma < -10:
                    tech_score += 5
                elif pct_from_ma < -5:
                    tech_score += 3

                # In lower quartile of 52-week range
                if range_position < 25:
                    tech_score += 5
                elif range_position < 40:
                    tech_score += 3

                technical_scores.append({
                    'ticker': ticker,
                    'technical_score': min(tech_score, 10),
                    'pct_from_200ma': pct_from_ma,
                    'range_position': range_position
                })

            tech_df = pd.DataFrame(technical_scores)
            df = df.merge(tech_df, on='ticker', how='left')
            df['technical_score'] = df['technical_score'].fillna(0)

            # Add technical score to total
            df['undervaluation_score'] = df['undervaluation_score'] + df['technical_score']

        except Exception as e:
            logger.warning(f"Could not calculate technical scores: {e}")
            df['technical_score'] = 0

        return df

    def find_top_undervalued(self, n: int = 5, min_market_cap_us: float = 100_000_000, 
                            min_market_cap_asx: float = 50_000_000, 
                            min_market_cap_nz: float = 0) -> pd.DataFrame:
        """
        Find top N undervalued stocks with region-specific market cap filters.

        Args:
            n: Number of stocks to return
            min_market_cap_us: Minimum market cap for US stocks (default: $100M)
            min_market_cap_asx: Minimum market cap for ASX stocks (default: $50M)
            min_market_cap_nz: Minimum market cap for NZX stocks (default: $0)
        """
        df = self.calculate_undervaluation_score()

        # Apply region-specific market cap filters
        initial_count = len(df)
        
        # US stocks (.MX, .US, or no suffix for major indices)
        us_mask = df['market'].str.contains('NASDAQ|NYSE|NYSEARCA|BATS', case=False, na=False) | \
                 df['ticker'].str.endswith('.US', na=False)
        df_us = df[us_mask & (df['market_cap'] >= min_market_cap_us)]
        
        # ASX stocks
        asx_mask = df['market'].str.contains('ASX', case=False, na=False) | \
                  df['ticker'].str.endswith('.AX', na=False)
        df_asx = df[asx_mask & (df['market_cap'] >= min_market_cap_asx)]
        
        # NZX stocks  
        nzx_mask = df['market'].str.contains('NZX', case=False, na=False) | \
                  df['ticker'].str.endswith('.NZ', na=False)
        df_nzx = df[nzx_mask & (df['market_cap'] >= min_market_cap_nz)]
        
        # Combine filtered results
        df = pd.concat([df_us, df_asx, df_nzx]).drop_duplicates()
        
        logger.info(f"Market cap filtering results:")
        logger.info(f"  Initial stocks: {initial_count}")
        logger.info(f"  US stocks: {len(df_us)} (min ${min_market_cap_us:,.0f})")
        logger.info(f"  ASX stocks: {len(df_asx)} (min ${min_market_cap_asx:,.0f})")
        logger.info(f"  NZ stocks: {len(df_nzx)} (min ${min_market_cap_nz:,.0f})")
        logger.info(f"  Final stocks: {len(df)}")

        # Sort by undervaluation score
        df_sorted = df.sort_values('undervaluation_score', ascending=False)

        # Select top N
        top_stocks = df_sorted.head(n)

        # Select display columns
        display_cols = [
            'ticker', 'market', 'sector', 'undervaluation_score',
            'pe_ratio', 'pe_vs_sector', 'pb_ratio', 'pb_vs_sector',
            'peg_ratio', 'roe', 'free_cash_flow_yield',
            'debt_to_equity', 'market_cap'
        ]

        # Only include columns that exist
        display_cols = [col for col in display_cols if col in top_stocks.columns]

        return top_stocks[display_cols]

    def print_top_opportunities(self, n: int = 5):
        """Print formatted report of top opportunities with region-specific filters"""
        top = self.find_top_undervalued(n=n)

        print("\n" + "=" * 100)
        print(f"TOP {n} UNDERVALUED STOCK OPPORTUNITIES")
        print("=" * 100)
        print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Stocks Analyzed: {self.data_quality['stock_count']}")
        print("=" * 100)

        for i, (idx, row) in enumerate(top.iterrows(), 1):
            print(f"\n{i}. {row['ticker']} ({row.get('market', 'N/A')}) - {row.get('sector', 'Unknown Sector')}")
            print(f"   {'-' * 90}")
            print(f"   Undervaluation Score: {row['undervaluation_score']:.1f}/100")
            print(f"   Valuation Metrics:")
            print(f"      - P/E Ratio: {row.get('pe_ratio', 0):.2f} ({row.get('pe_vs_sector', 1):.1%} of sector avg)")
            print(f"      - P/B Ratio: {row.get('pb_ratio', 0):.2f} ({row.get('pb_vs_sector', 1):.1%} of sector avg)")
            if row.get('peg_ratio', 0) > 0:
                print(f"      - PEG Ratio: {row['peg_ratio']:.2f}")
            print(f"   Quality Metrics:")
            print(f"      - ROE: {row.get('roe', 0):.1f}%")
            if row.get('free_cash_flow_yield', 0) != 0:
                print(f"      - FCF Yield: {row['free_cash_flow_yield']:.1f}%")
            print(f"      - Debt/Equity: {row.get('debt_to_equity', 0):.2f}")
            print(f"   Market Cap: ${row.get('market_cap', 0):,.0f}")

        print("\n" + "=" * 100)
        print("SCREENING METHODOLOGY")
        print("=" * 100)
        print("Market Cap Filters:")
        print("  • US Stocks: >=$100M market cap")
        print("  • ASX Stocks: >=$50M market cap") 
        print("  • NZX Stocks: >=$0M market cap (no minimum)")
        print("\nUndervaluation Score Components:")
        print("  • P/E vs Sector (0-25 pts): Lower relative P/E = higher score")
        print("  • P/B vs Sector (0-20 pts): Lower P/B = higher score")
        print("  • PEG Ratio (0-20 pts): PEG < 1.0 = undervalued growth")
        print("  • FCF Yield (0-15 pts): Higher yield = better cash generation")
        print("  • Quality (0-10 pts): High ROE = quality business")
        print("  • Safety (0-10 pts): Low debt = financial stability")
        print("  • Technical (0-10 pts): Oversold conditions = mean reversion opportunity")
        print("\nData Quality Filters:")
        print("  • FCF Yield capped at 50%")
        print("  • P/E ratio capped at 100")
        print("  • P/B ratio capped at 30")
        print("  • Positive P/E ratios only")
        print("=" * 100 + "\n")

    def export_results(self, filename: str = 'undervalued_opportunities.xlsx', n: int = 20):
        """Export screening results to Excel with region-specific filters"""
        logger.info(f"Exporting results to {filename}...")

        all_stocks = self.find_top_undervalued(n=n*2)  # Get more stocks to ensure we have enough
        top_stocks = all_stocks.head(n)  # Take top N for each sheet

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Sheet 1: Top opportunities
            top_stocks.to_excel(writer, sheet_name=f'Top {n}', index=False)

            # Sheet 2: By market
            for market in all_stocks['market'].unique():
                if pd.notna(market):
                    market_data = all_stocks[all_stocks['market'] == market].sort_values(
                        'undervaluation_score', ascending=False
                    ).head(10)
                    sheet_name = f'Top {market}'[:31]  # Excel sheet name limit
                    market_data.to_excel(writer, sheet_name=sheet_name, index=False)

            # Sheet 3: Data quality report
            quality_df = pd.DataFrame([
                {'Metric': 'Total Stocks', 'Value': self.data_quality['stock_count']},
                {'Metric': 'Validation Status', 'Value': 'PASS' if self.data_quality['is_valid'] else 'FAIL'},
            ])

            for field, coverage in self.data_quality['data_coverage'].items():
                quality_df = pd.concat([quality_df, pd.DataFrame([{
                    'Metric': f'{field} Coverage',
                    'Value': f'{coverage:.1f}%'
                }])], ignore_index=True)

            quality_df.to_excel(writer, sheet_name='Data Quality', index=False)

        logger.info(f"Results exported to {filename}")

    def close(self):
        """Close database connection"""
        self.conn.close()


def main():
    """Main execution"""
    print("\n" + "=" * 100)
    print("UNDERVALUED STOCK SCREENER")
    print("=" * 100)

    # Initialize screener
    screener = UndervaluedStockScreener()

    # Step 1: Validate database
    validation = screener.validate_database()
    screener.print_validation_report()

    # Step 2: Check if we can proceed
    if not validation['is_valid']:
        print("[ERROR] SCREENING CANNOT PROCEED")
        print("\nDatabase validation failed. Please address the recommendations above.")
        print("\nCommon fixes:")
        print("1. Ensure unified_stock_data.db exists at: data_collection/unified_stock_data.db")
        print("2. Run your data collection scripts to populate the database")
        print("3. Verify the database schema matches expected structure")
        screener.close()
        return

    # Step 3: Screen for opportunities
    print("[OK] Database validation passed. Beginning screening...\n")

    try:
        # Find and display top 5 undervalued stocks
        screener.print_top_opportunities(n=5)

        # Export detailed results
        screener.export_results(filename='undervalued_opportunities.xlsx', n=20)
        print("Detailed results exported to: undervalued_opportunities.xlsx")

    except Exception as e:
        print(f"\n[ERROR] Error during screening: {e}")
        logger.error(f"Screening error: {e}", exc_info=True)

    finally:
        screener.close()

    print("\n[OK] Screening complete!\n")


if __name__ == "__main__":
    main()