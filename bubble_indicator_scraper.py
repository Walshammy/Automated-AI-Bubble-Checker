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
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment
import warnings

warnings.filterwarnings('ignore')

class BubbleIndicatorScraper:
    def __init__(self):
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # OneDrive directory (only storage location)
        self.onedrive_dir = r"C:\Users\james\OneDrive - Silverdale Medical Limited\AIbubble"
        os.makedirs(self.onedrive_dir, exist_ok=True)

        # Master dataset file (OneDrive only)
        self.master_file = os.path.join(self.onedrive_dir, "bubble_indicators_dataset.xlsx")

        # Key stock tickers for tracking
        self.ai_stocks = {
            'NVDA': 'NVIDIA',
            'MSFT': 'Microsoft',
            'GOOGL': 'Alphabet',
            'AAPL': 'Apple',
            'AMZN': 'Amazon',
            'META': 'Meta',
            'TSLA': 'Tesla'
        }

        # Market indices
        self.indices = {
            '^GSPC': 'S&P 500',
            '^IXIC': 'NASDAQ',
            '^VIX': 'VIX',
            '^TNX': '10-Year Treasury'
        }

    def get_stock_data(self, ticker, period="1d"):
        """Get current stock data using yfinance"""
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period=period)
            info = stock.info

            if hist.empty:
                self.logger.warning(f"No historical data for {ticker}")
                return None

            current_price = hist['Close'].iloc[-1]
            market_cap = info.get('marketCap', 0)
            pe_ratio = info.get('trailingPE', None)
            forward_pe = info.get('forwardPE', None)
            price_to_sales = info.get('priceToSalesTrailing12Months', None)
            volume = hist['Volume'].iloc[-1] if 'Volume' in hist.columns else None

            return {
                'ticker': ticker,
                'price': current_price,
                'market_cap': market_cap,
                'pe_ratio': pe_ratio,
                'forward_pe': forward_pe,
                'price_to_sales': price_to_sales,
                'volume': volume
            }
        except Exception as e:
            self.logger.error(f"Error getting data for {ticker}: {e}")
            return None

    def get_vix_data(self):
        """Get VIX (Volatility Index) data"""
        try:
            vix = yf.Ticker("^VIX")
            hist = vix.history(period="1d")
            if not hist.empty:
                current_vix = hist['Close'].iloc[-1]
                return {
                    'vix_level': current_vix,
                    'vix_interpretation': self.interpret_vix(current_vix)
                }
        except Exception as e:
            self.logger.error(f"Error getting VIX data: {e}")
        return {'vix_level': None, 'vix_interpretation': 'N/A'}

    def interpret_vix(self, vix_level):
        """Interpret VIX levels for bubble analysis"""
        try:
            vix_level = float(vix_level)
        except Exception:
            return "N/A"
        if vix_level < 12:
            return "Very Low - Complacency Risk"
        elif vix_level < 20:
            return "Low - Normal Market"
        elif vix_level < 30:
            return "Elevated - Increased Volatility"
        elif vix_level < 40:
            return "High - Market Stress"
        else:
            return "Very High - Panic/Crisis"

    def get_sp500_pe_ratio(self):
        """Get S&P 500 P/E ratio from yfinance"""
        try:
            sp500 = yf.Ticker("^GSPC")
            hist = sp500.history(period="1d")
            info = sp500.info
            current_price = hist['Close'].iloc[-1] if not hist.empty else None
            estimated_pe = info.get('trailingPE', None)
            return {
                'sp500_price': current_price,
                'sp500_pe_estimate': estimated_pe
            }
        except Exception as e:
            self.logger.error(f"Error getting S&P 500 P/E: {e}")
            return {'sp500_price': None, 'sp500_pe_estimate': None}

    def calculate_market_concentration(self):
        """Calculate market concentration of top companies"""
        try:
            top_companies = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'TSLA', 'BRK-B', 'LLY', 'V']
            total_top_10_cap = 0
            company_data = {}
            for ticker in top_companies:
                data = self.get_stock_data(ticker)
                if data and data['market_cap']:
                    total_top_10_cap += data['market_cap']
                    company_data[ticker] = data['market_cap']
            sp500_approximate_cap = total_top_10_cap * 2.5 if total_top_10_cap > 0 else 0
            concentration_ratio = (total_top_10_cap / sp500_approximate_cap) * 100 if sp500_approximate_cap > 0 else 0
            return {
                'top_10_market_cap': total_top_10_cap,
                'concentration_ratio': concentration_ratio,
                'company_breakdown': company_data
            }
        except Exception as e:
            self.logger.error(f"Error calculating market concentration: {e}")
            return {'top_10_market_cap': 0, 'concentration_ratio': 0, 'company_breakdown': {}}

    def get_interest_rates(self):
        """Get current interest rates"""
        try:
            tnx = yf.Ticker("^TNX")
            hist = tnx.history(period="1d")
            ten_year_yield = hist['Close'].iloc[-1] if not hist.empty else None

            three_month = yf.Ticker("^IRX")
            hist_3m = three_month.history(period="1d")
            fed_funds_approx = hist_3m['Close'].iloc[-1] if not hist_3m.empty else None

            return {
                'ten_year_treasury': ten_year_yield,
                'fed_funds_rate_approx': fed_funds_approx
            }
        except Exception as e:
            self.logger.error(f"Error getting interest rates: {e}")
            return {'ten_year_treasury': None, 'fed_funds_rate_approx': None}

    def get_ai_sector_metrics(self):
        """Get specific AI sector bubble indicators"""
        try:
            ai_metrics = {}
            total_ai_market_cap = 0
            for ticker, company in self.ai_stocks.items():
                data = self.get_stock_data(ticker)
                safe_name = company.lower().replace(' ', '_')
                if data:
                    ai_metrics[f"{safe_name}_price"] = data['price']
                    ai_metrics[f"{safe_name}_market_cap"] = data['market_cap']
                    ai_metrics[f"{safe_name}_pe"] = data['pe_ratio']
                    if data['market_cap']:
                        total_ai_market_cap += data['market_cap']
            ai_metrics['total_ai_market_cap'] = total_ai_market_cap
            nvidia_data = self.get_stock_data('NVDA')
            if nvidia_data and nvidia_data['market_cap'] and total_ai_market_cap > 0:
                ai_metrics['nvidia_dominance_ratio'] = (nvidia_data['market_cap'] / total_ai_market_cap) * 100
            else:
                ai_metrics['nvidia_dominance_ratio'] = 0
            return ai_metrics
        except Exception as e:
            self.logger.error(f"Error getting AI sector metrics: {e}")
            return {}

    def assess_bubble_risk(self, data):
        """Assess overall bubble risk based on collected metrics"""
        risk_factors = []
        risk_score = 0

        vix_level = data.get('vix_level', 50)
        try:
            vix_level = float(vix_level)
        except Exception:
            vix_level = 50

        if vix_level < 12:
            risk_factors.append("VIX extremely low - complacency risk")
            risk_score += 2
        elif vix_level < 15:
            risk_factors.append("VIX low - reduced fear")
            risk_score += 1

        concentration = data.get('concentration_ratio', 0)
        try:
            concentration = float(concentration)
        except Exception:
            concentration = 0

        if concentration > 35:
            risk_factors.append("High market concentration risk")
            risk_score += 2
        elif concentration > 30:
            risk_factors.append("Elevated market concentration")
            risk_score += 1

        nvidia_dominance = data.get('nvidia_dominance_ratio', 0)
        try:
            nvidia_dominance = float(nvidia_dominance)
        except Exception:
            nvidia_dominance = 0

        if nvidia_dominance > 40:
            risk_factors.append("NVIDIA market dominance risk")
            risk_score += 2
        elif nvidia_dominance > 30:
            risk_factors.append("High NVIDIA concentration")
            risk_score += 1

        sp500_pe = data.get('sp500_pe_estimate', 20)
        try:
            sp500_pe = float(sp500_pe)
        except Exception:
            sp500_pe = 20

        if sp500_pe > 30:
            risk_factors.append("Elevated S&P 500 P/E ratio")
            risk_score += 2
        elif sp500_pe > 25:
            risk_factors.append("High S&P 500 P/E ratio")
            risk_score += 1

        ten_year = data.get('ten_year_treasury', 5)
        try:
            ten_year = float(ten_year)
        except Exception:
            ten_year = 5

        if ten_year < 2:
            risk_factors.append("Very low interest rates - asset inflation risk")
            risk_score += 1

        if risk_score >= 6:
            risk_level = "HIGH BUBBLE RISK"
        elif risk_score >= 4:
            risk_level = "MODERATE BUBBLE RISK"
        elif risk_score >= 2:
            risk_level = "ELEVATED BUBBLE RISK"
        else:
            risk_level = "LOW BUBBLE RISK"

        return {
            'bubble_risk_score': risk_score,
            'bubble_risk_level': risk_level,
            'risk_factors': '; '.join(risk_factors)
        }

    def collect_all_metrics(self):
        """Collect all bubble indicator metrics"""
        self.logger.info("Starting bubble indicator data collection")

        data = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'time': datetime.now().strftime('%H:%M:%S'),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        self.logger.info("Collecting VIX data...")
        vix_data = self.get_vix_data()
        data.update(vix_data)

        self.logger.info("Collecting S&P 500 data...")
        sp500_data = self.get_sp500_pe_ratio()
        data.update(sp500_data)

        self.logger.info("Calculating market concentration...")
        concentration_data = self.calculate_market_concentration()
        data.update(concentration_data)

        self.logger.info("Collecting interest rate data...")
        rates_data = self.get_interest_rates()
        data.update(rates_data)

        self.logger.info("Collecting AI sector metrics...")
        ai_data = self.get_ai_sector_metrics()
        data.update(ai_data)

        self.logger.info("Assessing bubble risk...")
        risk_assessment = self.assess_bubble_risk(data)
        data.update(risk_assessment)

        self.logger.info("Collecting market indices...")
        for ticker, name in self.indices.items():
            try:
                index_data = self.get_stock_data(ticker)
                if index_data:
                    safe_name = name.lower().replace(' ', '_').replace('-', '_')
                    data[f"{safe_name}_price"] = index_data['price']
                    if index_data['pe_ratio'] is not None:
                        data[f"{safe_name}_pe"] = index_data['pe_ratio']
            except Exception as e:
                self.logger.error(f"Error getting {name} data: {e}")

        self.logger.info(f"Collected {len(data)} metrics")
        return data

    def load_existing_dataset(self):
        """Load existing dataset if it exists"""
        if os.path.exists(self.master_file):
            try:
                df = pd.read_excel(self.master_file)
                self.logger.info(f"Loaded existing dataset with {len(df)} records")
                return df
            except Exception as e:
                self.logger.error(f"Error loading existing dataset: {e}")
                return pd.DataFrame()
        else:
            self.logger.info("No existing dataset found, creating new one")
            return pd.DataFrame()

    def update_dataset(self, new_data):
        """Update dataset with new daily data"""
        existing_df = self.load_existing_dataset()
        new_df = pd.DataFrame([new_data])

        if existing_df.empty:
            updated_df = new_df
        else:
            today = datetime.now().strftime('%Y-%m-%d')
            if 'date' in existing_df.columns:
                today_mask = existing_df['date'] == today
                if today_mask.any():
                    for col in new_df.columns:
                        if col in existing_df.columns:
                            existing_df.loc[today_mask, col] = new_df.iloc[0][col]
                        else:
                            existing_df[col] = None
                            existing_df.loc[today_mask, col] = new_df.iloc[0][col]
                    updated_df = existing_df
                    self.logger.info("Updated today's existing record")
                else:
                    updated_df = pd.concat([existing_df, new_df], ignore_index=True)
                    self.logger.info("Added new daily record")
            else:
                updated_df = pd.concat([existing_df, new_df], ignore_index=True)
                self.logger.info("Added new daily record (no date column found)")

        updated_df = updated_df.sort_values('date', ascending=False)
        updated_df = updated_df.reset_index(drop=True)
        return updated_df

    def apply_conditional_formatting(self, filepath):
        """Apply conditional formatting to Excel file"""
        try:
            wb = load_workbook(filepath)
            ws = wb.active

            header_fill = PatternFill(start_color="2F4F4F", end_color="2F4F4F", fill_type="solid")
            header_font = Font(bold=True, color="FFFFFF")

            high_risk_fill = PatternFill(start_color="FFB6C1", end_color="FFB6C1", fill_type="solid")  # Light red
            moderate_risk_fill = PatternFill(start_color="FFE4B5", end_color="FFE4B5", fill_type="solid")  # Light orange
            low_risk_fill = PatternFill(start_color="90EE90", end_color="90EE90", fill_type="solid")  # Light green

            # Style header row
            for col in range(1, ws.max_column + 1):
                cell = ws.cell(row=1, column=col)
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal='center', vertical='center')

            # Find bubble risk level column
            risk_level_col = None
            for col in range(1, ws.max_column + 1):
                if ws.cell(row=1, column=col).value == 'bubble_risk_level':
                    risk_level_col = col
                    break
            
            # Apply risk-based formatting
            if risk_level_col:
                for row in range(2, ws.max_row + 1):
                    risk_cell = ws.cell(row=row, column=risk_level_col)
                    risk_value = str(risk_cell.value).upper()
                    
                    if 'HIGH' in risk_value:
                        row_fill = high_risk_fill
                    elif 'MODERATE' in risk_value:
                        row_fill = moderate_risk_fill
                    else:
                        row_fill = low_risk_fill
                    
                    # Apply to entire row
                    for col in range(1, ws.max_column + 1):
                        ws.cell(row=row, column=col).fill = row_fill
            
            # Auto-adjust column widths
            for column in ws.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                for cell in column:
                    try:
                        if cell.value is not None:
                            max_length = max(max_length, len(str(cell.value)))
                    except:
                        pass
                
                adjusted_width = min(max_length + 2, 50)
                ws.column_dimensions[column_letter].width = adjusted_width
            
            wb.save(filepath)
            self.logger.info("Applied conditional formatting")
            
        except Exception as e:
            self.logger.error(f"Error applying formatting: {e}")

    def save_dataset(self, df):
        """Save dataset to OneDrive only"""
        if df.empty:
            self.logger.warning("No data to save")
            return
        
        try:
            # Save to OneDrive
            df.to_excel(self.master_file, index=False, engine='openpyxl')
            self.logger.info(f"Bubble indicators saved to: {self.master_file}")
            
            # Apply formatting
            self.apply_conditional_formatting(self.master_file)
            
            # Print summary
            print(f"\n=== Bubble Indicators Summary ===")
            print(f"Total daily records: {len(df)}")
            if not df.empty:
                latest = df.iloc[0]
                print(f"Latest data: {latest['date']}")
                print(f"Current VIX: {latest.get('vix_level', 'N/A')}")
                print(f"Bubble Risk Level: {latest.get('bubble_risk_level', 'N/A')}")
                print(f"Market Concentration: {latest.get('concentration_ratio', 'N/A'):.1f}%")
            print(f"OneDrive dataset: {self.master_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving dataset: {e}")

    def run(self):
        """Main execution method"""
        self.logger.info("Starting Bubble Indicator Scraper")
        start_time = datetime.now()
        
        try:
            # Collect all metrics
            new_data = self.collect_all_metrics()
            
            # Update dataset
            updated_df = self.update_dataset(new_data)
            
            # Save dataset
            self.save_dataset(updated_df)
            
            end_time = datetime.now()
            duration = end_time - start_time
            self.logger.info(f"Bubble indicator update completed in {duration}")
            
        except Exception as e:
            self.logger.error(f"Error during bubble indicator update: {e}")

def main():
    """Main function"""
    scraper = BubbleIndicatorScraper()
    scraper.run()

if __name__ == "__main__":
    main()
