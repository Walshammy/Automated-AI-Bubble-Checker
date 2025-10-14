#!/usr/bin/env python3
"""
MARKET ANOMALY DETECTION SYSTEM
===============================

Isolation Forest-based anomaly detection for unusual market behavior.
Detects outliers in price/volume patterns across the stock universe.

Features:
- Isolation Forest algorithm for outlier detection
- Price and volume pattern analysis
- Multi-dimensional feature engineering
- Anomaly scoring and ranking
- Visualization of detected anomalies

Author: AI Assistant
Date: 2025-10-14
"""

import sqlite3
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class MarketAnomalyDetector:
    """Isolation Forest-based market anomaly detection"""
    
    def __init__(self, db_path: str = r"C:\Users\james\Downloads\StockDB\unified_stock_data.db"):
        self.db_path = db_path
        self.scaler = StandardScaler()
        self.isolation_forest = IsolationForest(
            contamination=0.1,  # Expect 10% anomalies
            random_state=42,
            n_estimators=100
        )
        self.pca = PCA(n_components=0.95)  # Keep 95% variance
        
    def load_and_prepare_data(self) -> pd.DataFrame:
        """Load and prepare data for anomaly detection"""
        print("Loading market data for anomaly detection...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Load historical price data
        price_query = """
        SELECT 
            ticker,
            date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            adjusted_close,
            exchange
        FROM historical_prices 
        WHERE date >= '2020-01-01'  -- Focus on recent data
        ORDER BY ticker, date
        """
        
        price_data = pd.read_sql_query(price_query, conn)
        
        # Load fundamentals data
        fundamentals_query = """
        SELECT 
            ticker,
            market_cap,
            pe_ratio,
            pb_ratio,
            debt_to_equity,
            beta,
            volatility_1y,
            max_drawdown_5y,
            sector,
            industry
        FROM current_fundamentals
        """
        
        fundamentals_data = pd.read_sql_query(fundamentals_query, conn)
        
        conn.close()
        
        print(f"Loaded {len(price_data):,} price records for {price_data['ticker'].nunique()} tickers")
        print(f"Loaded {len(fundamentals_data):,} fundamental records")
        
        return price_data, fundamentals_data
    
    def engineer_features(self, price_data: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for anomaly detection"""
        print("Engineering features for anomaly detection...")
        
        features_list = []
        
        for ticker in price_data['ticker'].unique():
            ticker_data = price_data[price_data['ticker'] == ticker].copy()
            ticker_data = ticker_data.sort_values('date')
            
            if len(ticker_data) < 30:  # Skip tickers with insufficient data
                continue
            
            # Calculate technical indicators
            ticker_data['returns'] = ticker_data['adjusted_close'].pct_change()
            ticker_data['log_returns'] = np.log(ticker_data['adjusted_close'] / ticker_data['adjusted_close'].shift(1))
            
            # Price-based features
            ticker_data['price_volatility_5d'] = ticker_data['returns'].rolling(5).std()
            ticker_data['price_volatility_20d'] = ticker_data['returns'].rolling(20).std()
            ticker_data['price_momentum_5d'] = ticker_data['adjusted_close'].pct_change(5)
            ticker_data['price_momentum_20d'] = ticker_data['adjusted_close'].pct_change(20)
            
            # Volume-based features
            ticker_data['volume_ma_5d'] = ticker_data['volume'].rolling(5).mean()
            ticker_data['volume_ma_20d'] = ticker_data['volume'].rolling(20).mean()
            ticker_data['volume_ratio'] = ticker_data['volume'] / ticker_data['volume_ma_20d']
            ticker_data['volume_volatility'] = ticker_data['volume'].rolling(10).std()
            
            # Price-volume relationship
            ticker_data['pv_correlation'] = ticker_data['returns'].rolling(10).corr(ticker_data['volume'])
            
            # High-low spread
            ticker_data['hl_spread'] = (ticker_data['high_price'] - ticker_data['low_price']) / ticker_data['adjusted_close']
            ticker_data['hl_spread_ma'] = ticker_data['hl_spread'].rolling(10).mean()
            
            # Relative strength
            ticker_data['rsi'] = self._calculate_rsi(ticker_data['adjusted_close'])
            
            # Recent data only (last 6 months)
            recent_data = ticker_data.tail(120)  # ~6 months of trading days
            
            if len(recent_data) < 30:
                continue
            
            # Aggregate features per ticker
            features = {
                'ticker': ticker,
                'exchange': recent_data['exchange'].iloc[0],
                
                # Price features
                'avg_price_volatility_5d': recent_data['price_volatility_5d'].mean(),
                'avg_price_volatility_20d': recent_data['price_volatility_20d'].mean(),
                'max_price_volatility_5d': recent_data['price_volatility_5d'].max(),
                'avg_price_momentum_5d': recent_data['price_momentum_5d'].mean(),
                'avg_price_momentum_20d': recent_data['price_momentum_20d'].mean(),
                
                # Volume features
                'avg_volume_ratio': recent_data['volume_ratio'].mean(),
                'max_volume_ratio': recent_data['volume_ratio'].max(),
                'avg_volume_volatility': recent_data['volume_volatility'].mean(),
                'volume_trend': recent_data['volume'].tail(10).mean() / recent_data['volume'].head(10).mean(),
                
                # Price-volume relationship
                'avg_pv_correlation': recent_data['pv_correlation'].mean(),
                'min_pv_correlation': recent_data['pv_correlation'].min(),
                
                # Spread features
                'avg_hl_spread': recent_data['hl_spread'].mean(),
                'max_hl_spread': recent_data['hl_spread'].max(),
                'avg_hl_spread_ratio': recent_data['hl_spread'].mean() / recent_data['hl_spread_ma'].mean(),
                
                # RSI features
                'avg_rsi': recent_data['rsi'].mean(),
                'min_rsi': recent_data['rsi'].min(),
                'max_rsi': recent_data['rsi'].max(),
                
                # Return features
                'total_return_6m': (recent_data['adjusted_close'].iloc[-1] / recent_data['adjusted_close'].iloc[0]) - 1,
                'max_drawdown_6m': self._calculate_max_drawdown(recent_data['adjusted_close']),
                'sharpe_ratio_6m': recent_data['returns'].mean() / recent_data['returns'].std() if recent_data['returns'].std() > 0 else 0,
                
                # Data quality
                'data_points': len(recent_data),
                'missing_data_pct': recent_data.isnull().sum().sum() / (len(recent_data) * len(recent_data.columns))
            }
            
            features_list.append(features)
        
        features_df = pd.DataFrame(features_list)
        
        print(f"Engineered features for {len(features_df)} tickers")
        return features_df
    
    def _calculate_rsi(self, prices: pd.Series, window: int = 14) -> pd.Series:
        """Calculate RSI indicator"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _calculate_max_drawdown(self, prices: pd.Series) -> float:
        """Calculate maximum drawdown"""
        peak = prices.expanding().max()
        drawdown = (prices - peak) / peak
        return drawdown.min()
    
    def detect_anomalies(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """Detect anomalies using Isolation Forest"""
        print("Detecting anomalies using Isolation Forest...")
        
        # Select numerical features for anomaly detection
        feature_columns = [
            'avg_price_volatility_5d', 'avg_price_volatility_20d', 'max_price_volatility_5d',
            'avg_price_momentum_5d', 'avg_price_momentum_20d',
            'avg_volume_ratio', 'max_volume_ratio', 'avg_volume_volatility', 'volume_trend',
            'avg_pv_correlation', 'min_pv_correlation',
            'avg_hl_spread', 'max_hl_spread', 'avg_hl_spread_ratio',
            'avg_rsi', 'min_rsi', 'max_rsi',
            'total_return_6m', 'max_drawdown_6m', 'sharpe_ratio_6m'
        ]
        
        # Prepare data
        X = features_df[feature_columns].copy()
        
        # Handle missing values and infinite values
        X = X.fillna(X.median())
        
        # Replace infinite values with finite values
        X = X.replace([np.inf, -np.inf], np.nan)
        X = X.fillna(X.median())
        
        # Ensure all values are finite
        X = X.clip(-1e10, 1e10)  # Clip extreme values
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Apply PCA for dimensionality reduction
        X_pca = self.pca.fit_transform(X_scaled)
        
        print(f"Original features: {X_scaled.shape[1]}")
        print(f"PCA components: {X_pca.shape[1]}")
        print(f"Explained variance: {self.pca.explained_variance_ratio_.sum():.3f}")
        
        # Fit Isolation Forest
        self.isolation_forest.fit(X_pca)
        
        # Predict anomalies
        anomaly_scores = self.isolation_forest.decision_function(X_pca)
        anomaly_predictions = self.isolation_forest.predict(X_pca)
        
        # Add results to features dataframe
        features_df['anomaly_score'] = anomaly_scores
        features_df['is_anomaly'] = anomaly_predictions == -1
        features_df['anomaly_probability'] = 1 - (anomaly_scores - anomaly_scores.min()) / (anomaly_scores.max() - anomaly_scores.min())
        
        # Sort by anomaly score (most anomalous first)
        features_df = features_df.sort_values('anomaly_score')
        
        print(f"Detected {features_df['is_anomaly'].sum()} anomalies out of {len(features_df)} tickers")
        
        return features_df
    
    def analyze_anomalies(self, features_df: pd.DataFrame) -> None:
        """Analyze and report detected anomalies"""
        print("\n" + "="*80)
        print("ANOMALY DETECTION RESULTS")
        print("="*80)
        
        anomalies = features_df[features_df['is_anomaly'] == True]
        
        print(f"Total Anomalies Detected: {len(anomalies)}")
        print(f"Anomaly Rate: {len(anomalies)/len(features_df)*100:.1f}%")
        
        if len(anomalies) > 0:
            print(f"\nTOP 20 MOST ANOMALOUS STOCKS:")
            print("-" * 80)
            print(f"{'Rank':<5} {'Ticker':<10} {'Exchange':<8} {'Score':<8} {'Prob':<6} {'Return 6M':<10} {'Volatility':<10}")
            print("-" * 80)
            
            for i, (_, row) in enumerate(anomalies.head(20).iterrows()):
                print(f"{i+1:<5} {row['ticker']:<10} {row['exchange']:<8} {row['anomaly_score']:<8.3f} {row['anomaly_probability']:<6.1%} {row['total_return_6m']:<10.1%} {row['avg_price_volatility_20d']:<10.3f}")
            
            # Analyze anomaly characteristics
            print(f"\nANOMALY CHARACTERISTICS:")
            print("-" * 40)
            
            print(f"Average 6-month return: {anomalies['total_return_6m'].mean():.1%}")
            print(f"Average volatility (20d): {anomalies['avg_price_volatility_20d'].mean():.3f}")
            print(f"Average volume ratio: {anomalies['avg_volume_ratio'].mean():.2f}")
            print(f"Average RSI: {anomalies['avg_rsi'].mean():.1f}")
            print(f"Average max drawdown: {anomalies['max_drawdown_6m'].mean():.1%}")
            
            # Exchange breakdown
            print(f"\nANOMALIES BY EXCHANGE:")
            print("-" * 30)
            exchange_counts = anomalies['exchange'].value_counts()
            for exchange, count in exchange_counts.items():
                print(f"{exchange}: {count} ({count/len(anomalies)*100:.1f}%)")
            
            # Sector analysis (if available)
            if 'sector' in anomalies.columns:
                print(f"\nANOMALIES BY SECTOR:")
                print("-" * 30)
                sector_counts = anomalies['sector'].value_counts()
                for sector, count in sector_counts.head(10).items():
                    print(f"{sector}: {count}")
    
    def create_visualizations(self, features_df: pd.DataFrame) -> None:
        """Create visualizations of anomaly detection results"""
        print("\nCreating visualizations...")
        
        # Set up the plotting style
        plt.style.use('default')
        sns.set_palette("husl")
        
        # Create figure with subplots
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        fig.suptitle('Market Anomaly Detection Results', fontsize=16, fontweight='bold')
        
        # 1. Anomaly Score Distribution
        axes[0, 0].hist(features_df['anomaly_score'], bins=50, alpha=0.7, color='skyblue', edgecolor='black')
        axes[0, 0].axvline(features_df[features_df['is_anomaly']]['anomaly_score'].min(), 
                          color='red', linestyle='--', label='Anomaly Threshold')
        axes[0, 0].set_title('Anomaly Score Distribution')
        axes[0, 0].set_xlabel('Anomaly Score')
        axes[0, 0].set_ylabel('Frequency')
        axes[0, 0].legend()
        
        # 2. Return vs Volatility
        normal_stocks = features_df[features_df['is_anomaly'] == False]
        anomaly_stocks = features_df[features_df['is_anomaly'] == True]
        
        axes[0, 1].scatter(normal_stocks['avg_price_volatility_20d'], normal_stocks['total_return_6m'], 
                          alpha=0.6, label='Normal', s=20)
        axes[0, 1].scatter(anomaly_stocks['avg_price_volatility_20d'], anomaly_stocks['total_return_6m'], 
                          alpha=0.8, label='Anomaly', s=30, color='red')
        axes[0, 1].set_title('Return vs Volatility')
        axes[0, 1].set_xlabel('Average Volatility (20d)')
        axes[0, 1].set_ylabel('6-Month Return')
        axes[0, 1].legend()
        
        # 3. Volume Ratio Distribution
        axes[0, 2].hist(normal_stocks['avg_volume_ratio'], bins=30, alpha=0.7, label='Normal', color='blue')
        axes[0, 2].hist(anomaly_stocks['avg_volume_ratio'], bins=30, alpha=0.7, label='Anomaly', color='red')
        axes[0, 2].set_title('Volume Ratio Distribution')
        axes[0, 2].set_xlabel('Average Volume Ratio')
        axes[0, 2].set_ylabel('Frequency')
        axes[0, 2].legend()
        
        # 4. RSI Distribution
        axes[1, 0].hist(normal_stocks['avg_rsi'], bins=30, alpha=0.7, label='Normal', color='green')
        axes[1, 0].hist(anomaly_stocks['avg_rsi'], bins=30, alpha=0.7, label='Anomaly', color='red')
        axes[1, 0].set_title('RSI Distribution')
        axes[1, 0].set_xlabel('Average RSI')
        axes[1, 0].set_ylabel('Frequency')
        axes[1, 0].legend()
        
        # 5. Exchange Breakdown
        exchange_counts = features_df['exchange'].value_counts()
        anomaly_counts = features_df[features_df['is_anomaly']]['exchange'].value_counts()
        
        x_pos = range(len(exchange_counts))
        axes[1, 1].bar(x_pos, exchange_counts.values, alpha=0.7, label='Total', color='lightblue')
        axes[1, 1].bar(x_pos, [anomaly_counts.get(ex, 0) for ex in exchange_counts.index], 
                      alpha=0.9, label='Anomalies', color='red')
        axes[1, 1].set_title('Anomalies by Exchange')
        axes[1, 1].set_xlabel('Exchange')
        axes[1, 1].set_ylabel('Count')
        axes[1, 1].set_xticks(x_pos)
        axes[1, 1].set_xticklabels(exchange_counts.index, rotation=45)
        axes[1, 1].legend()
        
        # 6. Top Anomalies Timeline (if we have date data)
        top_anomalies = features_df.head(10)
        axes[1, 2].barh(range(len(top_anomalies)), top_anomalies['anomaly_score'], color='red', alpha=0.7)
        axes[1, 2].set_yticks(range(len(top_anomalies)))
        axes[1, 2].set_yticklabels(top_anomalies['ticker'])
        axes[1, 2].set_title('Top 10 Most Anomalous Stocks')
        axes[1, 2].set_xlabel('Anomaly Score')
        
        plt.tight_layout()
        
        # Save the plot
        output_path = 'anomaly_detection_results.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Visualization saved to: {output_path}")
        
        plt.show()
    
    def save_results(self, features_df: pd.DataFrame) -> None:
        """Save anomaly detection results to files"""
        print("\nSaving results...")
        
        # Save full results
        features_df.to_csv('anomaly_detection_full_results.csv', index=False)
        
        # Save only anomalies
        anomalies = features_df[features_df['is_anomaly'] == True]
        anomalies.to_csv('detected_anomalies.csv', index=False)
        
        # Save summary report
        with open('anomaly_detection_report.txt', 'w') as f:
            f.write("MARKET ANOMALY DETECTION REPORT\n")
            f.write("=" * 50 + "\n")
            f.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Stocks Analyzed: {len(features_df)}\n")
            f.write(f"Anomalies Detected: {len(anomalies)}\n")
            f.write(f"Anomaly Rate: {len(anomalies)/len(features_df)*100:.1f}%\n\n")
            
            f.write("TOP 20 MOST ANOMALOUS STOCKS:\n")
            f.write("-" * 50 + "\n")
            for i, (_, row) in enumerate(anomalies.head(20).iterrows()):
                f.write(f"{i+1:2d}. {row['ticker']:<10} {row['exchange']:<8} Score: {row['anomaly_score']:.3f} "
                       f"Return: {row['total_return_6m']:6.1%} Vol: {row['avg_price_volatility_20d']:.3f}\n")
        
        print(f"Results saved to current folder")
    
    def run_anomaly_detection(self):
        """Run complete anomaly detection pipeline"""
        print("MARKET ANOMALY DETECTION SYSTEM")
        print("=" * 50)
        print("Using Isolation Forest to detect unusual market behavior")
        print("=" * 50)
        
        try:
            # Load and prepare data
            price_data, fundamentals_data = self.load_and_prepare_data()
            
            # Engineer features
            features_df = self.engineer_features(price_data)
            
            if len(features_df) == 0:
                print("No data available for anomaly detection")
                return
            
            # Detect anomalies
            features_df = self.detect_anomalies(features_df)
            
            # Analyze results
            self.analyze_anomalies(features_df)
            
            # Create visualizations
            self.create_visualizations(features_df)
            
            # Save results
            self.save_results(features_df)
            
            print("\n" + "="*50)
            print("ANOMALY DETECTION COMPLETED SUCCESSFULLY!")
            print("="*50)
            
        except Exception as e:
            print(f"Error during anomaly detection: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    detector = MarketAnomalyDetector()
    detector.run_anomaly_detection()

if __name__ == "__main__":
    main()
