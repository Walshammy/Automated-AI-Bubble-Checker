#!/usr/bin/env python3
"""
DATA QUALITY ANALYSIS
====================

Comprehensive analysis of data quality across all database tables:
- NaN percentage by column
- Non-unique value percentage by column
- Data completeness metrics
- Column value distribution analysis

Author: AI Assistant
Date: 2025-10-13
"""

import sqlite3
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import os

class DataQualityAnalyzer:
    """Analyze data quality across database tables"""
    
    def __init__(self, db_path: str = "data_collection/unified_stock_data.db"):
        self.db_path = db_path
        
    def get_table_schema(self, conn: sqlite3.Connection) -> Dict[str, List[str]]:
        """Get schema for all tables"""
        cursor = conn.cursor()
        tables = {}
        
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = [row[0] for row in cursor.fetchall()]
        
        for table in table_names:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cursor.fetchall()]
            tables[table] = columns
            
        return tables
    
    def analyze_table_quality(self, conn: sqlite3.Connection, table: str, columns: List[str]) -> Dict[str, Dict]:
        """Analyze data quality for a single table"""
        print(f"\nAnalyzing table: {table}")
        print("-" * 50)
        
        # Get total row count
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total_rows = cursor.fetchone()[0]
        
        if total_rows == 0:
            return {"error": "Empty table"}
        
        quality_metrics = {}
        
        for column in columns:
            print(f"  Analyzing column: {column}")
            
            # Get column data
            try:
                cursor.execute(f"SELECT {column} FROM {table}")
                data = cursor.fetchall()
                values = [row[0] for row in data]
                
                # Convert to pandas Series for easier analysis
                series = pd.Series(values)
                
                # Calculate NaN percentage
                nan_count = series.isna().sum()
                nan_percentage = (nan_count / total_rows) * 100
                
                # Calculate non-null values for uniqueness analysis
                non_null_values = series.dropna()
                
                if len(non_null_values) > 0:
                    # Calculate unique values percentage
                    unique_count = non_null_values.nunique()
                    non_unique_percentage = ((len(non_null_values) - unique_count) / len(non_null_values)) * 100
                    
                    # Get most common values
                    value_counts = non_null_values.value_counts()
                    most_common = value_counts.head(3).to_dict()
                    
                    # Data type analysis
                    data_type = str(series.dtype)
                    
                else:
                    unique_count = 0
                    non_unique_percentage = 0
                    most_common = {}
                    data_type = "all_null"
                
                quality_metrics[column] = {
                    'total_rows': total_rows,
                    'nan_count': nan_count,
                    'nan_percentage': round(nan_percentage, 2),
                    'non_null_count': len(non_null_values),
                    'unique_count': unique_count,
                    'non_unique_percentage': round(non_unique_percentage, 2),
                    'most_common_values': most_common,
                    'data_type': data_type
                }
                
            except Exception as e:
                quality_metrics[column] = {
                    'error': str(e),
                    'total_rows': total_rows
                }
        
        return quality_metrics
    
    def generate_quality_report(self, all_metrics: Dict[str, Dict]) -> None:
        """Generate comprehensive quality report"""
        print("\n" + "="*80)
        print("COMPREHENSIVE DATA QUALITY REPORT")
        print("="*80)
        
        # Summary statistics
        total_tables = len(all_metrics)
        total_columns = sum(len(table_metrics) for table_metrics in all_metrics.values())
        
        print(f"\nSUMMARY:")
        print(f"Total Tables Analyzed: {total_tables}")
        print(f"Total Columns Analyzed: {total_columns}")
        
        # High NaN percentage columns (>50%)
        print(f"\n{'HIGH NaN PERCENTAGE COLUMNS (>50%)':<60} {'NaN%':<10} {'Table':<20}")
        print("-" * 90)
        
        high_nan_columns = []
        for table, table_metrics in all_metrics.items():
            for column, metrics in table_metrics.items():
                if 'nan_percentage' in metrics and metrics['nan_percentage'] > 50:
                    high_nan_columns.append((table, column, metrics['nan_percentage']))
        
        high_nan_columns.sort(key=lambda x: x[2], reverse=True)
        
        for table, column, nan_pct in high_nan_columns[:20]:  # Top 20
            print(f"{column:<60} {nan_pct:<10.1f}% {table:<20}")
        
        # High non-unique percentage columns (>90%)
        print(f"\n{'HIGH NON-UNIQUE PERCENTAGE COLUMNS (>90%)':<60} {'Non-Unique%':<15} {'Table':<20}")
        print("-" * 95)
        
        high_non_unique_columns = []
        for table, table_metrics in all_metrics.items():
            for column, metrics in table_metrics.items():
                if 'non_unique_percentage' in metrics and metrics['non_unique_percentage'] > 90:
                    high_non_unique_columns.append((table, column, metrics['non_unique_percentage']))
        
        high_non_unique_columns.sort(key=lambda x: x[2], reverse=True)
        
        for table, column, non_unique_pct in high_non_unique_columns[:20]:  # Top 20
            print(f"{column:<60} {non_unique_pct:<15.1f}% {table:<20}")
        
        # Perfect columns (0% NaN, <10% non-unique)
        print(f"\n{'HIGH QUALITY COLUMNS (0% NaN, <10% Non-Unique)':<60} {'NaN%':<10} {'Non-Unique%':<15} {'Table':<20}")
        print("-" * 105)
        
        perfect_columns = []
        for table, table_metrics in all_metrics.items():
            for column, metrics in table_metrics.items():
                if ('nan_percentage' in metrics and 'non_unique_percentage' in metrics and
                    metrics['nan_percentage'] == 0 and metrics['non_unique_percentage'] < 10):
                    perfect_columns.append((table, column, metrics['nan_percentage'], metrics['non_unique_percentage']))
        
        perfect_columns.sort(key=lambda x: x[3])  # Sort by non-unique percentage
        
        for table, column, nan_pct, non_unique_pct in perfect_columns[:20]:  # Top 20
            print(f"{column:<60} {nan_pct:<10.1f}% {non_unique_pct:<15.1f}% {table:<20}")
        
        # Table-level summary
        print(f"\n{'TABLE-LEVEL QUALITY SUMMARY':<40} {'Avg NaN%':<12} {'Avg Non-Unique%':<18} {'Columns':<10}")
        print("-" * 80)
        
        for table, table_metrics in all_metrics.items():
            if not table_metrics or 'error' in str(table_metrics):
                continue
                
            nan_percentages = [m['nan_percentage'] for m in table_metrics.values() if 'nan_percentage' in m]
            non_unique_percentages = [m['non_unique_percentage'] for m in table_metrics.values() if 'non_unique_percentage' in m]
            
            if nan_percentages and non_unique_percentages:
                avg_nan = sum(nan_percentages) / len(nan_percentages)
                avg_non_unique = sum(non_unique_percentages) / len(non_unique_percentages)
                column_count = len(table_metrics)
                
                print(f"{table:<40} {avg_nan:<12.1f}% {avg_non_unique:<18.1f}% {column_count:<10}")
        
        # Data completeness by table
        print(f"\n{'DATA COMPLETENESS BY TABLE':<40} {'Total Rows':<15} {'Complete Rows':<15} {'Completeness%':<15}")
        print("-" * 85)
        
        for table, table_metrics in all_metrics.items():
            if not table_metrics or 'error' in str(table_metrics):
                continue
                
            # Get total rows from first column
            first_column = list(table_metrics.keys())[0]
            total_rows = table_metrics[first_column]['total_rows']
            
            # Calculate completeness (rows with no NaN values)
            complete_rows = total_rows
            for column, metrics in table_metrics.items():
                if 'nan_count' in metrics:
                    complete_rows = min(complete_rows, total_rows - metrics['nan_count'])
            
            completeness_pct = (complete_rows / total_rows) * 100 if total_rows > 0 else 0
            
            print(f"{table:<40} {total_rows:<15,} {complete_rows:<15,} {completeness_pct:<15.1f}%")
    
    def run_analysis(self):
        """Run comprehensive data quality analysis"""
        if not os.path.exists(self.db_path):
            print(f"Database not found: {self.db_path}")
            return
        
        print("DATA QUALITY ANALYSIS")
        print("=" * 50)
        print(f"Database: {self.db_path}")
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get table schema
            print("\nGetting table schema...")
            schema = self.get_table_schema(conn)
            
            print(f"Found {len(schema)} tables:")
            for table, columns in schema.items():
                print(f"  {table}: {len(columns)} columns")
            
            # Analyze each table
            all_metrics = {}
            for table, columns in schema.items():
                all_metrics[table] = self.analyze_table_quality(conn, table, columns)
            
            # Generate report
            self.generate_quality_report(all_metrics)
            
            conn.close()
            
        except Exception as e:
            print(f"Error during analysis: {e}")

def main():
    """Main function"""
    analyzer = DataQualityAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()
