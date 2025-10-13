#!/usr/bin/env python3
"""
DATABASE SCHEMA EXPORT TO EXCEL
===============================

Export comprehensive database schema and quality metrics to Excel file:
- Table schemas with column details
- Data quality metrics
- Sample data previews
- Summary statistics

Author: AI Assistant
Date: 2025-10-13
"""

import sqlite3
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import os
from datetime import datetime

class DatabaseSchemaExporter:
    """Export database schema and quality metrics to Excel"""
    
    def __init__(self, db_path: str = "data_collection/unified_stock_data.db"):
        self.db_path = db_path
        self.output_file = f"database_schema_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
    def get_detailed_schema(self, conn: sqlite3.Connection) -> Dict[str, pd.DataFrame]:
        """Get detailed schema information for all tables"""
        cursor = conn.cursor()
        schema_info = {}
        
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = [row[0] for row in cursor.fetchall()]
        
        for table in table_names:
            # Get column information
            cursor.execute(f"PRAGMA table_info({table})")
            columns_info = cursor.fetchall()
            
            # Get table statistics
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            
            # Create schema DataFrame
            schema_df = pd.DataFrame(columns_info, columns=[
                'column_id', 'column_name', 'data_type', 'not_null', 'default_value', 'primary_key'
            ])
            
            # Add additional information
            schema_df['table_name'] = table
            schema_df['total_rows'] = row_count
            schema_df['not_null'] = schema_df['not_null'].astype(bool)
            schema_df['primary_key'] = schema_df['primary_key'].astype(bool)
            
            schema_info[table] = schema_df
        
        return schema_info
    
    def get_data_quality_metrics(self, conn: sqlite3.Connection, schema_info: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Get data quality metrics for all columns"""
        cursor = conn.cursor()
        quality_metrics = {}
        
        for table, schema_df in schema_info.items():
            if schema_df.empty:
                continue
                
            quality_data = []
            
            for _, row in schema_df.iterrows():
                column_name = row['column_name']
                total_rows = row['total_rows']
                
                if total_rows == 0:
                    quality_data.append({
                        'table_name': table,
                        'column_name': column_name,
                        'total_rows': 0,
                        'nan_count': 0,
                        'nan_percentage': 0,
                        'non_null_count': 0,
                        'unique_count': 0,
                        'non_unique_percentage': 0,
                        'most_common_value': None,
                        'most_common_count': 0,
                        'data_type_detected': 'empty_table'
                    })
                    continue
                
                try:
                    # Get column data
                    cursor.execute(f"SELECT {column_name} FROM {table}")
                    data = cursor.fetchall()
                    values = [row[0] for row in data]
                    
                    # Convert to pandas Series
                    series = pd.Series(values)
                    
                    # Calculate metrics
                    nan_count = series.isna().sum()
                    nan_percentage = (nan_count / total_rows) * 100
                    
                    non_null_values = series.dropna()
                    non_null_count = len(non_null_values)
                    
                    if non_null_count > 0:
                        unique_count = non_null_values.nunique()
                        non_unique_percentage = ((non_null_count - unique_count) / non_null_count) * 100
                        
                        # Most common value
                        value_counts = non_null_values.value_counts()
                        most_common_value = value_counts.index[0] if len(value_counts) > 0 else None
                        most_common_count = value_counts.iloc[0] if len(value_counts) > 0 else 0
                        
                        data_type_detected = str(series.dtype)
                    else:
                        unique_count = 0
                        non_unique_percentage = 0
                        most_common_value = None
                        most_common_count = 0
                        data_type_detected = 'all_null'
                    
                    quality_data.append({
                        'table_name': table,
                        'column_name': column_name,
                        'total_rows': total_rows,
                        'nan_count': nan_count,
                        'nan_percentage': round(nan_percentage, 2),
                        'non_null_count': non_null_count,
                        'unique_count': unique_count,
                        'non_unique_percentage': round(non_unique_percentage, 2),
                        'most_common_value': str(most_common_value) if most_common_value is not None else None,
                        'most_common_count': most_common_count,
                        'data_type_detected': data_type_detected
                    })
                    
                except Exception as e:
                    quality_data.append({
                        'table_name': table,
                        'column_name': column_name,
                        'total_rows': total_rows,
                        'nan_count': 0,
                        'nan_percentage': 0,
                        'non_null_count': 0,
                        'unique_count': 0,
                        'non_unique_percentage': 0,
                        'most_common_value': f"Error: {str(e)}",
                        'most_common_count': 0,
                        'data_type_detected': 'error'
                    })
            
            quality_metrics[table] = pd.DataFrame(quality_data)
        
        return quality_metrics
    
    def get_sample_data(self, conn: sqlite3.Connection, schema_info: Dict[str, pd.DataFrame], sample_size: int = 5) -> Dict[str, pd.DataFrame]:
        """Get sample data from each table"""
        cursor = conn.cursor()
        sample_data = {}
        
        for table, schema_df in schema_info.items():
            if schema_df.empty or schema_df['total_rows'].iloc[0] == 0:
                sample_data[table] = pd.DataFrame()
                continue
            
            try:
                cursor.execute(f"SELECT * FROM {table} LIMIT {sample_size}")
                columns = [description[0] for description in cursor.description]
                data = cursor.fetchall()
                
                if data:
                    sample_df = pd.DataFrame(data, columns=columns)
                    sample_data[table] = sample_df
                else:
                    sample_data[table] = pd.DataFrame()
                    
            except Exception as e:
                sample_data[table] = pd.DataFrame({'error': [str(e)]})
        
        return sample_data
    
    def get_table_summary(self, schema_info: Dict[str, pd.DataFrame], quality_metrics: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Get summary statistics for all tables"""
        summary_data = []
        
        for table, schema_df in schema_info.items():
            if schema_df.empty:
                continue
            
            total_rows = schema_df['total_rows'].iloc[0]
            total_columns = len(schema_df)
            
            # Get quality metrics for this table
            if table in quality_metrics:
                quality_df = quality_metrics[table]
                
                avg_nan_pct = quality_df['nan_percentage'].mean()
                avg_non_unique_pct = quality_df['non_unique_percentage'].mean()
                
                # Count columns by quality level
                high_quality_cols = len(quality_df[
                    (quality_df['nan_percentage'] == 0) & 
                    (quality_df['non_unique_percentage'] < 10)
                ])
                
                high_nan_cols = len(quality_df[quality_df['nan_percentage'] > 50])
                high_non_unique_cols = len(quality_df[quality_df['non_unique_percentage'] > 90])
                
            else:
                avg_nan_pct = 0
                avg_non_unique_pct = 0
                high_quality_cols = 0
                high_nan_cols = 0
                high_non_unique_cols = 0
            
            summary_data.append({
                'table_name': table,
                'total_rows': total_rows,
                'total_columns': total_columns,
                'avg_nan_percentage': round(avg_nan_pct, 2),
                'avg_non_unique_percentage': round(avg_non_unique_pct, 2),
                'high_quality_columns': high_quality_cols,
                'high_nan_columns': high_nan_cols,
                'high_non_unique_columns': high_non_unique_cols,
                'quality_rating': self._get_quality_rating(avg_nan_pct, avg_non_unique_pct)
            })
        
        return pd.DataFrame(summary_data)
    
    def _get_quality_rating(self, avg_nan_pct: float, avg_non_unique_pct: float) -> str:
        """Get quality rating based on metrics"""
        if avg_nan_pct == 0 and avg_non_unique_pct < 20:
            return "Excellent"
        elif avg_nan_pct < 5 and avg_non_unique_pct < 50:
            return "Good"
        elif avg_nan_pct < 15 and avg_non_unique_pct < 70:
            return "Moderate"
        else:
            return "Poor"
    
    def export_to_excel(self):
        """Export all data to Excel file"""
        if not os.path.exists(self.db_path):
            print(f"Database not found: {self.db_path}")
            return
        
        print("DATABASE SCHEMA EXPORT TO EXCEL")
        print("=" * 50)
        print(f"Database: {self.db_path}")
        print(f"Output file: {self.output_file}")
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get all data
            print("\n1. Getting database schema...")
            schema_info = self.get_detailed_schema(conn)
            
            print("2. Calculating data quality metrics...")
            quality_metrics = self.get_data_quality_metrics(conn, schema_info)
            
            print("3. Getting sample data...")
            sample_data = self.get_sample_data(conn, schema_info)
            
            print("4. Creating summary statistics...")
            table_summary = self.get_table_summary(schema_info, quality_metrics)
            
            # Create Excel file
            print("5. Writing to Excel file...")
            with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
                
                # Summary sheet
                table_summary.to_excel(writer, sheet_name='Table_Summary', index=False)
                
                # Schema sheets for each table
                for table, schema_df in schema_info.items():
                    if not schema_df.empty:
                        schema_df.to_excel(writer, sheet_name=f'Schema_{table}', index=False)
                
                # Quality metrics sheets for each table
                for table, quality_df in quality_metrics.items():
                    if not quality_df.empty:
                        quality_df.to_excel(writer, sheet_name=f'Quality_{table}', index=False)
                
                # Sample data sheets for each table
                for table, sample_df in sample_data.items():
                    if not sample_df.empty:
                        sample_df.to_excel(writer, sheet_name=f'Sample_{table}', index=False)
                
                # Combined schema sheet
                all_schema = pd.concat(schema_info.values(), ignore_index=True)
                all_schema.to_excel(writer, sheet_name='All_Schemas', index=False)
                
                # Combined quality metrics sheet
                all_quality = pd.concat(quality_metrics.values(), ignore_index=True)
                all_quality.to_excel(writer, sheet_name='All_Quality_Metrics', index=False)
            
            conn.close()
            
            print(f"\nExport completed successfully!")
            print(f"File saved as: {self.output_file}")
            print(f"Total sheets created: {len(schema_info) * 3 + 3}")  # Schema + Quality + Sample + Summary + Combined sheets
            
            # Print summary
            print(f"\nEXPORT SUMMARY:")
            print(f"   Tables exported: {len(schema_info)}")
            print(f"   Total columns: {len(all_schema)}")
            print(f"   Total rows across all tables: {table_summary['total_rows'].sum():,}")
            
        except Exception as e:
            print(f"Error during export: {e}")

def main():
    """Main function"""
    exporter = DatabaseSchemaExporter()
    exporter.export_to_excel()

if __name__ == "__main__":
    main()
