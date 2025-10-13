#!/usr/bin/env python3
"""
OPTIMIZED DATABASE SCHEMA EXPORT TO EXCEL
==========================================

Optimized export with minimal repetition and consolidated information:
- Single comprehensive schema sheet
- Consolidated quality metrics
- Smart sample data selection
- Efficient data structure

Author: AI Assistant
Date: 2025-10-13
"""

import sqlite3
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import os
from datetime import datetime

class OptimizedSchemaExporter:
    """Optimized database schema exporter with minimal repetition"""
    
    def __init__(self, db_path: str = "data_collection/unified_stock_data.db"):
        self.db_path = db_path
        self.output_file = f"optimized_schema_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
    def get_consolidated_schema(self, conn: sqlite3.Connection) -> pd.DataFrame:
        """Get consolidated schema with quality metrics in single DataFrame"""
        cursor = conn.cursor()
        
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = [row[0] for row in cursor.fetchall()]
        
        consolidated_data = []
        
        for table in table_names:
            # Get column information
            cursor.execute(f"PRAGMA table_info({table})")
            columns_info = cursor.fetchall()
            
            # Get table statistics
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            
            for col_info in columns_info:
                column_id, column_name, data_type, not_null, default_value, primary_key = col_info
                
                # Get quality metrics for this column
                quality_metrics = self._get_column_quality_metrics(cursor, table, column_name, row_count)
                
                # Create consolidated record
                record = {
                    'table_name': table,
                    'column_name': column_name,
                    'column_id': column_id,
                    'data_type': data_type,
                    'not_null': bool(not_null),
                    'default_value': default_value,
                    'primary_key': bool(primary_key),
                    'total_rows': row_count,
                    **quality_metrics
                }
                consolidated_data.append(record)
        
        return pd.DataFrame(consolidated_data)
    
    def _get_column_quality_metrics(self, cursor: sqlite3.Cursor, table: str, column: str, total_rows: int) -> Dict:
        """Get quality metrics for a single column"""
        if total_rows == 0:
            return {
                'nan_count': 0,
                'nan_percentage': 0,
                'non_null_count': 0,
                'unique_count': 0,
                'non_unique_percentage': 0,
                'most_common_value': None,
                'most_common_count': 0,
                'data_type_detected': 'empty_table'
            }
        
        try:
            # Get column data
            cursor.execute(f"SELECT {column} FROM {table}")
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
                most_common_value = str(value_counts.index[0]) if len(value_counts) > 0 else None
                most_common_count = value_counts.iloc[0] if len(value_counts) > 0 else 0
                
                data_type_detected = str(series.dtype)
            else:
                unique_count = 0
                non_unique_percentage = 0
                most_common_value = None
                most_common_count = 0
                data_type_detected = 'all_null'
            
            return {
                'nan_count': nan_count,
                'nan_percentage': round(nan_percentage, 2),
                'non_null_count': non_null_count,
                'unique_count': unique_count,
                'non_unique_percentage': round(non_unique_percentage, 2),
                'most_common_value': most_common_value,
                'most_common_count': most_common_count,
                'data_type_detected': data_type_detected
            }
            
        except Exception as e:
            return {
                'nan_count': 0,
                'nan_percentage': 0,
                'non_null_count': 0,
                'unique_count': 0,
                'non_unique_percentage': 0,
                'most_common_value': f"Error: {str(e)}",
                'most_common_count': 0,
                'data_type_detected': 'error'
            }
    
    def get_table_summary(self, consolidated_df: pd.DataFrame) -> pd.DataFrame:
        """Get table-level summary from consolidated data"""
        summary_data = []
        
        for table in consolidated_df['table_name'].unique():
            table_data = consolidated_df[consolidated_df['table_name'] == table]
            
            total_rows = table_data['total_rows'].iloc[0]
            total_columns = len(table_data)
            
            # Calculate quality metrics
            avg_nan_pct = table_data['nan_percentage'].mean()
            avg_non_unique_pct = table_data['non_unique_percentage'].mean()
            
            # Count columns by quality level
            high_quality_cols = len(table_data[
                (table_data['nan_percentage'] == 0) & 
                (table_data['non_unique_percentage'] < 10)
            ])
            
            high_nan_cols = len(table_data[table_data['nan_percentage'] > 50])
            high_non_unique_cols = len(table_data[table_data['non_unique_percentage'] > 90])
            
            # Calculate completeness
            complete_rows = total_rows
            for _, row in table_data.iterrows():
                complete_rows = min(complete_rows, total_rows - row['nan_count'])
            
            completeness_pct = (complete_rows / total_rows) * 100 if total_rows > 0 else 0
            
            summary_data.append({
                'table_name': table,
                'total_rows': total_rows,
                'total_columns': total_columns,
                'avg_nan_percentage': round(avg_nan_pct, 2),
                'avg_non_unique_percentage': round(avg_non_unique_pct, 2),
                'high_quality_columns': high_quality_cols,
                'high_nan_columns': high_nan_cols,
                'high_non_unique_columns': high_non_unique_cols,
                'completeness_percentage': round(completeness_pct, 2),
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
    
    def get_smart_sample_data(self, conn: sqlite3.Connection, consolidated_df: pd.DataFrame) -> pd.DataFrame:
        """Get smart sample data - only from tables with data and key columns"""
        cursor = conn.cursor()
        sample_data = []
        
        # Only sample from tables with data
        tables_with_data = consolidated_df[consolidated_df['total_rows'] > 0]['table_name'].unique()
        
        for table in tables_with_data[:5]:  # Limit to 5 most important tables
            table_data = consolidated_df[consolidated_df['table_name'] == table]
            
            # Get key columns (primary keys, important data columns)
            key_columns = table_data[
                (table_data['primary_key'] == True) | 
                (table_data['nan_percentage'] < 10) |
                (table_data['column_name'].str.contains('ticker|date|price|value', case=False, na=False))
            ]['column_name'].tolist()
            
            if not key_columns:
                key_columns = table_data['column_name'].head(3).tolist()
            
            try:
                columns_str = ', '.join(key_columns)
                cursor.execute(f"SELECT {columns_str} FROM {table} LIMIT 3")
                data = cursor.fetchall()
                
                for i, row in enumerate(data):
                    for j, col in enumerate(key_columns):
                        sample_data.append({
                            'table_name': table,
                            'sample_row': i + 1,
                            'column_name': col,
                            'sample_value': str(row[j]) if row[j] is not None else 'NULL'
                        })
                        
            except Exception as e:
                sample_data.append({
                    'table_name': table,
                    'sample_row': 0,
                    'column_name': 'error',
                    'sample_value': str(e)
                })
        
        return pd.DataFrame(sample_data)
    
    def get_quality_insights(self, consolidated_df: pd.DataFrame) -> pd.DataFrame:
        """Get quality insights and recommendations"""
        insights = []
        
        # High NaN columns
        high_nan = consolidated_df[consolidated_df['nan_percentage'] > 50].copy()
        if not high_nan.empty:
            insights.append({
                'category': 'High NaN Columns (>50%)',
                'count': len(high_nan),
                'description': f"{len(high_nan)} columns have >50% missing values",
                'recommendation': 'Review data collection process or consider data imputation',
                'examples': ', '.join((high_nan['table_name'] + '.' + high_nan['column_name']).head(3).astype(str))
            })
        
        # High non-unique columns
        high_non_unique = consolidated_df[consolidated_df['non_unique_percentage'] > 90].copy()
        if not high_non_unique.empty:
            insights.append({
                'category': 'High Non-Unique Columns (>90%)',
                'count': len(high_non_unique),
                'description': f"{len(high_non_unique)} columns have >90% duplicate values",
                'recommendation': 'Expected for categorical data (exchanges, flags) or consider normalization',
                'examples': ', '.join((high_non_unique['table_name'] + '.' + high_non_unique['column_name']).head(3).astype(str))
            })
        
        # Perfect columns
        perfect = consolidated_df[
            (consolidated_df['nan_percentage'] == 0) & 
            (consolidated_df['non_unique_percentage'] < 10)
        ].copy()
        if not perfect.empty:
            insights.append({
                'category': 'High Quality Columns (0% NaN, <10% Non-Unique)',
                'count': len(perfect),
                'description': f"{len(perfect)} columns have excellent data quality",
                'recommendation': 'These columns are ready for analysis',
                'examples': ', '.join((perfect['table_name'] + '.' + perfect['column_name']).head(3).astype(str))
            })
        
        # Table completeness
        table_summary = consolidated_df.groupby('table_name').agg({
            'total_rows': 'first',
            'nan_percentage': 'mean',
            'nan_count': 'sum'
        }).reset_index()
        
        table_summary['completeness_percentage'] = ((table_summary['total_rows'] - table_summary['nan_count']) / table_summary['total_rows'] * 100).fillna(0)
        
        incomplete_tables = table_summary[table_summary['completeness_percentage'] < 50]
        if not incomplete_tables.empty:
            insights.append({
                'category': 'Incomplete Tables (<50% Complete)',
                'count': len(incomplete_tables),
                'description': f"{len(incomplete_tables)} tables have <50% complete rows",
                'recommendation': 'Focus on improving data collection for these tables',
                'examples': ', '.join(incomplete_tables['table_name'].head(3).astype(str))
            })
        
        return pd.DataFrame(insights)
    
    def export_to_excel(self):
        """Export optimized data to Excel file"""
        if not os.path.exists(self.db_path):
            print(f"Database not found: {self.db_path}")
            return
        
        print("OPTIMIZED DATABASE SCHEMA EXPORT")
        print("=" * 50)
        print(f"Database: {self.db_path}")
        print(f"Output file: {self.output_file}")
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            print("\n1. Getting consolidated schema with quality metrics...")
            consolidated_df = self.get_consolidated_schema(conn)
            
            print("2. Creating table summary...")
            table_summary = self.get_table_summary(consolidated_df)
            
            print("3. Getting smart sample data...")
            sample_data = self.get_smart_sample_data(conn, consolidated_df)
            
            print("4. Generating quality insights...")
            quality_insights = self.get_quality_insights(consolidated_df)
            
            # Create Excel file with minimal sheets
            print("5. Writing to Excel file...")
            with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
                
                # Main consolidated sheet
                consolidated_df.to_excel(writer, sheet_name='Complete_Schema', index=False)
                
                # Table summary
                table_summary.to_excel(writer, sheet_name='Table_Summary', index=False)
                
                # Smart sample data
                sample_data.to_excel(writer, sheet_name='Sample_Data', index=False)
                
                # Quality insights
                quality_insights.to_excel(writer, sheet_name='Quality_Insights', index=False)
                
                # High-level overview
                overview_data = {
                    'Metric': [
                        'Total Tables',
                        'Total Columns', 
                        'Total Rows',
                        'High Quality Columns',
                        'High NaN Columns',
                        'High Non-Unique Columns',
                        'Average Table Completeness',
                        'Database Size (MB)'
                    ],
                    'Value': [
                        len(table_summary),
                        len(consolidated_df),
                        table_summary['total_rows'].sum(),
                        len(consolidated_df[(consolidated_df['nan_percentage'] == 0) & (consolidated_df['non_unique_percentage'] < 10)]),
                        len(consolidated_df[consolidated_df['nan_percentage'] > 50]),
                        len(consolidated_df[consolidated_df['non_unique_percentage'] > 90]),
                        f"{table_summary['completeness_percentage'].mean():.1f}%",
                        f"{os.path.getsize(self.db_path) / (1024*1024):.1f}"
                    ]
                }
                pd.DataFrame(overview_data).to_excel(writer, sheet_name='Overview', index=False)
            
            conn.close()
            
            print(f"\nExport completed successfully!")
            print(f"File saved as: {self.output_file}")
            print(f"Total sheets: 5 (vs 42 in original)")
            print(f"Data reduction: ~85% fewer sheets")
            
            # Print summary
            print(f"\nOPTIMIZED EXPORT SUMMARY:")
            print(f"   Tables: {len(table_summary)}")
            print(f"   Columns: {len(consolidated_df)}")
            print(f"   Total rows: {table_summary['total_rows'].sum():,}")
            print(f"   Sheets: 5 (consolidated)")
            print(f"   File size: ~{os.path.getsize(self.output_file) / 1024:.0f} KB")
            
        except Exception as e:
            print(f"Error during export: {e}")

def main():
    """Main function"""
    exporter = OptimizedSchemaExporter()
    exporter.export_to_excel()

if __name__ == "__main__":
    main()
