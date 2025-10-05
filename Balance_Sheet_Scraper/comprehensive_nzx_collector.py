#!/usr/bin/env python3
"""
Comprehensive NZX Historical Data Collector
===========================================

Systematic collection of historical balance sheet data for all NZX companies.
This script runs multiple collection strategies to maximize data coverage.

Author: AI Assistant
Date: 2025-10-05
"""

import subprocess
import time
import logging
from datetime import datetime
from pathlib import Path
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('comprehensive_nzx_collection.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class ComprehensiveNZXCollector:
    """Comprehensive collector for NZX historical data"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.output_dir = self.base_dir / "comprehensive_nzx_data"
        self.output_dir.mkdir(exist_ok=True)
        
        # Major NZX companies for priority collection
        self.major_companies = [
            'AIR', 'ATM', 'FPH', 'MCY', 'SPK', 'RYM', 'SKC', 'TPW',
            'ARG', 'EBO', 'FBU', 'GMT', 'IFT', 'KMD', 'MEL', 'NZX',
            'PYS', 'RAK', 'SKO', 'SML', 'VCT', 'WHS', 'AIA', 'AFI',
            'GNE', 'CEN', 'NTL', 'CVT', 'TRA', 'BRW', 'POT', 'PCT'
        ]
        
        # Extended NZX companies list
        self.extended_companies = [
            'BRM', 'CNU', 'CNV', 'CRP', 'EIR', 'ENS', 'FPA', 'FTZ',
            'GSH', 'HAU', 'HBL', 'HMU', 'HUM', 'IRT', 'JLG', 'KFL',
            'KYN', 'MAE', 'MDZ', 'MET', 'MLZ', 'MWR', 'NZM', 'OCT',
            'PGW', 'PLX', 'PNH', 'PPH', 'SCT', 'SPN', 'STM', 'SUM',
            'TWR', 'VTL', 'WHK', 'WYN', 'ZKB'
        ]
    
    def run_collection_strategy(self, strategy_name, command_args):
        """Run a specific collection strategy"""
        logging.info(f"\n{'='*80}")
        logging.info(f"STARTING COLLECTION STRATEGY: {strategy_name}")
        logging.info(f"{'='*80}")
        
        try:
            # Change to Balance_Sheet_Scraper directory
            cmd = ["python", "main_balance_sheet_scraper.py"] + command_args
            process = subprocess.Popen(
                cmd,
                cwd=str(self.base_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Monitor process
            start_time = time.time()
            logging.info(f"Process started with PID: {process.pid}")
            
            # Wait for completion with timeout
            try:
                stdout, stderr = process.communicate(timeout=3600)  # 1 hour timeout
                duration = time.time() - start_time
                
                if process.returncode == 0:
                    logging.info(f"[SUCCESS] Strategy '{strategy_name}' completed successfully in {duration:.1f}s")
                    if stdout:
                        logging.info(f"Output: {stdout[-500:]}")  # Last 500 chars
                else:
                    logging.error(f"[ERROR] Strategy '{strategy_name}' failed with return code {process.returncode}")
                    if stderr:
                        logging.error(f"Error: {stderr}")
                        
            except subprocess.TimeoutExpired:
                logging.warning(f"[TIMEOUT] Strategy '{strategy_name}' timed out after 1 hour")
                process.kill()
                
        except Exception as e:
            logging.error(f"[ERROR] Error running strategy '{strategy_name}': {e}")
    
    def collect_major_companies(self):
        """Collect data for major NZX companies"""
        logging.info("🎯 Collecting data for major NZX companies...")
        
        for i, company in enumerate(self.major_companies, 1):
            logging.info(f"[PROCESSING] {company} ({i}/{len(self.major_companies)})")
            
            cmd_args = [
                "--tickers", company,
                "--years", "15",
                "--export", "csv",
                "--output-dir", str(self.output_dir)
            ]
            
            self.run_collection_strategy(f"Major_Company_{company}", cmd_args)
            
            # Brief pause between companies
            time.sleep(5)
    
    def collect_extended_companies(self):
        """Collect data for extended NZX companies"""
        logging.info("📈 Collecting data for extended NZX companies...")
        
        # Process in batches to avoid overwhelming the system
        batch_size = 5
        for i in range(0, len(self.extended_companies), batch_size):
            batch = self.extended_companies[i:i+batch_size]
            batch_name = f"Extended_Batch_{i//batch_size + 1}"
            
            logging.info(f"\n📦 Processing batch: {batch}")
            
            cmd_args = [
                "--tickers"] + batch + [
                "--years", "10",
                "--export", "csv",
                "--output-dir", str(self.output_dir)
            ]
            
            self.run_collection_strategy(batch_name, cmd_args)
            
            # Longer pause between batches
            time.sleep(10)
    
    def collect_all_companies(self):
        """Collect data for all NZX companies"""
        logging.info("🌐 Collecting data for ALL NZX companies...")
        
        cmd_args = [
            "--years", "15",
            "--export", "excel",
            "--output-dir", str(self.output_dir),
            "--resume"
        ]
        
        self.run_collection_strategy("All_NZX_Companies", cmd_args)
    
    def generate_summary_report(self):
        """Generate a summary report of collected data"""
        logging.info("📋 Generating summary report...")
        
        try:
            # Get database stats
            cmd_args = ["--database-stats"]
            self.run_collection_strategy("Database_Stats", cmd_args)
            
            # Create summary file
            summary_file = self.output_dir / f"collection_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
            with open(summary_file, 'w') as f:
                f.write("COMPREHENSIVE NZX DATA COLLECTION SUMMARY\n")
                f.write("=" * 50 + "\n")
                f.write(f"Collection Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Output Directory: {self.output_dir}\n")
                f.write(f"Major Companies Processed: {len(self.major_companies)}\n")
                f.write(f"Extended Companies Processed: {len(self.extended_companies)}\n")
                f.write("\nCollection Strategies Used:\n")
                f.write("1. Major NZX Companies (15 years)\n")
                f.write("2. Extended NZX Companies (10 years)\n")
                f.write("3. All NZX Companies (15 years)\n")
                f.write("\nFiles Generated:\n")
                
                # List generated files
                for file_path in self.output_dir.glob("*"):
                    if file_path.is_file():
                        f.write(f"- {file_path.name}\n")
            
            logging.info(f"📄 Summary report saved to: {summary_file}")
            
        except Exception as e:
            logging.error(f"❌ Error generating summary report: {e}")
    
    def run_comprehensive_collection(self):
        """Run the complete comprehensive collection process"""
        logging.info("🚀 STARTING COMPREHENSIVE NZX HISTORICAL DATA COLLECTION")
        logging.info("=" * 80)
        
        start_time = datetime.now()
        
        try:
            # Strategy 1: Major companies (high priority)
            self.collect_major_companies()
            
            # Strategy 2: Extended companies
            self.collect_extended_companies()
            
            # Strategy 3: All companies (comprehensive)
            self.collect_all_companies()
            
            # Generate summary report
            self.generate_summary_report()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            logging.info(f"\n🎉 COMPREHENSIVE COLLECTION COMPLETED!")
            logging.info(f"⏱️  Total Duration: {duration}")
            logging.info(f"📁 Output Directory: {self.output_dir}")
            
        except KeyboardInterrupt:
            logging.info("\n⏹️  Collection interrupted by user")
        except Exception as e:
            logging.error(f"\n❌ Collection failed with error: {e}")

def main():
    """Main function"""
    collector = ComprehensiveNZXCollector()
    collector.run_comprehensive_collection()

if __name__ == "__main__":
    main()
