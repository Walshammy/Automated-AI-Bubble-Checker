#!/usr/bin/env python3
"""Debug ASX Selenium scraping to see what's happening"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time

def debug_asx_selenium():
    """Debug ASX Selenium scraping"""
    
    # Setup Chrome driver
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # Run without GUI
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    
    try:
        # Test different URLs
        urls_to_test = [
            'https://www.asx.com.au/asx/v2/statistics/announcements.do?by=asxCode&asxCode=CBA&timeframe=Y&period=Y3',
            'https://www.asx.com.au/asx/v2/statistics/announcements.do?by=asxCode&asxCode=CBA&timeframe=Y',
            'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=CBA&timeframe=Y',
            'https://www.asx.com.au/asx/v2/statistics/announcements.do?asxCode=CBA',
        ]
        
        for i, url in enumerate(urls_to_test):
            print(f"\n=== Testing URL {i+1}: {url} ===")
            
            driver.get(url)
            
            # Wait for page to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for JavaScript to execute
            time.sleep(3)
            
            # Get page source
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Look for tables
            tables = soup.find_all('table')
            print(f"Tables found: {len(tables)}")
            
            for j, table in enumerate(tables):
                rows = table.find_all('tr')
                print(f"Table {j+1}: {len(rows)} rows")
                
                if len(rows) > 4:  # Potential data table
                    print(f"  Potential data table with {len(rows)} rows")
                    
                    # Show first few rows
                    for k, row in enumerate(rows[:5]):
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 3:
                            cell_texts = [cell.get_text(strip=True) for cell in cells]
                            print(f"    Row {k+1}: {cell_texts}")
            
            # Look for any links
            links = soup.find_all('a', href=True)
            pdf_links = [link for link in links if '.pdf' in link['href'].lower()]
            print(f"PDF links found: {len(pdf_links)}")
            
            if pdf_links:
                print("Sample PDF links:")
                for link in pdf_links[:3]:
                    href = link['href']
                    text = link.get_text(strip=True)
                    print(f"  {text} -> {href}")
            
            # Look for any error messages
            error_elements = soup.find_all(text=lambda text: text and 'error' in text.lower())
            if error_elements:
                print(f"Error messages: {error_elements[:3]}")
            
            # Save page source for manual inspection
            filename = f'asx_debug_{i+1}.html'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print(f"Page source saved to {filename}")
            
            time.sleep(1)  # Be respectful to the server
    
    finally:
        driver.quit()

if __name__ == "__main__":
    debug_asx_selenium()
