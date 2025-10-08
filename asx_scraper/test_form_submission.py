#!/usr/bin/env python3
"""Test proper ASX form submission with Selenium"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time

def test_asx_form_submission():
    """Test proper ASX form submission"""
    
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
        # Navigate to ASX announcements page
        url = 'https://www.asx.com.au/asx/v2/statistics/announcements.do'
        print(f"Navigating to: {url}")
        driver.get(url)
        
        # Wait for page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        time.sleep(2)
        
        # Look for the form
        print("Looking for form elements...")
        
        # Find the ASX code input field
        try:
            asx_code_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, 'asxCode'))
            )
            print("Found ASX code input field")
            
            # Clear and enter CBA ticker
            asx_code_input.clear()
            asx_code_input.send_keys('CBA')
            print("Entered CBA ticker")
            
            # Find and select the year timeframe
            try:
                year_radio = driver.find_element(By.CSS_SELECTOR, 'input[name="timeframe"][value="Y"]')
                year_radio.click()
                print("Selected year timeframe")
            except Exception as e:
                print(f"Could not find year timeframe radio button: {e}")
            
            # Find and click the search button
            try:
                search_button = driver.find_element(By.CSS_SELECTOR, 'input[type="submit"][value="Search"]')
                search_button.click()
                print("Clicked search button")
                
                # Wait for results to load
                time.sleep(5)
                
                # Check for results
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # Look for tables
                tables = soup.find_all('table')
                print(f"Tables after form submission: {len(tables)}")
                
                for i, table in enumerate(tables):
                    rows = table.find_all('tr')
                    print(f"Table {i+1}: {len(rows)} rows")
                    
                    if len(rows) > 4:  # More than just the search form
                        print("Found potential results table!")
                        
                        # Show first few rows
                        for j, row in enumerate(rows[:10]):
                            cells = row.find_all(['td', 'th'])
                            if len(cells) >= 3:
                                cell_texts = [cell.get_text(strip=True) for cell in cells]
                                print(f"  Row {j+1}: {cell_texts}")
                
                # Look for PDF links
                pdf_links = soup.find_all('a', href=lambda x: x and '.pdf' in x.lower())
                print(f"PDF links after form submission: {len(pdf_links)}")
                
                for link in pdf_links[:5]:
                    href = link['href']
                    text = link.get_text(strip=True)
                    print(f"  PDF: {text} -> {href}")
                
                # Save page source for manual inspection
                with open('asx_form_submission_result.html', 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                print("Form submission result saved to asx_form_submission_result.html")
                
            except Exception as e:
                print(f"Error clicking search button: {e}")
                
        except Exception as e:
            print(f"Error finding form elements: {e}")
        
        # Try alternative approach - look for any JavaScript that might handle the form
        print("\n=== Looking for JavaScript form handling ===")
        
        # Get page source and look for JavaScript
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        scripts = soup.find_all('script')
        
        for i, script in enumerate(scripts):
            if script.string:
                script_content = script.string
                if 'submit' in script_content.lower() or 'form' in script_content.lower():
                    print(f"Script {i+1} contains form handling")
                    
                    # Look for any URLs in the script
                    import re
                    urls = re.findall(r'https?://[^\s\'"]+', script_content)
                    if urls:
                        print(f"  URLs found: {urls[:3]}")
    
    finally:
        driver.quit()

if __name__ == "__main__":
    test_asx_form_submission()
