#!/usr/bin/env python3
"""Deep dive into ASX announcement page structure"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time

def deep_dive_asx():
    """Deep dive into ASX announcement page structure"""
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    })
    
    # Test the historical announcements page
    url = 'https://www.asx.com.au/content/asx/home/markets/trade-our-cash-market/historical-announcements.html'
    
    print(f"=== Deep dive into {url} ===")
    
    try:
        response = session.get(url, timeout=30)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for all JavaScript
            scripts = soup.find_all('script')
            print(f"Total scripts found: {len(scripts)}")
            
            for i, script in enumerate(scripts):
                if script.string:
                    script_content = script.string
                    
                    # Look for any URLs in the script
                    urls = re.findall(r'https?://[^\s\'"]+', script_content)
                    if urls:
                        print(f"Script {i+1} URLs: {urls[:5]}")  # Show first 5 URLs
                    
                    # Look for any data loading patterns
                    if any(keyword in script_content.lower() for keyword in ['fetch', 'xhr', 'ajax', 'api', 'announcement']):
                        print(f"Script {i+1} contains data loading patterns")
                        
                        # Extract relevant parts
                        lines = script_content.split('\n')
                        relevant_lines = [line.strip() for line in lines if any(keyword in line.lower() for keyword in ['fetch', 'xhr', 'ajax', 'api', 'announcement'])]
                        if relevant_lines:
                            print(f"  Relevant lines: {relevant_lines[:3]}")  # Show first 3 lines
                    
                    # Look for any configuration or data
                    if 'config' in script_content.lower() or 'data' in script_content.lower():
                        print(f"Script {i+1} contains config/data")
                        
                        # Try to extract JSON-like data
                        json_matches = re.findall(r'\{[^{}]*\}', script_content)
                        for match in json_matches[:3]:  # Show first 3 matches
                            try:
                                data = json.loads(match)
                                print(f"  JSON data: {data}")
                            except:
                                pass
            
            # Look for any hidden inputs or data attributes
            hidden_inputs = soup.find_all('input', type='hidden')
            print(f"Hidden inputs: {len(hidden_inputs)}")
            for input_field in hidden_inputs:
                name = input_field.get('name', '')
                value = input_field.get('value', '')
                print(f"  {name}: {value}")
            
            # Look for any data attributes
            data_elements = soup.find_all(attrs={'data-': True})
            print(f"Elements with data attributes: {len(data_elements)}")
            for elem in data_elements[:5]:  # Show first 5
                attrs = {k: v for k, v in elem.attrs.items() if k.startswith('data-')}
                print(f"  {elem.name}: {attrs}")
            
            # Look for any iframes that might load content
            iframes = soup.find_all('iframe')
            print(f"Iframes found: {len(iframes)}")
            for iframe in iframes:
                src = iframe.get('src', '')
                print(f"  Iframe src: {src}")
            
            # Look for any divs that might contain the actual data
            content_divs = soup.find_all('div', class_=lambda x: x and any(keyword in x.lower() for keyword in ['content', 'main', 'announcement', 'result', 'list', 'data']))
            print(f"Content divs found: {len(content_divs)}")
            for div in content_divs[:3]:  # Show first 3
                print(f"  Div class: {div.get('class', [])}")
                print(f"  Div content preview: {div.get_text()[:100]}...")
            
            # Save the full HTML for manual inspection
            with open('asx_page_source.html', 'w', encoding='utf-8') as f:
                f.write(response.text)
            print("Full page source saved to asx_page_source.html")
            
    except Exception as e:
        print(f"Error: {e}")
    
    # Try to find any API endpoints by looking at the page source
    print(f"\n=== Looking for API endpoints ===")
    
    try:
        response = session.get(url, timeout=30)
        content = response.text
        
        # Look for common API patterns
        api_patterns = [
            r'https?://[^\s\'"]*api[^\s\'"]*',
            r'https?://[^\s\'"]*announcement[^\s\'"]*',
            r'https?://[^\s\'"]*data[^\s\'"]*',
            r'https?://[^\s\'"]*statistics[^\s\'"]*',
            r'https?://[^\s\'"]*market[^\s\'"]*',
        ]
        
        for pattern in api_patterns:
            matches = re.findall(pattern, content)
            if matches:
                print(f"Pattern {pattern}: {matches[:5]}")  # Show first 5 matches
        
        # Look for any JavaScript variables that might contain URLs
        js_vars = re.findall(r'var\s+\w+\s*=\s*[\'"](https?://[^\s\'"]*)[\'"]', content)
        if js_vars:
            print(f"JavaScript variables with URLs: {js_vars[:5]}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    deep_dive_asx()
