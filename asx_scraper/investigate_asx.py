#!/usr/bin/env python3
"""Investigate current ASX website structure for announcements"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time

def investigate_asx_structure():
    """Investigate the current ASX website structure"""
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    })
    
    # Test the current ASX announcement pages
    urls_to_test = [
        'https://www.asx.com.au/content/asx/home/markets/trade-our-cash-market/todays-announcements.html',
        'https://www.asx.com.au/content/asx/home/markets/trade-our-cash-market/historical-announcements.html',
        'https://www.asx.com.au/markets/trade-our-cash-market/todays-announcements',
        'https://www.asx.com.au/markets/trade-our-cash-market/historical-announcements',
    ]
    
    for url in urls_to_test:
        print(f"\n=== Testing {url} ===")
        try:
            response = session.get(url, timeout=30)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for any JavaScript that might load data
                scripts = soup.find_all('script')
                for script in scripts:
                    if script.string:
                        # Look for API endpoints
                        api_urls = re.findall(r'https?://[^\s\'"]+api[^\s\'"]*', script.string)
                        if api_urls:
                            print(f"API URLs found: {api_urls}")
                        
                        # Look for announcement-related endpoints
                        announcement_urls = re.findall(r'https?://[^\s\'"]*announcement[^\s\'"]*', script.string)
                        if announcement_urls:
                            print(f"Announcement URLs found: {announcement_urls}")
                        
                        # Look for any data loading patterns
                        if 'fetch(' in script.string or 'XMLHttpRequest' in script.string:
                            print("Found data loading patterns in JavaScript")
                
                # Look for any forms that might be used for searching
                forms = soup.find_all('form')
                for form in forms:
                    action = form.get('action', '')
                    method = form.get('method', 'GET')
                    print(f"Form: {method} {action}")
                    
                    # Look for input fields
                    inputs = form.find_all('input')
                    for input_field in inputs:
                        name = input_field.get('name', '')
                        input_type = input_field.get('type', '')
                        value = input_field.get('value', '')
                        print(f"  Input: {input_type} {name} = {value}")
                
                # Look for any data attributes
                data_elements = soup.find_all(attrs={'data-': True})
                if data_elements:
                    print(f"Elements with data attributes: {len(data_elements)}")
                    for elem in data_elements[:3]:
                        attrs = {k: v for k, v in elem.attrs.items() if k.startswith('data-')}
                        print(f"  {elem.name}: {attrs}")
                
                # Look for any tables or structured data
                tables = soup.find_all('table')
                print(f"Tables found: {len(tables)}")
                
                # Look for any divs with specific classes that might contain data
                data_divs = soup.find_all('div', class_=lambda x: x and any(keyword in x.lower() for keyword in ['data', 'announcement', 'result', 'list']))
                print(f"Data divs found: {len(data_divs)}")
                
                # Show page title and meta information
                title = soup.find('title')
                if title:
                    print(f"Page title: {title.get_text()}")
                
                # Look for any meta tags that might indicate the page structure
                meta_tags = soup.find_all('meta')
                for meta in meta_tags:
                    name = meta.get('name', '')
                    content = meta.get('content', '')
                    if 'description' in name.lower() or 'keywords' in name.lower():
                        print(f"Meta {name}: {content}")
                
        except Exception as e:
            print(f"Error: {e}")
        
        time.sleep(1)  # Be respectful to the server
    
    # Try to find any API documentation or endpoints
    print(f"\n=== Looking for API documentation ===")
    api_docs_urls = [
        'https://www.asx.com.au/api',
        'https://www.asx.com.au/developers',
        'https://www.asx.com.au/data',
        'https://www.asx.com.au/markets/market-resources',
    ]
    
    for url in api_docs_urls:
        try:
            response = session.get(url, timeout=30)
            print(f"{url}: {response.status_code}")
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                links = soup.find_all('a', href=True)
                api_links = [link['href'] for link in links if 'api' in link['href'].lower()]
                if api_links:
                    print(f"  API links found: {api_links[:5]}")
        except Exception as e:
            print(f"{url}: Error - {e}")

if __name__ == "__main__":
    investigate_asx_structure()
