#!/usr/bin/env python3
"""Test detail page structure to find website URL."""
import requests
from bs4 import BeautifulSoup
import re

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}

# Test with a known company that has a website
detail_url = "https://www.enfsolar.com/evergreen-solar-battery?directory=installer&list=United+States"
print(f"Fetching: {detail_url}")

resp = requests.get(detail_url, headers=headers, timeout=30)
soup = BeautifulSoup(resp.text, 'html.parser')

print(f"Status: {resp.status_code}")
print(f"Page title: {soup.title.get_text() if soup.title else 'N/A'}")

# Look for all links on the page
print("\n=== ALL EXTERNAL LINKS ===")
all_links = soup.find_all('a', href=True)
for a in all_links:
    href = a.get('href', '')
    if href.startswith('http') and 'enfsolar' not in href:
        text = a.get_text(strip=True)[:40]
        print(f"  {href[:80]} - {text}")

# Look for website in specific sections
print("\n=== COMPANY INFO SECTION ===")
# Try to find company details table/section
info_section = soup.select('.enf-company-info, .company-info, .info-box, table.info-table')
print(f"Found {len(info_section)} info sections")

# Check for table with Website row
tables = soup.find_all('table')
print(f"\nFound {len(tables)} tables")
for i, table in enumerate(tables):
    table_text = table.get_text()
    if 'website' in table_text.lower():
        print(f"\nTable {i} contains 'website':")
        rows = table.find_all('tr')
        for row in rows:
            row_text = row.get_text(strip=True)[:120]
            if row_text:
                print(f"  {row_text}")
                # Check for links in this row
                links = row.find_all('a')
                for link in links:
                    href = link.get('href', '')
                    if href and 'enfsolar' not in href.lower():
                        print(f"    -> WEBSITE: {href}")

# Check for direct URL patterns in text
print("\n=== URL PATTERNS IN TEXT ===")
text_content = soup.get_text()
urls = re.findall(r'https?://[^\s<>"\']+', text_content)
for url in urls[:20]:
    if 'enfsolar' not in url:
        print(f"  {url}")

# Check the raw HTML for website patterns
print("\n=== RAW HTML PATTERNS ===")
html = resp.text
website_matches = re.findall(r'Website[^<]*<[^>]*href="([^"]+)"', html, re.IGNORECASE)
for match in website_matches[:5]:
    print(f"  {match}")

# Look for data attributes
print("\n=== DATA ATTRIBUTES ===")
elements_with_data = soup.select('[data-url], [data-website], [data-href]')
for el in elements_with_data[:5]:
    print(f"  {el.get('data-url') or el.get('data-website') or el.get('data-href')}")

# Check for href with target blank (often used for external links)
print("\n=== LINKS WITH target=_blank ===")
blank_links = soup.select('a[target="_blank"]')
for a in blank_links[:10]:
    href = a.get('href', '')
    if href and 'enfsolar' not in href.lower():
        print(f"  {href[:80]}")
