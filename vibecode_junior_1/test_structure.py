#!/usr/bin/env python3
"""Quick test to understand ENF Solar page structure."""
import requests
from bs4 import BeautifulSoup

url = 'https://www.enfsolar.com/directory/installer/United%20States?page=1'
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}

resp = requests.get(url, headers=headers, timeout=30)
soup = BeautifulSoup(resp.text, 'html.parser')

# Check header row
thead = soup.select('table.enf-list-table thead tr th')
print('Headers:', [th.get_text(strip=True) for th in thead])

# Look at first 3 rows in detail
rows = soup.select('table.enf-list-table tbody tr')[:3]
for i, row in enumerate(rows):
    print(f'\n--- Row {i} ---')
    cells = row.find_all('td')
    for j, cell in enumerate(cells):
        text = cell.get_text(strip=True)[:80]
        links = cell.find_all('a')
        link_info = [(a.get('href', '')[:60], a.get_text(strip=True)[:30]) for a in links]
        print(f'  Cell {j}: text="{text}" links={link_info}')
        # Check for icons
        icons = cell.find_all('i')
        if icons:
            print(f'    Icons: {[ic.get("class", []) for ic in icons]}')

# Check a detail page
print("\n\n=== DETAIL PAGE TEST ===")
detail_url = "https://www.enfsolar.com/sunrise-electrician?directory=installer&list=United+States"
resp2 = requests.get(detail_url, headers=headers, timeout=30)
soup2 = BeautifulSoup(resp2.text, 'html.parser')

# Look for website link
print("\nLooking for website links...")
# Check company info section
info_tables = soup2.select('table')
for table in info_tables[:2]:
    rows = table.find_all('tr')
    for row in rows[:10]:
        row_text = row.get_text(strip=True)[:100]
        if 'website' in row_text.lower() or 'http' in row_text.lower():
            print(f"Found: {row_text}")
            links = row.find_all('a')
            for a in links:
                href = a.get('href', '')
                if href and 'enfsolar' not in href:
                    print(f"  Website link: {href}")

# Look for external links with nofollow
print("\nExternal links with nofollow:")
external = soup2.select('a[rel="nofollow"]')
for a in external[:5]:
    href = a.get('href', '')
    text = a.get_text(strip=True)[:40]
    print(f"  {href[:60]} - {text}")
