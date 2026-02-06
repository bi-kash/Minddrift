#!/usr/bin/env python3
"""
ENF Solar Installer Directory Scraper
Scrapes pages 1-84 of US installers using requests + BeautifulSoup
Outputs CSV in same format as enf_us_installers_pilot_pages1-5_with_domains.csv
"""
import os
import csv
import json
import time
import re
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
ROOT = Path(__file__).resolve().parents[1]
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_CSV = ROOT / f"enf_us_installers_.csv"
SUMMARY_FILE = ROOT / f"enf_scraper_summary.txt"

BASE_URL = "https://www.enfsolar.com/directory/installer/United%20States"
DETAIL_BASE = "https://www.enfsolar.com"
TOTAL_PAGES = 84

# Headers to mimic browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

# Rate limiting
REQUEST_DELAY = 0.3  # seconds between requests


def normalize_domain(url: str) -> str:
    """Extract and normalize domain from URL.
    
    - Strip scheme (http/https)
    - Strip www prefix
    - Strip paths and query strings
    - Keep punycode for internationalized domains
    """
    if not url or url.strip() == "":
        return ""
    
    url = url.strip()
    
    # Add scheme if missing for proper parsing
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Remove www prefix
        if domain.startswith("www."):
            domain = domain[4:]
        
        # Remove port if present
        if ":" in domain:
            domain = domain.split(":")[0]
        
        return domain
    except Exception:
        return ""


def extract_installers_from_page(html: str, page_num: int) -> list[dict]:
    """Parse a listing page and extract installer entries."""
    soup = BeautifulSoup(html, "html.parser")
    installers = []
    
    # Find installer rows in the main table
    rows = soup.select("table.enf-list-table tbody tr")
    
    for row in rows:
        try:
            entry = extract_row_data(row, page_num)
            if entry and entry.get("company_name"):
                installers.append(entry)
        except Exception as e:
            print(f"  Warning: Error parsing row: {e}")
            continue
    
    return installers


def extract_row_data(row, page_num: int) -> dict | None:
    """Extract data from a single table row.
    
    Structure:
    - Cell 0: Company name with link to detail page
    - Cell 1: State/Area
    - Cell 2: Battery storage (Yes or empty)
    - Cell 3-4: Other info
    - Cell 5: Country
    """
    cells = row.find_all("td")
    if len(cells) < 2:
        return None
    
    entry = {
        "company_name": "",
        "area": "",
        "battery_storage": "",
        "detail_url": "",
        "source_page": page_num,
    }
    
    # Cell 0: Company name and detail URL
    name_cell = cells[0]
    name_link = name_cell.find("a")
    if name_link:
        entry["company_name"] = name_link.get_text(strip=True)
        href = name_link.get("href", "")
        if href:
            # Ensure full URL
            if href.startswith("/"):
                entry["detail_url"] = DETAIL_BASE + href
            elif href.startswith("http"):
                entry["detail_url"] = href
            else:
                entry["detail_url"] = DETAIL_BASE + "/" + href
    
    # Cell 1: Area/State
    if len(cells) > 1:
        entry["area"] = cells[1].get_text(strip=True)
    
    # Cell 2: Battery Storage
    if len(cells) > 2:
        battery_text = cells[2].get_text(strip=True)
        if battery_text.lower() == "yes":
            entry["battery_storage"] = "Yes"
    
    return entry


def fetch_listing_page(session: requests.Session, page_num: int) -> str:
    """Fetch a listing page."""
    url = f"{BASE_URL}?page={page_num}"
    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"Error fetching page {page_num}: {e}")
        return ""


def scrape_all_pages(start_page: int = 1, end_page: int = TOTAL_PAGES) -> list[dict]:
    """Scrape all listing pages and collect installer entries."""
    all_installers = []
    seen_urls = set()  # For deduplication
    
    session = requests.Session()
    
    print(f"Scraping ENF Solar installer directory (pages {start_page}-{end_page})...")
    print(f"Output will be saved to: {OUTPUT_CSV}")
    print("-" * 60)
    
    for page in range(start_page, end_page + 1):
        print(f"Page {page}/{end_page}...", end=" ", flush=True)
        
        html = fetch_listing_page(session, page)
        if not html:
            print("FAILED")
            continue
        
        installers = extract_installers_from_page(html, page)
        
        # Deduplicate and add to results
        new_count = 0
        for installer in installers:
            detail_url = installer.get("detail_url", "")
            if detail_url and detail_url not in seen_urls:
                seen_urls.add(detail_url)
                all_installers.append(installer)
                new_count += 1
        
        print(f"found {len(installers)} entries ({new_count} new)")
        time.sleep(REQUEST_DELAY)
    
    return all_installers


def save_to_csv(installers: list[dict], output_path: Path) -> None:
    """Save installers to CSV file."""
    fieldnames = [
        "company_name",
        "area", 
        "battery_storage",
        "detail_url",
        "source_page",
    ]
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for installer in installers:
            writer.writerow({k: installer.get(k, "") for k in fieldnames})
    
    print(f"\nSaved {len(installers)} entries to {output_path}")


def generate_summary(installers: list[dict], output_path: Path) -> str:
    """Generate summary statistics."""
    total = len(installers)
    if total == 0:
        return "No data collected."
    
    with_area = sum(1 for i in installers if i.get("area"))
    without_area = total - with_area
    
    with_battery = sum(1 for i in installers if i.get("battery_storage") == "Yes")
    without_battery = total - with_battery
    
    with_detail_url = sum(1 for i in installers if i.get("detail_url"))
    without_detail_url = total - with_detail_url
    
    # Count by source page
    pages = {}
    for i in installers:
        page = i.get("source_page", 0)
        pages[page] = pages.get(page, 0) + 1
    
    # Count by area/state
    areas = {}
    for i in installers:
        area = i.get("area", "Unknown") or "Unknown"
        areas[area] = areas.get(area, 0) + 1
    
    summary = f"""
================================================================================
ENF Solar US Installers Scrape Summary
================================================================================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

TOTAL ENTRIES: {total}

FIELD COVERAGE:
--------------------------------------------------------------------------------
  Field                    | Present      | Missing      | Coverage
--------------------------------------------------------------------------------
  company_name             | {total:>6}       | {0:>6}       | 100.0%
  area                     | {with_area:>6}       | {without_area:>6}       | {100*with_area/total:>5.1f}%
  battery_storage (Yes)    | {with_battery:>6}       | {without_battery:>6}       | {100*with_battery/total:>5.1f}%
  detail_url               | {with_detail_url:>6}       | {without_detail_url:>6}       | {100*with_detail_url/total:>5.1f}%
--------------------------------------------------------------------------------

ENTRIES PER PAGE:
  Pages scraped: {len(pages)}
  Average entries per page: {total/len(pages) if pages else 0:.1f}
  Min entries on a page: {min(pages.values()) if pages else 0}
  Max entries on a page: {max(pages.values()) if pages else 0}

TOP 15 STATES/AREAS:
"""
    
    sorted_areas = sorted(areas.items(), key=lambda x: x[1], reverse=True)[:15]
    for area, count in sorted_areas:
        summary += f"  {area:<30} : {count:>5} ({100*count/total:>5.1f}%)\n"
    
    summary += f"""
OUTPUT FILES:
  - CSV: {OUTPUT_CSV}
  - Summary: {output_path}
================================================================================
"""
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(summary)
    
    return summary


def main():
    """Main entry point."""
    start_time = time.time()
    
    # Check if we should do a test run first
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        print("Running test mode (pages 1-3 only)...")
        installers = scrape_all_pages(1, 3)
    else:
        # Scrape all pages
        installers = scrape_all_pages(1, TOTAL_PAGES)
    
    if not installers:
        print("No installers found. Check if the website structure has changed.")
        return
    
    # Save to CSV
    save_to_csv(installers, OUTPUT_CSV)
    
    # Generate summary
    summary = generate_summary(installers, SUMMARY_FILE)
    print(summary)
    
    elapsed = time.time() - start_time
    print(f"\nCompleted in {elapsed/60:.1f} minutes ({elapsed:.0f} seconds)")


if __name__ == "__main__":
    main()
