#!/usr/bin/env python3
"""
ENF Solar Installer Directory Scraper using Apify Actor
Uses Apify's web scraper actor via the proxy API to bypass 403 blocks
"""
import os
import csv
import json
import time
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

from apify_client import ApifyClient
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
ROOT = Path(__file__).resolve().parents[1]
OUTPUT_CSV = ROOT / "enf_us_installers.csv"
SUMMARY_FILE = ROOT / "enf_scraper_summary.txt"

APIFY_API_URL = "https://agents.toloka.ai/api/proxy/apify"
EXPERT_APIKEY = os.getenv("EXPERT_APIKEY", "")

BASE_URL = "https://www.enfsolar.com/directory/installer/United%20States"
TOTAL_PAGES = 84


def normalize_domain(url: str) -> str:
    """Extract and normalize domain from URL."""
    if not url or url.strip() == "":
        return ""
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        if ":" in domain:
            domain = domain.split(":")[0]
        return domain
    except Exception:
        return ""


def generate_urls(start_page: int = 1, end_page: int = TOTAL_PAGES) -> list[dict]:
    """Generate URL list for Apify web scraper."""
    urls = []
    for page in range(start_page, end_page + 1):
        urls.append({
            "url": f"{BASE_URL}?page={page}",
            "method": "GET",
            "userData": {"pageNum": page}
        })
    return urls


def parse_listing_page(html: str, page_num: int) -> list[dict]:
    """Parse a listing page and extract installer entries."""
    soup = BeautifulSoup(html, "html.parser")
    installers = []
    
    rows = soup.select("table.enf-list-table tbody tr")
    
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        
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
                if href.startswith("/"):
                    entry["detail_url"] = "https://www.enfsolar.com" + href
                elif href.startswith("http"):
                    entry["detail_url"] = href
        
        # Cell 1: Area/State
        if len(cells) > 1:
            entry["area"] = cells[1].get_text(strip=True)
        
        # Cell 2: Battery Storage
        if len(cells) > 2:
            battery_text = cells[2].get_text(strip=True)
            if battery_text.lower() == "yes":
                entry["battery_storage"] = "Yes"
        
        if entry["company_name"]:
            installers.append(entry)
    
    return installers


def run_apify_scraper():
    """Run Apify web scraper to fetch all pages."""
    if not EXPERT_APIKEY:
        print("ERROR: EXPERT_APIKEY not found in .env file")
        return []
    
    print(f"Initializing Apify client...")
    client = ApifyClient(token=EXPERT_APIKEY, api_url=APIFY_API_URL)
    
    # Verify connection
    try:
        user = client.user().get()
        print(f"Connected as: {user.get('username', 'Unknown')}")
    except Exception as e:
        print(f"Failed to connect to Apify: {e}")
        return []
    
    # Generate URLs to scrape
    urls = generate_urls(1, TOTAL_PAGES)
    print(f"Prepared {len(urls)} URLs to scrape")
    
    # Run web scraper actor
    # Using cheerio-scraper which is faster for static HTML
    actor_id = "apify/cheerio-scraper"
    
    run_input = {
        "startUrls": urls,
        "pageFunction": """
async function pageFunction(context) {
    const { $, request, log } = context;
    const pageNum = request.userData.pageNum || 1;
    
    const results = [];
    $('table.enf-list-table tbody tr').each((index, row) => {
        const cells = $(row).find('td');
        if (cells.length < 2) return;
        
        const nameLink = $(cells[0]).find('a').first();
        const companyName = nameLink.text().trim();
        let detailUrl = nameLink.attr('href') || '';
        if (detailUrl.startsWith('/')) {
            detailUrl = 'https://www.enfsolar.com' + detailUrl;
        }
        
        const area = $(cells[1]).text().trim();
        const batteryStorage = cells.length > 2 && $(cells[2]).text().trim().toLowerCase() === 'yes' ? 'Yes' : '';
        
        if (companyName) {
            results.push({
                company_name: companyName,
                area: area,
                battery_storage: batteryStorage,
                detail_url: detailUrl,
                source_page: pageNum
            });
        }
    });
    
    return results;
}
        """,
        "proxyConfiguration": {"useApifyProxy": True},
        "maxConcurrency": 5,
        "maxRequestRetries": 3,
    }
    
    print(f"Starting Apify actor run...")
    try:
        run = client.actor(actor_id).call(run_input=run_input, timeout_secs=600)
        print(f"Actor run finished with status: {run.get('status')}")
        
        # Get results from dataset
        dataset_id = run.get("defaultDatasetId")
        if dataset_id:
            items = list(client.dataset(dataset_id).iterate_items())
            print(f"Retrieved {len(items)} result batches")
            
            # Flatten results (each item is an array from one page)
            all_installers = []
            for item in items:
                if isinstance(item, list):
                    all_installers.extend(item)
                elif isinstance(item, dict):
                    all_installers.append(item)
            
            return all_installers
        else:
            print("No dataset ID returned")
            return []
            
    except Exception as e:
        print(f"Actor run failed: {e}")
        return []


def save_to_csv(installers: list[dict], output_path: Path) -> None:
    """Save installers to CSV file."""
    fieldnames = [
        "company_name",
        "area", 
        "battery_storage",
        "detail_url",
        "source_page",
    ]
    
    # Deduplicate by detail_url
    seen = set()
    unique = []
    for inst in installers:
        url = inst.get("detail_url", "")
        if url and url not in seen:
            seen.add(url)
            unique.append(inst)
        elif not url:
            unique.append(inst)
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for installer in unique:
            writer.writerow({k: installer.get(k, "") for k in fieldnames})
    
    print(f"\nSaved {len(unique)} entries to {output_path}")
    return unique


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
    
    print("=" * 60)
    print("ENF Solar Installer Directory Scraper (Apify)")
    print("=" * 60)
    
    # Run Apify scraper
    installers = run_apify_scraper()
    
    if not installers:
        print("No installers found.")
        return
    
    # Save to CSV
    unique = save_to_csv(installers, OUTPUT_CSV)
    
    # Generate summary
    summary = generate_summary(unique, SUMMARY_FILE)
    print(summary)
    
    elapsed = time.time() - start_time
    print(f"\nCompleted in {elapsed:.1f} seconds")


if __name__ == "__main__":
    main()
