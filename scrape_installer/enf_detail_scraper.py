#!/usr/bin/env python3
"""
ENF Solar Detail Page Scraper using Apify Actor
1. Sorts enf_us_installers.csv by source_page
2. Visits each detail_url in batches
3. Saves progress after each batch to avoid data loss
4. Extracts: website, phone, address info
"""
import os
import csv
import time
import math
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

from apify_client import ApifyClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = ROOT / "enf_us_installers.csv"
OUTPUT_CSV = ROOT / "enf_us_installers.csv"  # Same file - will update in place
BACKUP_CSV = ROOT / "enf_us_installers_backup.csv"
SUMMARY_FILE = ROOT / "enf_detail_scraper_summary.txt"

APIFY_API_URL = "https://agents.toloka.ai/api/proxy/apify"
EXPERT_APIKEY = os.getenv("EXPERT_APIKEY", "")

# Batch size for Apify runs
BATCH_SIZE = 200


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


def read_and_sort_csv() -> list[dict]:
    """Read input CSV and sort by source_page."""
    entries = []
    with open(INPUT_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert source_page to int for proper sorting
            try:
                row["_source_page_int"] = int(row.get("source_page", 0))
            except (ValueError, TypeError):
                row["_source_page_int"] = 0
            entries.append(row)
    
    # Sort by source_page
    entries.sort(key=lambda x: x["_source_page_int"])
    
    # Remove temporary sort key
    for entry in entries:
        del entry["_source_page_int"]
    
    return entries


def get_fieldnames(entries: list[dict]) -> list[str]:
    """Get all fieldnames ensuring proper order."""
    base_cols = [
        "company_name", "area", "battery_storage", "detail_url", "source_page",
        "website_url_primary", "website_domain_primary", "phone", "address",
        "website_count", "website_urls_all", "website_domains_all"
    ]
    
    # Get any extra columns from existing data
    existing_cols = set()
    for entry in entries:
        existing_cols.update(entry.keys())
    
    # Keep base columns in order, add any others at end
    fieldnames = [c for c in base_cols if c in existing_cols or c in base_cols]
    for col in existing_cols:
        if col not in fieldnames:
            fieldnames.append(col)
    
    return fieldnames


def save_csv(entries: list[dict], filepath: Path):
    """Save entries to CSV file."""
    if not entries:
        return
    
    fieldnames = get_fieldnames(entries)
    
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(entries)


def prepare_urls_for_batch(entries: list[dict], start_idx: int, end_idx: int) -> list[dict]:
    """Prepare URL list for Apify scraper for a batch of entries."""
    urls = []
    for i in range(start_idx, min(end_idx, len(entries))):
        entry = entries[i]
        detail_url = entry.get("detail_url", "")
        if detail_url:
            urls.append({
                "url": detail_url,
                "method": "GET",
                "userData": {
                    "entryIndex": i,
                    "company_name": entry.get("company_name", ""),
                }
            })
    return urls


def run_apify_batch(client: ApifyClient, urls: list[dict], batch_num: int, total_batches: int) -> dict:
    """Run Apify scraper for a batch of URLs and return results keyed by URL."""
    print(f"\n{'='*60}")
    print(f"Batch {batch_num}/{total_batches}: Processing {len(urls)} URLs...")
    print(f"{'='*60}")
    
    actor_id = "apify/cheerio-scraper"
    
    run_input = {
        "startUrls": urls,
        "pageFunction": """
async function pageFunction(context) {
    const { $, request, log } = context;
    
    const result = {
        detail_url: request.url,
        entryIndex: request.userData.entryIndex,
        websites: [],
        phone: '',
        address: '',
    };
    
    // Method 1: Look for website links with globe icon or Website label
    // ENF uses fa-globe icon next to website links
    $('a').each((i, el) => {
        const $el = $(el);
        const href = $el.attr('href') || '';
        
        // Skip internal/non-website links
        if (!href.startsWith('http')) return;
        if (href.includes('enfsolar.com')) return;
        if (href.includes('facebook.com') || href.includes('twitter.com') || 
            href.includes('linkedin.com') || href.includes('instagram.com') ||
            href.includes('youtube.com') || href.includes('google.com/maps')) return;
        
        // Check if this link is near a globe icon or Website text
        const parent = $el.parent();
        const parentHtml = parent.html() || '';
        const prevSibling = $el.prev();
        
        if (parentHtml.includes('fa-globe') || 
            parentHtml.includes('fa-external') ||
            parentHtml.toLowerCase().includes('website') ||
            prevSibling.hasClass('fa-globe')) {
            if (!result.websites.includes(href)) {
                result.websites.push(href);
            }
        }
    });
    
    // Method 2: Look in the company info section/table
    $('table.table tr, .company-profile tr, .enf-company-profile tr').each((i, row) => {
        const $row = $(row);
        const text = $row.text();
        const lowerText = text.toLowerCase();
        
        // Website row
        if (lowerText.includes('website')) {
            $row.find('a').each((j, link) => {
                const href = $(link).attr('href') || '';
                if (href.startsWith('http') && !href.includes('enfsolar.com')) {
                    if (!result.websites.includes(href)) {
                        result.websites.push(href);
                    }
                }
            });
        }
        
        // Phone row
        if (lowerText.includes('phone') || lowerText.includes('tel')) {
            const phoneMatch = text.match(/[\+]?[1]?[\s\-\.]?[\(]?\d{3}[\)]?[\s\-\.]?\d{3}[\s\-\.]?\d{4}/);
            if (phoneMatch && !result.phone) {
                result.phone = phoneMatch[0].trim();
            }
        }
        
        // Address row
        if (lowerText.includes('address') || lowerText.includes('location')) {
            const link = $row.find('a');
            if (link.length && link.attr('href')?.includes('maps')) {
                result.address = link.text().trim();
            } else {
                // Try to get address text
                const tds = $row.find('td');
                if (tds.length > 1) {
                    result.address = $(tds[1]).text().trim();
                }
            }
        }
    });
    
    // Method 3: Look for specific ENF page structure
    // Company info is often in a section with icons
    $('p, div').each((i, el) => {
        const $el = $(el);
        const html = $el.html() || '';
        const text = $el.text().trim();
        
        // Check for globe icon followed by link
        if (html.includes('fa-globe') && !result.websites.length) {
            const link = $el.find('a[href^="http"]');
            if (link.length) {
                const href = link.attr('href');
                if (href && !href.includes('enfsolar.com')) {
                    result.websites.push(href);
                }
            }
        }
        
        // Check for phone icon
        if (html.includes('fa-phone') && !result.phone) {
            const phoneMatch = text.match(/[\+]?[1]?[\s\-\.]?[\(]?\d{3}[\)]?[\s\-\.]?\d{3}[\s\-\.]?\d{4}/);
            if (phoneMatch) {
                result.phone = phoneMatch[0].trim();
            }
        }
        
        // Check for map marker icon
        if (html.includes('fa-map-marker') && !result.address) {
            const link = $el.find('a');
            if (link.length) {
                result.address = link.text().trim();
            } else {
                result.address = text.replace(/^[^a-zA-Z0-9]+/, '').trim();
            }
        }
    });
    
    // Method 4: Direct search in page content
    const bodyText = $('body').text();
    const bodyHtml = $('body').html() || '';
    
    // Find all external links that look like company websites
    if (!result.websites.length) {
        $('a[href^="http"]').each((i, el) => {
            const href = $(el).attr('href') || '';
            const text = $(el).text().toLowerCase();
            
            if (href.includes('enfsolar.com')) return;
            if (href.includes('facebook.com') || href.includes('twitter.com') ||
                href.includes('linkedin.com') || href.includes('google.com')) return;
            
            // If link text looks like a domain or says "website"
            if (text.includes('.com') || text.includes('.net') || text.includes('.org') ||
                text.includes('website') || text.includes('visit')) {
                if (!result.websites.includes(href)) {
                    result.websites.push(href);
                }
            }
        });
    }
    
    // Phone pattern search
    if (!result.phone) {
        const phonePattern = /(?:phone|tel|call)[:\s]*([+\d\s\-\(\)\.]{10,20})/gi;
        const match = phonePattern.exec(bodyText);
        if (match) {
            result.phone = match[1].trim();
        }
    }
    
    // Deduplicate websites
    result.websites = [...new Set(result.websites)];
    
    return result;
}
""",
        "proxyConfiguration": {
            "useApifyProxy": True,
        },
        "maxConcurrency": 10,
        "maxRequestRetries": 3,
        "requestTimeoutSecs": 30,
    }
    
    print(f"Starting Apify actor run...")
    start = time.time()
    
    try:
        actor_run = client.actor(actor_id).call(run_input=run_input, timeout_secs=1200)
    except Exception as e:
        print(f"ERROR: Actor call failed: {e}")
        return {}
    
    # Poll for status
    run_id = actor_run.get("id")
    status = "UNKNOWN"
    while True:
        try:
            run_info = client.run(run_id).get()
            status = run_info.get("status", "UNKNOWN")
            msg = run_info.get("statusMessage", "")[:70]
            print(f"  Status: {status} | {msg}")
            
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                break
        except Exception as e:
            print(f"  Poll error: {e}")
        time.sleep(5)
    
    elapsed = time.time() - start
    print(f"Actor finished: {status} in {elapsed:.1f}s")
    
    # Collect results
    results_by_url = {}
    if status == "SUCCEEDED":
        dataset_id = actor_run.get("defaultDatasetId")
        if dataset_id:
            try:
                items = list(client.dataset(dataset_id).iterate_items())
                print(f"Retrieved {len(items)} results from dataset")
                
                for item in items:
                    url = item.get("detail_url", "")
                    if url:
                        results_by_url[url] = item
            except Exception as e:
                print(f"Error fetching results: {e}")
    
    return results_by_url


def update_entries_with_results(entries: list[dict], results: dict, start_idx: int, end_idx: int):
    """Update entries in-place with scraped results."""
    updated = 0
    for i in range(start_idx, min(end_idx, len(entries))):
        entry = entries[i]
        detail_url = entry.get("detail_url", "")
        result = results.get(detail_url, {})
        
        websites = result.get("websites", [])
        phone = result.get("phone", "")
        address = result.get("address", "")
        
        # Update entry with new fields
        if websites:
            entry["website_url_primary"] = websites[0]
            entry["website_domain_primary"] = normalize_domain(websites[0])
            
            if len(websites) > 1:
                entry["website_count"] = str(len(websites))
                entry["website_urls_all"] = "|".join(websites)
                entry["website_domains_all"] = "|".join(normalize_domain(w) for w in websites)
            updated += 1
        else:
            entry["website_url_primary"] = ""
            entry["website_domain_primary"] = ""
        
        entry["phone"] = phone
        entry["address"] = address
    
    return updated


def generate_summary(entries: list[dict]):
    """Generate summary report."""
    total = len(entries)
    
    with_primary_website = sum(1 for e in entries if e.get("website_url_primary"))
    with_phone = sum(1 for e in entries if e.get("phone"))
    with_address = sum(1 for e in entries if e.get("address"))
    with_multiple = sum(1 for e in entries if e.get("website_count"))
    
    summary = f"""
================================================================================
ENF Solar Detail Scraper Summary
================================================================================
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

TOTAL ENTRIES: {total}

FIELD COVERAGE:
--------------------------------------------------------------------------------
  Field                    | Present      | Missing      | Coverage
--------------------------------------------------------------------------------
  website_url_primary      | {with_primary_website:>6}       | {total - with_primary_website:>6}       | {100*with_primary_website/total:>5.1f}%
  phone                    | {with_phone:>6}       | {total - with_phone:>6}       | {100*with_phone/total:>5.1f}%
  address                  | {with_address:>6}       | {total - with_address:>6}       | {100*with_address/total:>5.1f}%
--------------------------------------------------------------------------------

MULTIPLE WEBSITES:
  Entries with multiple websites: {with_multiple}

OUTPUT FILE: {OUTPUT_CSV}
================================================================================
"""
    
    print(summary)
    
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(summary.strip())


def main():
    start_time = time.time()
    
    print("=" * 60)
    print("ENF Solar Detail Page Scraper")
    print("=" * 60)
    
    # Step 1: Read and sort CSV
    print(f"\n[Step 1] Reading and sorting {INPUT_CSV}...")
    entries = read_and_sort_csv()
    print(f"Loaded {len(entries)} entries")
    
    if not entries:
        print("No entries found!")
        return
    
    # Step 2: Create backup
    print(f"\n[Step 2] Creating backup at {BACKUP_CSV}...")
    save_csv(entries, BACKUP_CSV)
    print("Backup saved")
    
    # Step 3: Save sorted CSV
    print(f"\n[Step 3] Saving sorted CSV to {OUTPUT_CSV}...")
    save_csv(entries, OUTPUT_CSV)
    print("Sorted CSV saved")
    
    # Step 4: Initialize Apify client
    if not EXPERT_APIKEY:
        print("ERROR: EXPERT_APIKEY not found in .env file")
        return
    
    print(f"\n[Step 4] Initializing Apify client...")
    client = ApifyClient(token=EXPERT_APIKEY, api_url=APIFY_API_URL)
    
    try:
        user = client.user().get()
        print(f"Connected as: {user.get('username', 'Unknown')}")
    except Exception as e:
        print(f"Failed to connect to Apify: {e}")
        return
    
    # Step 5: Process in batches
    total_entries = len(entries)
    total_batches = math.ceil(total_entries / BATCH_SIZE)
    print(f"\n[Step 5] Processing {total_entries} URLs in {total_batches} batches of {BATCH_SIZE}")
    
    total_with_website = 0
    
    for batch_num in range(1, total_batches + 1):
        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = start_idx + BATCH_SIZE
        
        urls = prepare_urls_for_batch(entries, start_idx, end_idx)
        if not urls:
            continue
        
        # Run batch
        results = run_apify_batch(client, urls, batch_num, total_batches)
        
        # Update entries with results
        updated = update_entries_with_results(entries, results, start_idx, end_idx)
        total_with_website += updated
        
        # Save after each batch
        print(f"\nSaving progress after batch {batch_num}...")
        save_csv(entries, OUTPUT_CSV)
        print(f"Saved. Total websites found so far: {total_with_website}")
        
        # Brief pause between batches
        if batch_num < total_batches:
            print("Pausing 2s before next batch...")
            time.sleep(2)
    
    # Step 6: Generate summary
    print(f"\n[Step 6] Generating summary...")
    generate_summary(entries)
    
    elapsed = time.time() - start_time
    print(f"\nCompleted in {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")


if __name__ == "__main__":
    main()
