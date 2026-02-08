#!/usr/bin/env python3
"""
Re-scrape only entries missing website_url_primary
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

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
CSV_FILE = ROOT / "enf_us_installers.csv"
SUMMARY_FILE = ROOT / "enf_rescrape_summary.txt"

APIFY_API_URL = "https://agents.toloka.ai/api/proxy/apify"
EXPERT_APIKEY = os.getenv("EXPERT_APIKEY", "")

BATCH_SIZE = 100


def normalize_domain(url: str) -> str:
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


def read_csv():
    entries = []
    with open(CSV_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            entries.append(row)
    return entries, fieldnames


def save_csv(entries, fieldnames):
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(entries)


def get_missing_indices(entries):
    missing = []
    for i, entry in enumerate(entries):
        if not entry.get("website_url_primary"):
            missing.append(i)
    return missing


def prepare_urls(entries, indices):
    urls = []
    for idx in indices:
        entry = entries[idx]
        detail_url = entry.get("detail_url", "")
        if detail_url:
            urls.append({
                "url": detail_url,
                "method": "GET",
                "userData": {"entryIndex": idx}
            })
    return urls


def run_apify_batch(client, urls, batch_num, total_batches):
    print(f"\n{'='*60}")
    print(f"Batch {batch_num}/{total_batches}: Re-checking {len(urls)} URLs...")
    print(f"{'='*60}")
    
    actor_id = "apify/cheerio-scraper"
    
    page_function = '''
async function pageFunction(context) {
    const { $, request, log } = context;
    
    const result = {
        detail_url: request.url,
        entryIndex: request.userData.entryIndex,
        websites: [],
        phone: '',
        address: '',
    };
    
    const allLinks = [];
    $('a[href]').each((i, el) => {
        const href = $(el).attr('href') || '';
        if (href.startsWith('http') && !href.includes('enfsolar.com')) {
            allLinks.push({
                href: href,
                text: $(el).text().trim().toLowerCase(),
                parentText: $(el).parent().text().toLowerCase().slice(0, 100),
                hasGlobe: $(el).prev().hasClass('fa') || ($(el).parent().html() || '').includes('fa-globe')
            });
        }
    });
    
    const socialDomains = [
        'facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com',
        'youtube.com', 'pinterest.com', 'tiktok.com', 'yelp.com',
        'google.com', 'bbb.org', 'trustpilot.com', 'angieslist.com',
        'homeadvisor.com', 'thumbtack.com', 'apple.com', 'play.google.com'
    ];
    
    for (const link of allLinks) {
        const isSocial = socialDomains.some(d => link.href.toLowerCase().includes(d));
        if (isSocial) continue;
        
        const isWebsiteIndicator = 
            link.hasGlobe ||
            link.parentText.includes('website') ||
            link.parentText.includes('web:') ||
            link.text.includes('website') ||
            link.text.includes('visit') ||
            link.text.includes('.com') ||
            link.text.includes('.net') ||
            link.text.includes('.org') ||
            link.text.includes('.us') ||
            link.text.includes('.io');
        
        if (isWebsiteIndicator && !result.websites.includes(link.href)) {
            result.websites.push(link.href);
        }
    }
    
    if (!result.websites.length) {
        for (const link of allLinks) {
            const isSocial = socialDomains.some(d => link.href.toLowerCase().includes(d));
            if (!isSocial && !result.websites.includes(link.href)) {
                const skipDomains = ['cloudflare', 'amazonaws', 'googleapi', 'gstatic', 
                                     'cloudfront', 'jsdelivr', 'cdnjs', 'jquery'];
                const isSkip = skipDomains.some(d => link.href.includes(d));
                if (!isSkip) {
                    result.websites.push(link.href);
                }
            }
        }
    }
    
    const bodyText = $('body').text();
    const phoneMatch = bodyText.match(/\\(?\\d{3}\\)?[\\s.-]?\\d{3}[\\s.-]?\\d{4}/);
    if (phoneMatch) {
        result.phone = phoneMatch[0].trim();
    }
    
    $('div, p, span').each((i, el) => {
        const html = $(el).html() || '';
        if (html.includes('fa-map-marker') && !result.address) {
            const link = $(el).find('a');
            if (link.length) {
                result.address = link.text().trim();
            }
        }
    });
    
    result.websites = [...new Set(result.websites)];
    
    return result;
}
'''
    
    run_input = {
        "startUrls": urls,
        "pageFunction": page_function,
        "proxyConfiguration": {"useApifyProxy": True},
        "maxConcurrency": 10,
        "maxRequestRetries": 5,
        "requestTimeoutSecs": 45,
    }
    
    print(f"Starting Apify actor run...")
    start = time.time()
    
    try:
        actor_run = client.actor(actor_id).call(run_input=run_input, timeout_secs=900)
    except Exception as e:
        print(f"ERROR: Actor call failed: {e}")
        return {}
    
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
    
    results = {}
    if status == "SUCCEEDED":
        dataset_id = actor_run.get("defaultDatasetId")
        if dataset_id:
            try:
                items = list(client.dataset(dataset_id).iterate_items())
                print(f"Retrieved {len(items)} results")
                for item in items:
                    idx = item.get("entryIndex")
                    if idx is not None:
                        results[idx] = item
            except Exception as e:
                print(f"Error fetching results: {e}")
    
    return results


def update_entries(entries, results):
    updated = 0
    for idx, item in results.items():
        websites = item.get("websites", [])
        phone = item.get("phone", "")
        address = item.get("address", "")
        
        if websites:
            entries[idx]["website_url_primary"] = websites[0]
            entries[idx]["website_domain_primary"] = normalize_domain(websites[0])
            
            if len(websites) > 1:
                entries[idx]["website_count"] = str(len(websites))
                entries[idx]["website_urls_all"] = "|".join(websites)
                entries[idx]["website_domains_all"] = "|".join(normalize_domain(w) for w in websites)
            updated += 1
        
        if phone and not entries[idx].get("phone"):
            entries[idx]["phone"] = phone
        if address and not entries[idx].get("address"):
            entries[idx]["address"] = address
    
    return updated


def main():
    start_time = time.time()
    
    print("=" * 60)
    print("ENF Solar - Re-scrape Missing Websites")
    print("=" * 60)
    
    print("\n[Step 1] Reading CSV...")
    entries, fieldnames = read_csv()
    print(f"Loaded {len(entries)} entries")
    
    missing_indices = get_missing_indices(entries)
    print(f"Found {len(missing_indices)} entries missing website_url_primary")
    
    if not missing_indices:
        print("No missing entries to process!")
        return
    
    if not EXPERT_APIKEY:
        print("ERROR: EXPERT_APIKEY not found")
        return
    
    print("\n[Step 2] Initializing Apify client...")
    client = ApifyClient(token=EXPERT_APIKEY, api_url=APIFY_API_URL)
    
    try:
        user = client.user().get()
        print(f"Connected as: {user.get('username', 'Unknown')}")
    except Exception as e:
        print(f"Failed to connect: {e}")
        return
    
    total_batches = math.ceil(len(missing_indices) / BATCH_SIZE)
    print(f"\n[Step 3] Processing {len(missing_indices)} URLs in {total_batches} batches")
    
    total_found = 0
    
    for batch_num in range(1, total_batches + 1):
        start_idx = (batch_num - 1) * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(missing_indices))
        batch_indices = missing_indices[start_idx:end_idx]
        
        urls = prepare_urls(entries, batch_indices)
        if not urls:
            continue
        
        results = run_apify_batch(client, urls, batch_num, total_batches)
        
        found = update_entries(entries, results)
        total_found += found
        
        print(f"\nSaving after batch {batch_num}... Found {found} new websites")
        save_csv(entries, fieldnames)
        print(f"Saved. Total new websites found: {total_found}")
        
        if batch_num < total_batches:
            time.sleep(2)
    
    still_missing = len(get_missing_indices(entries))
    
    summary = f"""
================================================================================
ENF Solar Re-scrape Summary
================================================================================
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Initially missing: {len(missing_indices)}
Newly found: {total_found}
Still missing: {still_missing}
Recovery rate: {100*total_found/len(missing_indices):.1f}%

================================================================================
"""
    print(summary)
    
    with open(SUMMARY_FILE, "w") as f:
        f.write(summary.strip())
    
    elapsed = time.time() - start_time
    print(f"Completed in {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")


if __name__ == "__main__":
    main()
