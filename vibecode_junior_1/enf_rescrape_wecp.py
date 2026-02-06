#!/usr/bin/env python3
"""
ENF Solar - Re-scrape entries with multiple websites
Only extract website links with data-event="wecp_clk"
"""

import os
import csv
import time
import math
from urllib.parse import urlparse
from dotenv import load_dotenv
from apify_client import ApifyClient

load_dotenv()

CSV_FILE = "enf_us_installers.csv"
BATCH_SIZE = 100

# Page function to extract only links with data-event="wecp_clk"
PAGE_FUNCTION = '''
async function pageFunction(context) {
    const { request, $, log } = context;
    
    // Find all links with data-event="wecp_clk"
    const wecpLinks = [];
    $('a[data-event="wecp_clk"]').each((i, el) => {
        const href = $(el).attr('href');
        if (href && href.startsWith('http')) {
            wecpLinks.push(href);
        }
    });
    
    // Get the first wecp_clk link as primary
    let website = '';
    if (wecpLinks.length > 0) {
        website = wecpLinks[0];
    }
    
    return {
        url: request.url,
        website: website,
        allWecpLinks: wecpLinks.join(' | ')
    };
}
'''

def extract_domain(url):
    """Extract domain from URL"""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return ""

def main():
    print("=" * 60)
    print("ENF Solar - Re-scrape Multi-Website Entries")
    print("Extracting only data-event='wecp_clk' links")
    print("=" * 60)
    
    # Read CSV
    print("\n[Step 1] Reading CSV...")
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    
    print(f"Loaded {len(rows)} entries")
    
    # Find entries with website_count > 1
    multi_website_indices = []
    for i, row in enumerate(rows):
        count = row.get('website_count', '0')
        if count not in ['', '0', '1']:
            multi_website_indices.append(i)
    
    print(f"Found {len(multi_website_indices)} entries with website_count > 1")
    
    if not multi_website_indices:
        print("No entries to process!")
        return
    
    # Clear website_url_primary and website_domain_primary for these entries
    print("\n[Step 2] Clearing primary website fields for multi-website entries...")
    urls_to_scrape = []
    for idx in multi_website_indices:
        rows[idx]['website_url_primary'] = ''
        rows[idx]['website_domain_primary'] = ''
        urls_to_scrape.append({
            'index': idx,
            'url': rows[idx]['detail_url']
        })
    
    print(f"Cleared {len(multi_website_indices)} entries")
    
    # Save cleared state
    with open(CSV_FILE, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print("Saved cleared state to CSV")
    
    # Initialize Apify
    print("\n[Step 3] Initializing Apify client...")
    api_key = os.getenv("EXPERT_APIKEY")
    proxy_url = "https://agents.toloka.ai/api/proxy/apify"
    
    client = ApifyClient(api_key, api_url=proxy_url)
    user = client.user("me").get()
    print(f"Connected as: {user.get('username', 'Unknown')}")
    
    # Process in batches
    num_batches = math.ceil(len(urls_to_scrape) / BATCH_SIZE)
    print(f"\n[Step 4] Processing {len(urls_to_scrape)} URLs in {num_batches} batches")
    
    total_found = 0
    start_time = time.time()
    
    for batch_num in range(num_batches):
        batch_start = batch_num * BATCH_SIZE
        batch_end = min(batch_start + BATCH_SIZE, len(urls_to_scrape))
        batch = urls_to_scrape[batch_start:batch_end]
        
        print(f"\n{'=' * 60}")
        print(f"Batch {batch_num + 1}/{num_batches}: Processing {len(batch)} URLs...")
        print("=" * 60)
        
        # Prepare start URLs
        start_urls = [{"url": item['url']} for item in batch]
        
        run_input = {
            "startUrls": start_urls,
            "pageFunction": PAGE_FUNCTION,
            "proxyConfiguration": {"useApifyProxy": True},
            "maxConcurrency": 5,
            "maxRequestRetries": 5,
        }
        
        print("Starting Apify actor run...")
        
        try:
            run = client.actor("apify/cheerio-scraper").call(
                run_input=run_input,
                timeout_secs=600,
                memory_mbytes=1024
            )
            
            print(f"Actor finished: {run['status']} in {run.get('stats', {}).get('runTimeSecs', 0):.1f}s")
            
            # Get results
            results = list(client.dataset(run["defaultDatasetId"]).iterate_items())
            print(f"Retrieved {len(results)} results")
            
            # Create URL to result mapping
            url_to_result = {r['url']: r for r in results}
            
            # Update rows
            batch_found = 0
            for item in batch:
                idx = item['index']
                url = item['url']
                
                if url in url_to_result:
                    result = url_to_result[url]
                    website = result.get('website', '').strip()
                    
                    if website:
                        rows[idx]['website_url_primary'] = website
                        rows[idx]['website_domain_primary'] = extract_domain(website)
                        batch_found += 1
            
            total_found += batch_found
            print(f"Found {batch_found} wecp_clk websites in this batch")
            
            # Save after each batch
            with open(CSV_FILE, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f"Saved. Total wecp_clk websites found: {total_found}")
            
        except Exception as e:
            print(f"Error in batch {batch_num + 1}: {e}")
            continue
    
    elapsed = time.time() - start_time
    
    # Final summary
    print("\n" + "=" * 80)
    print("ENF Solar WECP Re-scrape Summary")
    print("=" * 80)
    
    still_missing = sum(1 for idx in multi_website_indices if not rows[idx].get('website_url_primary', '').strip())
    
    print(f"Entries processed: {len(multi_website_indices)}")
    print(f"wecp_clk websites found: {total_found}")
    print(f"Still missing: {still_missing}")
    print(f"Success rate: {total_found/len(multi_website_indices)*100:.1f}%")
    print("=" * 80)
    print(f"\nCompleted in {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")

if __name__ == "__main__":
    main()
