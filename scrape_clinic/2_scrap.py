import requests
from bs4 import BeautifulSoup
import csv
import time
from urllib.parse import urljoin

# Base URL
BASE_URL = 'https://web.archive.org/web/20250708180027/https://www.myfootdr.com.au/our-clinics/'
FILE_NAME = 'clinic_data.csv'

def get_soup(url, max_retries=3):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            time.sleep(9)
            if response.status_code == 200:
                return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            print(f"    Error: {str(e)[:80]}...")
            time.sleep(9)
    
    return None

def extract_region_links():
    print("\n" + "="*80)
    print("Step 1: Getting all regions")
    print("="*80)
    soup = get_soup(BASE_URL)
    if not soup:
        print("Couldn't load the main page")
        return []
    
    region_links = []
    seen = set()
    
    for link in soup.find_all('a', href=lambda x: x and '/our-clinics/regions/' in x):
        full_url = link['href'] if link['href'].startswith('http') else urljoin(BASE_URL, link['href'])
        
        if full_url not in seen:
            region_name = link.get_text(strip=True)
            region_links.append((region_name, full_url))
            seen.add(full_url)
            print(f"  ✓ {region_name}")
    
    print(f"\nFound {len(region_links)} regions total")
    return region_links

def extract_clinic_links_from_region(region_url):
    soup = get_soup(region_url)
    if not soup:
        return []
    
    clinics = []
    
    clinic_tables = soup.find_all('div', class_='regional-clinics')
    

    for table in clinic_tables:
        # Find all clinic links within the table
        for link in table.find_all('a', class_='feature-button'):
            clinic_name_div = link.find('div', class_='clinic-name')
            if clinic_name_div:
                clinic_name = clinic_name_div.get_text(strip=True)
                clinic_url = link['href']
                
                # Make sure URL is absolute
                if not clinic_url.startswith('http'):
                    clinic_url = urljoin(region_url, clinic_url)
                
                clinics.append({
                    'name': clinic_name,
                    'url': clinic_url
                })
    
    return clinics


def extract_all_clinic_links():
    region_links = extract_region_links()
    all_clinics = []
    
    for region_name, region_url in region_links:
        print(f"\n  {region_name}...")
        clinics = extract_clinic_links_from_region(region_url)
        print(f"    Got {len(clinics)} clinics")
        all_clinics.extend(clinics)
    
    print(f"\n{'='*80}")
    print(f"Total: {len(all_clinics)} clinics")
    print(f"{'='*80}")
    return all_clinics

def extract_clinic_details(clinic_url):
    soup = get_soup(clinic_url)
    if not soup:
        return None
    
    details = {
        'Name of Clinic': '',
        'Address': '',
        'Email': '',
        'Phone': '',
        'Services': []
    }
    
    metabox = soup.find('div', class_='clinic-metabox')
    
    if metabox:
        brand_logo = metabox.find('img', class_='brand-logo')
        if brand_logo:
            details['Name of Clinic'] = brand_logo.get('alt', '') or brand_logo.get('title', '')
        
        phone_link = metabox.find('a', href=lambda x: x and 'tel:' in x)
        if phone_link:
            details['Phone'] = phone_link.get_text(strip=True).replace('Call ', '').replace('(', '').replace(')', '').strip()
        
        address_div = metabox.find('div', class_='address')
        if address_div:
            address_link = address_div.find('a')
            if address_link:
                address_text = address_link.get_text(separator=' ', strip=True)
                details['Address'] = address_text.replace('i ', '').strip()
        
        email_links = metabox.find_all('a', href=True)
        for link in email_links:
            if 'mailto:' in link['href']:
                details['Email'] = link['href'].split('mailto:')[1]
                break
    
    services_heading = soup.find('h2', string=lambda text: text and 'Services Available' in text)
    if services_heading:
        services_container = services_heading.find_parent('div')
        if services_container:
            articles = services_container.find_all('article')
            for article in articles:
                h3 = article.find('h3')
                if h3:
                    service_link = h3.find('a')
                    if service_link:
                        service_name = service_link.get_text(strip=True)
                        details['Services'].append(service_name)
    
    details['Services'] = '; '.join(details['Services'])
    
    return details

def update_csv(filename, data):
    """Write data to CSV file"""
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        if data:
            fieldnames = ['Name of Clinic', 'Address', 'Email', 'Phone', 'Services']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

def extract_and_save_detailed_info(clinics, output_csv='clinic_details.csv', max_attempts=3):
    print("\n" + "="*80)
    print("Step 3: Getting details for each clinic")
    print("="*80)
    print(f"Processing {len(clinics)} clinics")
    print(f"Saving to: {output_csv}")
    print("="*80 + "\n")
    
    all_details = []
    failed_clinics = []
    
    for i, clinic in enumerate(clinics, 1):
        print(f"[{i}/{len(clinics)}] {clinic['name']}")
        
        details = extract_clinic_details(clinic['url'])
        
        if details and (details['Address'] or details['Email'] or details['Phone']):
            if not details['Name of Clinic']:
                details['Name of Clinic'] = clinic['name']
            all_details.append(details)
            
            update_csv(output_csv, all_details)
            
            print(f"  ✓ Done - saved {len(all_details)} clinics")
            if details['Address']:
                print(f"    {details['Address'][:60]}...")
        else:
            failed_clinics.append(clinic)
            print(f"  ✗ Couldn't get info")
        
    
    print("\n" + "="*80)
    print("Done!")
    print("="*80)
    print(f"✓ Got data for {len(all_details)} clinics")
    print(f"✗ Couldn't get {len(failed_clinics)} clinics")
    print(f"\nSaved to: {output_csv}")
    print("="*80)
    
    if failed_clinics:
        print(f"\nMissing:")
        for clinic in failed_clinics[:10]:
            print(f"  - {clinic['name']}")
        if len(failed_clinics) > 10:
            print(f"  ... and {len(failed_clinics) - 10} more")
    
    return len(all_details), len(failed_clinics)

def main():
    print("\n" + "="*80)
    print("MyFootDr Clinic Scraper")
    print("="*80)
    
    all_clinics = extract_all_clinic_links()
    
    if not all_clinics:
        print("\nNo clinics found")
        return
    
    
    successful, failed = extract_and_save_detailed_info(all_clinics, FILE_NAME)
    
    print("\n" + "="*80)
    print("All done!")
    print("="*80)
    print(f"File: {FILE_NAME}")
    print(f"Total: {successful} clinics")
    print(f"Failed: {failed}")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
