#!/usr/bin/env python3
"""Remove specified columns from CSV and overwrite the file (with backup).
Columns removed: website_urls_all, website_domains_all, website_count
"""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / 'enf_us_installers_pilot_pages1-5_with_domains.csv'
BACKUP = ROOT / (INPUT.name + '.bak')
COLUMNS_TO_REMOVE = {'website_urls_all', 'website_domains_all', 'website_count'}

if not INPUT.exists():
    raise SystemExit(f"Input file not found: {INPUT}")

# Make a backup
if not BACKUP.exists():
    BACKUP.write_bytes(INPUT.read_bytes())
    print(f"Backup written to: {BACKUP}")
else:
    print(f"Backup already exists: {BACKUP}")

# Read and rewrite without the unwanted columns
with INPUT.open(newline='', encoding='utf-8') as f_in:
    reader = csv.DictReader(f_in)
    # Determine remaining fieldnames in original order
    remaining = [fn for fn in reader.fieldnames if fn not in COLUMNS_TO_REMOVE]
    rows = list(reader)

with INPUT.open('w', newline='', encoding='utf-8') as f_out:
    writer = csv.DictWriter(f_out, fieldnames=remaining)
    writer.writeheader()
    for r in rows:
        # Build a new row with only remaining keys
        out = {k: r.get(k, '') for k in remaining}
        writer.writerow(out)

print(f"Removed columns: {sorted(COLUMNS_TO_REMOVE)}")
print(f"Updated file: {INPUT}")
