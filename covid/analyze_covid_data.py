#!/usr/bin/env python3
"""Analyze covid draft and summary CSVs in this folder.

Outputs:
 - Column presence and date parsing confirmation
 - Row counts and expected vs actual
 - Missingness summary from summary CSV
 - Optional spot-checks against OWID (internet required)
"""
from pathlib import Path
import sys
import pandas as pd
import requests
import io
import certifi
try:
    import pycountry
except Exception:
    pycountry = None

HERE = Path(__file__).parent
DRAFT = HERE / "covid_global_impact_15countries_daily_2022_2024_DRAFT.csv"
SUMMARY = HERE / "covid_global_impact_15countries_daily_2022_2024_DRAFT_summary.csv"

print("Paths:")
print(" - draft:", DRAFT)
print(" - summary:", SUMMARY)

if not DRAFT.exists():
    print("Draft CSV not found at", DRAFT)
    sys.exit(1)
if not SUMMARY.exists():
    print("Summary CSV not found at", SUMMARY)
    sys.exit(1)

# Read small sample to confirm columns and date format
sample = pd.read_csv(DRAFT, nrows=5)
print('\nSample header and dtypes:')
print(sample.dtypes)
print('\nSample rows:')
print(sample.to_csv(index=False))

required_cols = ['Country', 'Date', 'New Cases', 'New Deaths', 'Total Vaccinations', 'Stringency Index']
missing_cols = [c for c in required_cols if c not in sample.columns]
if missing_cols:
    print('\nMissing required columns:', missing_cols)
else:
    print('\nAll required columns present.')

# Try parsing Date column
try:
    full = pd.read_csv(DRAFT, parse_dates=['Date'])
    print('\nLoaded full draft; Date parsed as', full['Date'].dtype)
except Exception as e:
    print('\nFailed to parse Date automatically:', e)
    full = pd.read_csv(DRAFT)
    # try manual
    try:
        full['Date_parsed'] = pd.to_datetime(full['Date'], format='%Y-%m-%d')
        print('Manually parsed Date with format YYYY-MM-DD')
    except Exception as e2:
        print('Date parse failed:', e2)

# Row counts
total_rows = len(full)
countries = full['Country'].unique().tolist()
n_countries = len(countries)
print(f"\nTotal rows: {total_rows}")
print(f"Unique countries: {n_countries} (sample: {countries[:5]})")

# Expected complete daily coverage estimate
# Expected 1096 rows per country (2022-01-01..2024-12-31 inclusive accounting for leap 2024)
expected_per_country = 1096
expected_total = expected_per_country * n_countries
print(f"Expected rows for complete coverage: {expected_per_country} * {n_countries} = {expected_total}")
print(f"Missing rows vs expected: {expected_total - total_rows} (positive means missing)")

# Per-country counts
cnts = full.groupby('Country').size().sort_values(ascending=False)
print('\nPer-country row counts (top 10):')
print(cnts.head(10).to_string())

# Read summary CSV
summary = pd.read_csv(SUMMARY)
print('\nSummary CSV preview:')
print(summary.head(20).to_string(index=False))

# Check plausibility of missingness from summary
print('\nMissingness checks:')
for idx, row in summary.iterrows():
    country = row['Country']
    rows = row['rows']
    miss_vax = row['missing_total_vax']
    miss_str = row['missing_stringency']
    # plausibility: rows + missing_vax should be roughly number of dates available to source
    if rows + miss_vax < expected_per_country - 50:  # heuristic
        note = 'UNUSUALLY LOW coverage for vaccinations'
    else:
        note = ''
    print(f"{country}: rows={rows}, missing_vax={miss_vax}, missing_stringency={miss_str} {note}")

# Spot checks against OWID for a few countries/dates
print('\nAttempting spot-checks against Our World in Data (OWID). This requires internet access.')
try:
    owid_url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv'
    usecols = ['location', 'date', 'new_cases', 'new_deaths', 'total_vaccinations', 'stringency_index']
    print('Fetching OWID data securely using certifi...')
    resp = requests.get(owid_url, stream=True, verify=certifi.where(), timeout=60)
    resp.raise_for_status()
    owid = pd.read_csv(io.StringIO(resp.text), usecols=usecols, parse_dates=['date'])
    print('Fetched OWID data (this may take a few seconds).')

    checks = [
        ('United States', '2022-01-01'),
        ('India', '2023-06-01'),
        ('Australia', '2024-07-01')
    ]
    for loc, dt in checks:
        dt_ts = pd.to_datetime(dt)
        row_local = full[(full['Country'] == loc) & (full['Date'] == dt)]
        ow = owid[(owid['location'] == loc) & (owid['date'] == dt_ts)]
        print(f"\nSpot-check {loc} {dt}:")
        if not row_local.empty:
            print(' Local draft row:')
            print(row_local.to_string(index=False))
        else:
            print(' Local draft: MISSING')
        if not ow.empty:
            print(' OWID row:')
            print(ow.to_string(index=False))
        else:
            print(' OWID: MISSING')

except Exception as e:
    print('OWID fetch or compare failed:', e)

except Exception as e:
    print('OWID fetch or compare failed:', e)

# Analyze missing dates relative to full expected range 2022-01-01..2024-12-31
print('\nAnalyzing missing dates relative to 2022-01-01..2024-12-31 (expected 1096 days per country)')
expected_dates = pd.date_range('2022-01-01', '2024-12-31', freq='D')
expected_n = len(expected_dates)
miss_summary = []
for country in sorted(full['Country'].unique()):
    dates = pd.to_datetime(full.loc[full['Country'] == country, 'Date']).dropna().unique()
    dates = pd.to_datetime(dates)
    missing = expected_dates.difference(dates)
    miss_summary.append((country, len(missing), missing[:5]))

print(f'Expected days per country: {expected_n}')
print('\nMissing counts per country:')
for country, cnt, sample in miss_summary:
    pct = cnt / expected_n * 100
    sample_str = ', '.join(pd.DatetimeIndex(sample).strftime('%Y-%m-%d')) if len(sample) else '[]'
    print(f"{country}: missing={cnt} ({pct:.1f}%), sample_missing={sample_str}")

# Dates missing across many countries
all_present = full.copy()
all_present['Date_only'] = pd.to_datetime(all_present['Date']).dt.date
date_country_counts = all_present.groupby('Date_only')['Country'].nunique()
missing_country_counts = {d: 15 - c for d, c in date_country_counts.items()}
# convert to series with full expected dates
miss_counts_series = pd.Series({d: missing_country_counts.get(d, 15) for d in pd.date_range('2022-01-01', '2024-12-31').date})
top_problem_dates = miss_counts_series.sort_values(ascending=False).head(10)
print('\nTop dates with most countries missing data (date: countries_missing):')
for d, m in top_problem_dates.items():
    print(f"{d}: {m}")

# Build a full date spine per country and optionally fill cases/deaths from OWID
print('\nBuilding full date spine (2022-01-01..2024-12-31) and filling from OWID where possible...')
countries = sorted(full['Country'].unique())
spine_dates = pd.date_range('2022-01-01', '2024-12-31', freq='D')
mi = pd.MultiIndex.from_product([countries, spine_dates], names=['Country', 'Date'])
spine = pd.DataFrame(index=mi).reset_index()

# prepare full with Date as datetime
full['Date'] = pd.to_datetime(full['Date'])

# merge existing data onto spine
spined = pd.merge(spine, full, how='left', on=['Country', 'Date'])

# If OWID is available, try to fill New Cases/New Deaths where draft has NaN
if 'owid' in globals():
    owid_ren = owid.rename(columns={'location': 'Country', 'date': 'Date'})
    # ensure Date dtype
    owid_ren['Date'] = pd.to_datetime(owid_ren['Date'])
    # merge OWID values for filling
    spined = pd.merge(spined, owid_ren[['Country', 'Date', 'new_cases', 'new_deaths']], how='left', on=['Country', 'Date'])
    # fill missing draft values from OWID (note: OWID may have 0.0 while draft used NaN)
    spined['New Cases'] = spined['New Cases'].fillna(spined['new_cases'])
    spined['New Deaths'] = spined['New Deaths'].fillna(spined['new_deaths'])
    spined = spined.drop(columns=['new_cases', 'new_deaths'])

# helper: iso3 mapping (pycountry if available, else manual map for our 15 countries)
def country_to_iso3(name):
    manual = {
        'Australia': 'AUS', 'Brazil': 'BRA', 'Canada': 'CAN', 'China': 'CHN',
        'France': 'FRA', 'Germany': 'DEU', 'India': 'IND', 'Italy': 'ITA',
        'Japan': 'JPN', 'Mexico': 'MEX', 'South Africa': 'ZAF', 'South Korea': 'KOR',
        'Spain': 'ESP', 'United Kingdom': 'GBR', 'United States': 'USA'
    }
    if pycountry:
        try:
            c = pycountry.countries.lookup(name)
            return c.alpha_3
        except Exception:
            return manual.get(name)
    return manual.get(name)

# write spined CSV
out_path = HERE / 'covid_global_impact_15countries_daily_2022_2024_spined.csv'
spined.to_csv(out_path, index=False)
print(f'Wrote spined CSV to {out_path} with rows={len(spined)}')

# report per-country counts after spining
per_country_after = spined.groupby('Country').size()
print('\nPer-country row counts after spining (should be 1096 each):')
print(per_country_after.to_string())

print('\nIf WHO/HDX joins are producing NaN for cases/deaths due to ISO mapping, use `country_to_iso3()` for a robust mapping (pycountry preferred).')

print('\nDone.')
