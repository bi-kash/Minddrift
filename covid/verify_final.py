#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

CSV = Path(__file__).parent / 'covid_global_impact_15countries_daily_2022_2024_FINAL.csv'
XLSX = Path(__file__).parent / 'covid_global_impact_15countries_daily_2022_2024_FINAL.xlsx'

# Read CSV
df = pd.read_csv(CSV, parse_dates=['Date'])
print('columns:', list(df.columns))
# sample date format
print('sample date format example:', df['Date'].dt.strftime('%Y-%m-%d').iat[0])
print('total rows:', len(df))
print('\nper-country counts:')
print(df.groupby('Country').size().to_string())

# Read Excel sheets
xls = pd.ExcelFile(XLSX)
print('\nsheets:', xls.sheet_names)

# show README (first 50 cells)
if 'README' in xls.sheet_names:
    rd = pd.read_excel(xls, 'README', header=None)
    print('\nREADME preview (first 50 cells):')
    flat = rd.stack().astype(str).tolist()
    print('\n'.join(flat[:50]))
    # check for OWID note
    text = ' '.join(flat)
    if 'OWID' in text and '2024-08-12' in text:
        print('\nFOUND: README mentions OWID last date 2024-08-12')
    else:
        print('\nREADME does not mention OWID last date 2024-08-12 in preview')

# check required sheets exist
for s in ['data','continuity_check','missingness_by_country']:
    print(f"sheet '{s}' present:", s in xls.sheet_names)

# continuity_check preview
if 'continuity_check' in xls.sheet_names:
    cc = pd.read_excel(xls, 'continuity_check')
    print('\ncontinuity_check preview:')
    print(cc.head(10).to_string())

# missingness_by_country preview
if 'missingness_by_country' in xls.sheet_names:
    m = pd.read_excel(xls, 'missingness_by_country')
    print('\nmissingness_by_country preview:')
    print(m.head(10).to_string())
