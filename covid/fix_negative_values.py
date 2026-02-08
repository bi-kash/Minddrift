#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

HERE = Path(__file__).parent
IN = HERE / 'covid_global_impact_15countries_daily_2022_2024_FINAL.csv'
OUT = HERE / 'covid_global_impact_15countries_daily_2022_2024_FINAL_fixed.csv'
REPORT = HERE / 'negatives_report.csv'

print('Loading', IN)
df = pd.read_csv(IN, parse_dates=['Date'])

# find negatives
neg_cases = df[df['New Cases'] < 0]
neg_deaths = df[df['New Deaths'] < 0]
print('Negative New Cases count:', len(neg_cases))
print('Negative New Deaths count:', len(neg_deaths))

# save report of negative rows (keep original values)
if len(neg_cases) or len(neg_deaths):
    neg_all = pd.concat([neg_cases, neg_deaths]).drop_duplicates()
    neg_all.to_csv(REPORT, index=False)
    print('Wrote report to', REPORT)
else:
    print('No negatives found; no report written.')

# Correct negatives: set to 0 (could choose NaN instead)
fixed = df.copy()
fixed['New Cases_fixed'] = False
fixed['New Deaths_fixed'] = False

fixed.loc[fixed['New Cases'] < 0, 'New Cases'] = 0
fixed.loc[fixed['New Cases'] == 0, 'New Cases_fixed'] = True
fixed.loc[fixed['New Deaths'] < 0, 'New Deaths'] = 0
fixed.loc[fixed['New Deaths'] == 0, 'New Deaths_fixed'] = True

fixed.to_csv(OUT, index=False)
print('Wrote fixed CSV to', OUT, 'rows=', len(fixed))
print('Done.')
