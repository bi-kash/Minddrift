#!/usr/bin/env python3
from pathlib import Path
import shutil
import pandas as pd

HERE=Path(__file__).parent
SRC=HERE/'covid_global_impact_15countries_daily_2022_2024_FINAL.csv'
BACK=HERE/'covid_global_impact_15countries_daily_2022_2024_FINAL_backup.csv'
REPORT=HERE/'negatives_report.csv'

# make backup copy
shutil.copy2(SRC, BACK)
print('Backup created:', BACK)

# load
df=pd.read_csv(SRC, parse_dates=['Date'])
# locate negatives
neg = df[(df['New Cases']<0)|(df['New Deaths']<0)].copy()
print('Negatives found:', len(neg))
if len(neg):
    neg.to_csv(REPORT, index=False)
    print('Wrote report to', REPORT)

# add flags if missing
for col in ['New Cases_fixed','New Deaths_fixed']:
    if col not in df.columns:
        df[col]=False

# fix negatives
mask_cases = df['New Cases'] < 0
mask_deaths = df['New Deaths'] < 0
if mask_cases.any():
    df.loc[mask_cases,'New Cases_fixed'] = True
    df.loc[mask_cases,'New Cases'] = 0
if mask_deaths.any():
    df.loc[mask_deaths,'New Deaths_fixed'] = True
    df.loc[mask_deaths,'New Deaths'] = 0
# remove fix-flag columns from final output
for col in ['New Cases_fixed','New Deaths_fixed']:
    if col in df.columns:
        df.drop(columns=[col], inplace=True)

# write back
df.to_csv(SRC, index=False)
print('Wrote updated FINAL CSV (flags removed):', SRC)
print('Remaining negatives (cases):', int((df['New Cases']<0).sum()))
print('Remaining negatives (deaths):', int((df['New Deaths']<0).sum()))
