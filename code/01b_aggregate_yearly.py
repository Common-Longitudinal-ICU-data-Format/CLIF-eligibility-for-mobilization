"""
01b_aggregate_yearly.py — Aggregate yearly outputs from 01_cohort_identification.py

When the pipeline runs in --chunked mode, 01_cohort_identification.py saves per-year
outputs to output/yearly/{year}/. This script concatenates them into the standard
output/intermediate/ layout that 02_mobilization_analysis.py expects.

Usage:
    uv run python 01b_aggregate_yearly.py
"""

import os
import sys
import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────────
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
yearly_base = os.path.join(project_root, 'output', 'yearly')
output_intermediate = os.path.join(project_root, 'output', 'intermediate')
output_final = os.path.join(project_root, 'output', 'final')

os.makedirs(output_intermediate, exist_ok=True)
os.makedirs(output_final, exist_ok=True)

# ── Discover year directories ────────────────────────────────────────────────
if not os.path.isdir(yearly_base):
    print(f"ERROR: {yearly_base} does not exist. Run the pipeline with --chunked first.")
    sys.exit(1)

year_dirs = sorted([
    d for d in os.listdir(yearly_base)
    if os.path.isdir(os.path.join(yearly_base, d)) and d.isdigit()
])

if not year_dirs:
    print(f"ERROR: No year directories found in {yearly_base}")
    sys.exit(1)

print(f"Found {len(year_dirs)} year directories: {', '.join(year_dirs)}")

# ── Aggregate critical parquets (simple concatenation) ───────────────────────
PARQUET_FILES = [
    'final_df_hourly.parquet',
    'final_df_blocks.parquet',
    'cohort_all_ids_w_outcome.parquet',
]

for parquet_name in PARQUET_FILES:
    frames = []
    for y in year_dirs:
        path = os.path.join(yearly_base, y, 'intermediate', parquet_name)
        if os.path.exists(path):
            df = pd.read_parquet(path)
            frames.append(df)
            print(f"  {y}/{parquet_name}: {len(df):,} rows")
        else:
            print(f"  WARNING: {path} not found — skipping year {y}")

    if frames:
        combined = pd.concat(frames, ignore_index=True)
        out_path = os.path.join(output_intermediate, parquet_name)
        combined.to_parquet(out_path, index=False)
        print(f"  => Aggregated {parquet_name}: {len(combined):,} rows from {len(frames)} years\n")
    else:
        print(f"  ERROR: No data found for {parquet_name}\n")

# ── Aggregate STROBE counts (sum across years) ──────────────────────────────
strobe_frames = []
for y in year_dirs:
    path = os.path.join(yearly_base, y, 'final', 'strobe_counts.csv')
    if os.path.exists(path):
        df = pd.read_csv(path)
        strobe_frames.append(df)

if strobe_frames:
    all_strobe = pd.concat(strobe_frames)
    summed = all_strobe.groupby('Metric')['Value'].sum().reset_index()
    out_path = os.path.join(output_final, 'strobe_counts.csv')
    summed.to_csv(out_path, index=False)
    print(f"Aggregated strobe_counts.csv: {len(summed)} metrics from {len(strobe_frames)} years")
else:
    print("WARNING: No strobe_counts.csv found in any year directory")

# ── Summary ──────────────────────────────────────────────────────────────────
print("\nAggregation complete.")
print(f"  Intermediate files: {output_intermediate}")
print(f"  Final files:        {output_final}")
print("\nNOTE: Summary CSVs (vitals, meds, respiratory support) are not aggregated.")
print("      They exist per-year in output/yearly/{year}/final/ for reference,")
print("      and the last year's versions are in output/final/.")
