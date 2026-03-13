# CHANGELOG

## Version 1 – October 30, 2024

Initial implementation.

---

## Version 2 – February 10, 2025

- Extracted `discharge_dttm` and `death_dttm`. Every patient must have one; assume discharged alive if not dead.
- Created flags for paralytics. Instead of excluding patients, we now exclude only the hours when paralytics were active.
- Updated exclusion: exclude patients intubated for **< 4 hours** (previously 2 hours).
- Added stitching logic for encounters across multiple hospitals at the same site.
- Extended analysis to **competing risk** framework:
  - **Event 1**: Became eligible  
  - **Event 2**: Died  
  - **Event 3**: Discharged alive

---

## Version 3 – March 12, 2025

- Implemented automatic UTC → local time conversion using config file.
- No manual datetime adjustments required in downstream scripts.

---

## Version 4 – March 20, 2025

- Eligibility now only begins during **business hours**, enabling true time-to-eligibility measurement.
- Full hospitalization is now the input for **survival analysis**.
- Updated `.qmd` script for competing risk analysis and validated against UMN results.

---

## Version 5 – April 15, 2025

- Fixed lactate carry-forward bug: values were incorrectly filtered to the first 3 days.
- Now lactate carry-forward times out after **24 hours**.
- For TEAM: eligible if lactate ≤ 4 or missing.
- `team_ne` flag is 1 if **no NE administered** (no more NA).
- Added SOFA score calculation using the original **1997 definition**.

---

## Version 6 – April 17, 2025

- Default criteria flags to **1 (eligible)** for NAs, unless a lower threshold is defined.
- Cases where **NAs are not eligible**:
  - **Red**: NE > 0.3
  - **Yellow**: MAP ≥ 65 and NE in [0.1–0.3]
- General rule:  
  - If a **minimum threshold** exists → NA = not eligible  
  - If **no threshold** → NA = eligible

---

## Version 7 – April 23, 2025

- Integrated updated SOFA logic from `sofa_score.py`.
- Refactored waterfall logic to match **Nick Ingraham's R version**.

---

## Version 8 – April 28, 2025

- Updated outlier threshold config to include 0 as a **valid lower bound**.
- Red and paralytic **med flags set to 1 only if dose > 0**.
- Pressor summary: use **last recorded value** per hour (not max).
- Added **CRRT flag**: if CRRT administered within `[start_dttm - 72hrs, end_dttm]`, renal SOFA score = 4.

---

## Version 9 – May 1, 2025

- Reinforced **24-hour lactate timeout**.
- Final outcome set to **dead** if discharge category includes **both "Hospice" and "Expired"**.
- Updated TEAM criteria to use **respiratory rate from vitals**, not respiratory support. Same as the other two criteria.
- Pressor value logic now **always uses last recorded value** for the hour.
- The sequence of hours starts with the **first hour of intubation** to the last recorded hour for vitals, labs, and medications.

## Version 10 – May 5, 2025
- Updated the Chicago/Patel and consensus criteria to use Average MAP instead of the min and max value of MAP.
- Removed deprecated survival analysis code using Kaplan-Meier, superseded by competing risk framework on full patient timeline in 03 R script.
- Added code to look at aggregates for the first 72 hours of intubation, and an additional TableOne for the first 72 hours. 
- Sensitivity analysis for the weekend vs weekday eligibility still pending. 

## Version 11- June 18, 2025
- Extending the analysis beyond 2020-2022. Run the code on the entire CLIF database at each site. 
- Weekday Sensitivity Analysis: 
  - Added Weekday Detection Logic in (02 script)
  - Created Weekday-Only Business Hours Flags (02 script)
  - Updated Competing Risk Dataset Generation (02 script)
  - Generate Weekday Analysis (03 script)
- Fill values of fio2_set if the device is nasal cannula, and lpm_set is available. Follow [this mapping](https://www.respiratorytherapyzone.com/oxygen-flow-rate-fio2/)
- Created a patient facing dashboard for this cohort, specifically for mobilization criteria. 

---

## Version 12 – February 22, 2026

1. Marimo migration 
- Migrated all analysis notebooks from Jupyter (`.ipynb`) to **marimo** (`.py`).
- New `run_pipeline.sh` (macOS/Linux) and `run_pipeline.bat` (Windows) using **uv** for dependency management.
- Pipeline: `01_cohort_identification.py` → `02_mobilization_analysis.py` → `03_combined_analysis.R` → `04_sensitivity_forest_plots.R`.
- Archived old Jupyter notebooks, `.py` scripts, and `run_project.sh`/`.bat`.

2. Fill strategy overhaul
- Implemented **slice-first** fill: 72h slice → forward-fill only (no back-fill). Prevents cross-boundary contamination.
- Dual datasets: `final_df` (72h primary, ffill-only) and `final_df_all` (all hours for competing risk / legacy).
- Grouped `ffill()` — always `.groupby('encounter_block').ffill()` 

3. Stacked sensitivity analysis (4 definitions × 2 cohorts)
- **Original cohort (IMV ≥4h)**: 1h any-day, 1h weekday-only, 4h-continuous any-day, 4h-continuous weekday.
- **IMV ≥24h subcohort**: same 4 definitions.
- Diamond partial order validation (1h_weekday and 4h_anyday are incomparable — different restriction dimensions).
- Total: 32 sensitivity datasets (4 criteria × 8 definitions).

4. Unified R analysis
- Merged `03_competing_risk_analysis.R` and `05_sensitivity_competing_risk.R` into single `03_combined_analysis.R`.
- Part A: Main CIF + Fine-Gray (full / 72h / weekday-only).
- Part B: Stacked sensitivity CIF, median times, subdistribution hazard ratios, forest plots.

5. Other
- 6-group extubation curve (split extubated into eligible/not-eligible).
- Time-to-eligibility windows at [12, 24, 36, 48, 60, 72]h.

6. Bug fixes
- Fixed FiO2 double-division bug in `pyCLIF.refill_fio2()` 
- Fixed R SHR: pass outcome variable directly to `crr()` instead of collapsing competing events.
- Fixed combination analysis pandas compat error (`only 0-dimensional arrays can be converted to Python scalars`).
- Vectorized `_pick_outcome` in `create_competing_risk_dataset` — replaced `.apply()` with `np.where()`.
- BMI 30-day time window constraint added.

## Version 13 – March 3, 2026

### Memory optimization

Sites with ≤32GB RAM hit an OOM error during the vitals pivot in `01_cohort_identification.py`. Three changes reduce peak memory usage — **no logic or output changes**.

1. **Chunked vitals pivot** — instead of pivoting the entire `_vitals_min_max` DataFrame at once (which creates a large dense intermediate), the pivot now processes 500 encounter_blocks per batch and concatenates the results. `_vitals_stitched` is freed immediately after aggregation.

2. **Consolidated med pivot** — replaced 4 separate `pivot_table` calls (min/max/first/last) + a 4-way merge with a single `pivot_table` over all 4 value columns. Produces identical column names (`min_norepinephrine`, `max_norepinephrine`, etc.).

3. **Explicit memory cleanup** — added `del` + `gc.collect()` calls after large intermediates (`_vitals_stitched`, `_vitals_min_max`, `_dose_agg`, `_ne_df`) are no longer needed.


## Version 14 – March 13, 2026

### DuckDB migration for memory + speed

Sites with limited RAM (Windows, ≤32GB) were OOM-ing during heavy pandas operations. Migrated three bottleneck operations from pandas to DuckDB SQL, with a generic batching wrapper for memory safety.

1. **Vitals pivot** — replaced chunked pandas `groupby → pivot_table → concat → flatten → rename` (40 lines) with a single DuckDB conditional aggregation query. 

2. **Hourly scaffold generation** — replaced `groupby().apply(pd.date_range)` (Python loop per block) with DuckDB `generate_series() + UNNEST`. 

3. **Hourly vent aggregation** — replaced pandas `groupby().agg()` with DuckDB SQL aggregation. 

4. **Meds pivot** — replaced pandas `groupby + pivot_table` with DuckDB conditional aggregation. Uses `ARG_MIN`/`ARG_MAX` with `admin_dttm` for deterministic first/last dose ordering (previously depended on arbitrary row order).

5. **Batching wrapper** (`pyCLIF.run_duckdb_batched`) — generic utility that splits DataFrames by `encounter_block` and processes in configurable batches. Batch size set via `config.json` key `duckdb_batch_size` (default 500). Only activates for datasets >10M rows.

6. Removed unused module-level `duckdb.connect()` in `pyCLIF.py`.

**Output changes**: `ne_calc_first`/`ne_calc_last` now use timestamp-ordered first/last (via `ARG_MIN`/`ARG_MAX` on `admin_dttm`) instead of arbitrary row-order first/last. Min/max values are identical.

---