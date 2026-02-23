## Code directory

This directory contains the analysis scripts. Run via `run_pipeline.sh` (or `run_pipeline.bat` on Windows) from the project root, or execute manually:

### Pipeline order

1. `01_cohort_identification_marimo.py` — Cohort identification, STROBE diagram, hourly scaffold
2. `02_mobilization_analysis_marimo.py` — Eligibility criteria, TableOne, failure analysis, stacked sensitivity
3. `03_combined_analysis.R` — CIF curves, Fine-Gray SHR, stacked sensitivity CIF/SHR, federation outputs
4. `sensitivity_forest_plots.R` — Sensitivity forest plots

### Utility modules

- `pyCLIF.py` — CLIF data loading and site configuration
- `sofa_score.py` — SOFA score calculation (1997 definition)
- `waterfall.py` — Eligibility criteria logic

### Running manually

```bash
cd code
uv sync
uv run python 01_cohort_identification_marimo.py
uv run python 02_mobilization_analysis_marimo.py
Rscript 03_combined_analysis.R
Rscript sensitivity_forest_plots.R
```

Upload results from `output/final/` to the project box folder.
