# Eligibility for Mobilization

## Objective

The primary objective of this project is to determine the windows of opportunity for safely mobilizing patients on ventilators within the first 72 hours of first intubation, during business hours (8am-5pm). The analysis is guided by three established criteria sets, *Patel et al.*, and *TEAM Study*, as well as a consensus criteria approach, which includes Green, Yellow, and Red safety flags.

## Required CLIF tables and fields

Version: CLIF-2.1

### Core pipeline tables

1. **patient**: `patient_id`, `race_category`, `ethnicity_category`, `sex_category`, `death_dttm`
2. **hospitalization**: `patient_id`, `hospitalization_id`, `admission_dttm`, `discharge_dttm`, `discharge_category`, `age_at_admission`
3. **vitals**: `hospitalization_id`, `recorded_dttm`, `vital_category`, `vital_value`
   - `vital_category` = 'heart_rate', 'resp_rate', 'sbp', 'dbp', 'map', 'spo2', 'weight_kg', 'height_cm'
4. **labs**: `hospitalization_id`, `lab_result_dttm`, `lab_order_dttm`, `lab_category`, `lab_value`, `lab_value_numeric`
   - `lab_category` = 'lactate', 'creatinine', 'bilirubin_total', 'po2_arterial', 'platelet_count'
5. **medication_admin_continuous**: `hospitalization_id`, `admin_dttm`, `med_name`, `med_category`, `med_dose`, `med_dose_unit`, `med_group`
   - `med_category` = "norepinephrine", "epinephrine", "phenylephrine", "vasopressin", "dopamine", "angiotensin", "nicardipine", "nitroprusside", "clevidipine", "cisatracurium", "vecuronium", "rocuronium"
6. **respiratory_support**: `hospitalization_id`, `recorded_dttm`, `device_category`, `mode_category`, `tracheostomy`, `fio2_set`, `lpm_set`, `resp_rate_set`, `peep_set`, `resp_rate_obs`
7. **crrt_therapy**: `hospitalization_id`, `recorded_dttm`

### ASE (Adult Sepsis Event) calculation tables

8. **microbiology_culture**: `hospitalization_id`, `collect_dttm`, `fluid_category`
   - `fluid_category` = 'blood_buffy'
9. **medication_admin_intermittent**: `hospitalization_id`, `admin_dttm`, `med_category`, `med_route_category`, `med_group`
   - `med_group` = 'CMS_sepsis_qualifying_antibiotics'
10. **adt**: `hospitalization_id`, `in_dttm`, `out_dttm`, `location_category`
11. **hospital_diagnosis**: `hospitalization_id`, `diagnosis_code`

## Cohort Identification

The study period is from January 1, 2018, to December 31, 2024. The cohort consists of patients who were placed on invasive ventilation at any point during their hospitalization within this time period. Encounters that were intubated for less than 4 hours were excluded. Additionally, encounters that received tracheostomy or were receiving any paralytic drug were considered ineligible to be mobilised for that hour.

## Configuration

1. Navigate to the `config/` directory.
2. Rename `config_template.json` to `config.json`.
3. Update the `config.json` with site-specific settings.

## Pipeline execution

The full analysis pipeline is executed via `run_pipeline.sh` (macOS/Linux) or `run_pipeline.bat` (Windows). Both use [uv](https://docs.astral.sh/uv/) for dependency management.

### Prerequisites

- **Python 3.11+** with [uv](https://docs.astral.sh/uv/getting-started/installation/) installed
- **R 4.x** with packages: `cmprsk`, `data.table`, `dplyr`, `ggplot2`, `writexl`, `jsonlite`, `arrow`
- CLIF data tables in the format specified by `config.json`

### Running the pipeline

**macOS / Linux:**
```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

**Windows (Command Prompt):**
```bat
run_pipeline.bat
```

Both scripts:
- Sync Python dependencies via `uv sync` before running
- Run all 4 pipeline steps sequentially, logging each to `code/logs/`
- Skip R steps gracefully if `Rscript` is not found on PATH
- Print a summary of passed/failed steps at the end

> **Windows note:** Ensure `uv` and `Rscript` are on your system PATH. If R is installed but `Rscript` is not recognized, add `C:\Program Files\R\R-4.x.x\bin` to your PATH environment variable.

### Pipeline steps

| Step | Script | Language | Description |
|------|--------|----------|-------------|
| 1 | `01_cohort_identification.py` | Python | Cohort identification, STROBE diagram, hourly scaffold |
| 2 | `02_mobilization_analysis.py` | Python | Eligibility criteria, TableOne, failure analysis, sensitivity |
| 3 | `03_combined_analysis.R` | R | CIF curves, Fine-Gray SHR, stacked sensitivity CIF/SHR |
| 4 | `04_sensitivity_forest_plots.R` | R | Sensitivity forest plots |


## Project structure

```
├── code/
│   ├── 01_cohort_identification.py   # Step 1: Cohort + STROBE
│   ├── 02_mobilization_analysis.py   # Step 2: Criteria + analysis
│   ├── 03_combined_analysis.R               # Step 3: CIF + Fine-Gray
│   ├── 04_sensitivity_forest_plots.R           # Step 4: Forest plots
│   ├── pyCLIF.py                            # CLIF data loading utilities
│   └── sofa_score.py                        # SOFA score (1997 definition)
├── config/
│   ├── config_template.json                 # Site config template
│   └── outlier_config.json                  # Outlier thresholds
├── run_pipeline.sh                          # Pipeline runner (macOS/Linux)
├── run_pipeline.bat                         # Pipeline runner (Windows)
└── pyproject.toml                           # Python dependencies (uv)
```

## References

1. Patel BK, Wolfe KS, Patel SB, Dugan KC, Esbrook CL, Pawlik AJ, Stulberg M, Kemple C, Teele M, Zeleny E, Hedeker D, Pohlman AS, Arora VM, Hall JB, Kress JP. Effect of early mobilisation on long-term cognitive impairment in critical illness in the USA: a randomised controlled trial. Lancet Respir Med. 2023 Jun;11(6):563-572. doi: 10.1016/S2213-2600(22)00489-1. Epub 2023 Jan 21. [Link](https://pubmed.ncbi.nlm.nih.gov/36693400/)
2. The TEAM Study Investigators and the ANZICS Clinical Trials Group. Early Active Mobilization during Mechanical Ventilation in the ICU. N Engl J Med. 2022 Oct 26;387(19):1747-1758. doi: 10.1056/NEJMoa2209083. [Link](https://www.nejm.org/doi/full/10.1056/NEJMoa2209083)
3. Hodgson CL, Stiller K, Needham DM, Tipping CJ, Harrold M, Baldwin CE, Bradley S, Berney S, Caruana LR, Elliott D, Green M, Haines K, Higgins AM, Kaukonen KM, Leditschke IA, Nickels MR, Paratz J, Patman S, Skinner EH, Young PJ, Zanni JM, Denehy L, Webb SA. Expert consensus and recommendations on safety criteria for active mobilization of mechanically ventilated critically ill adults. Crit Care. 2014 Dec 4;18(6):658. doi: 10.1186/s13054-014-0658-y. [Link](https://pubmed.ncbi.nlm.nih.gov/25475522/)
4. Ohbe H, Jo T, Matsui H, Fushimi K, Yasunaga H. Differences in effect of early enteral nutrition on mortality among ventilated adults with shock requiring low-, medium-, and high-dose noradrenaline: A propensity-matched analysis. Clin Nutr. 2020 Feb;39(2):460-467. doi: 10.1016/j.clnu.2019.02.020. [Link](https://pubmed.ncbi.nlm.nih.gov/30808573/)
5. Goradia S, Sardaneh AA, Narayan SW, Penm J, Patanwala AE. Vasopressor dose equivalence: A scoping review and suggested formula. J Crit Care. 2021 Feb;61:233-240. doi: 10.1016/j.jcrc.2020.11.002. [Link](https://pubmed.ncbi.nlm.nih.gov/33220576/)
