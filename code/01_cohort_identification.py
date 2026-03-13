import marimo

__generated_with = "0.14.17"
app = marimo.App(width="full")


@app.cell
def _(mo):
    mo.md(r"""# Eligibility for Mobilization: Cohort Identification""")
    return


@app.cell
def imports():
    import pandas as pd
    import numpy as np
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle, FancyArrowPatch
    # Force white backgrounds regardless of marimo/system dark theme
    matplotlib.rcParams.update({
        'figure.facecolor': 'white',
        'axes.facecolor': 'white',
        'savefig.facecolor': 'white',
        'axes.edgecolor': 'black',
        'axes.labelcolor': 'black',
        'xtick.color': 'black',
        'ytick.color': 'black',
        'text.color': 'black',
    })
    import os
    import sys
    import shutil
    from datetime import datetime, timedelta
    import gc
    import json
    import pyarrow
    import warnings
    warnings.filterwarnings('ignore')
    import pyCLIF
    import sofa_score
    from clifpy.tables.respiratory_support import RespiratorySupport
    import marimo as mo
    return (
        FancyArrowPatch,
        Rectangle,
        RespiratorySupport,
        gc,
        json,
        mo,
        np,
        os,
        pd,
        plt,
        pyCLIF,
        shutil,
        sofa_score,
        timedelta,
        warnings,
    )


@app.cell
def setup_logger(os, pyCLIF, output_folder):
    import logging

    _log_dir = f'{output_folder}/final'
    os.makedirs(_log_dir, exist_ok=True)

    _logger = logging.getLogger('clif_01')
    _logger.setLevel(logging.INFO)
    _logger.handlers.clear()

    _fh = logging.FileHandler(
        f'{_log_dir}/{pyCLIF.helper["site_name"]}_01_cohort_log.txt', mode='w'
    )
    _fh.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    _logger.addHandler(_fh)

    _ch = logging.StreamHandler()
    _ch.setFormatter(logging.Formatter('%(message)s'))
    _logger.addHandler(_ch)

    def log(*args, **kwargs):
        _msg = ' '.join(str(a) for a in args)
        _logger.info(_msg)

    log(f"=== CLIF Pipeline 01: Cohort Identification ===")
    log(f"Site: {pyCLIF.helper['site_name']}")

    return (log,)



@app.cell
def config(json, os, pyCLIF):
    _config_path = os.path.join(pyCLIF.project_root, 'config', 'outlier_config.json')
    with open(_config_path, 'r', encoding='utf-8') as _f:
        outlier_cfg = json.load(_f)
    return (outlier_cfg,)


@app.cell
def output_folders(os, pyCLIF, shutil):
    # NOTE: Uses print() not log() — logger is created AFTER this cell
    # to ensure the log file points to the fresh output directory.
    print("=== Output Folder Management ===")

    output_folder = f'{pyCLIF.project_root}/output'
    _output_old_folder = f'{pyCLIF.project_root}/output_old'

    # Check if output folder exists
    if os.path.exists(output_folder):
        print(f"Existing output folder found: {output_folder}")

        # If output_old already exists, remove it first
        if os.path.exists(_output_old_folder):
            print(f"Removing existing output_old folder...")
            shutil.rmtree(_output_old_folder)

        # Rename current output to output_old
        print(f"Renaming {output_folder} -> {_output_old_folder}")
        os.rename(output_folder, _output_old_folder)

        # Log what was backed up
        if os.path.exists(_output_old_folder):
            _backup_size = sum(
                os.path.getsize(os.path.join(_dirpath, _filename))
                for _dirpath, _dirnames, _filenames in os.walk(_output_old_folder)
                for _filename in _filenames
            ) / (1024 * 1024)
            print(f"Backup created: {_backup_size:.1f} MB")

    # Create fresh output directory structure
    print(f"Creating fresh output directory structure...")
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(f'{output_folder}/final', exist_ok=True)
    os.makedirs(f'{output_folder}/intermediate', exist_ok=True)
    # Create empty output files
    with open(f'{output_folder}/final/final_output.txt', 'w') as _f:
        pass
    with open(f'{output_folder}/intermediate/intermediate.txt', 'w') as _f:
        pass

    # Create graphs subfolder
    graphs_folder = f'{output_folder}/final/graphs'
    os.makedirs(graphs_folder, exist_ok=True)

    print(f"Output directory structure ready:")
    print(f"   {output_folder}/")
    print(f"   +-- final/")
    print(f"   |   +-- graphs/")
    print(f"   +-- intermediate/")
    return (output_folder,)


@app.cell
def _(mo):
    mo.md(r"""## Required Columns and Categories""")
    return


@app.cell
def constants():
    rst_required_columns = [
        'hospitalization_id',
        'recorded_dttm',
        'device_name',
        'device_category',
        'mode_name',
        'mode_category',
        'tracheostomy',
        'fio2_set',
        'lpm_set',
        'resp_rate_set',
        'peep_set',
        'resp_rate_obs',
        'tidal_volume_set',
        'pressure_control_set',
        'pressure_support_set',
        'peak_inspiratory_pressure_set'
    ]

    vitals_required_columns = [
        'hospitalization_id',
        'recorded_dttm',
        'vital_category',
        'vital_value'
    ]
    vitals_of_interest = ['heart_rate', 'respiratory_rate', 'sbp', 'dbp', 'map', 'spo2', 'weight_kg', 'height_cm']

    labs_required_columns = [
        'hospitalization_id',
        'lab_result_dttm',
        'lab_category',
        'lab_value',
        'lab_value_numeric'
    ]
    labs_of_interest = ['lactate']

    meds_required_columns = [
        'hospitalization_id',
        'admin_dttm',
        'med_name',
        'med_category',
        'med_dose',
        'med_dose_unit'
    ]
    meds_of_interest = [
        'norepinephrine', 'epinephrine', 'phenylephrine', 'vasopressin',
        'dopamine', 'angiotensin', 'nicardipine', 'nitroprusside',
        'clevidipine', 'cisatracurium', 'vecuronium', 'rocuronium'
    ]
    return (
        labs_of_interest,
        labs_required_columns,
        meds_of_interest,
        meds_required_columns,
        rst_required_columns,
        vitals_of_interest,
        vitals_required_columns,
    )


@app.cell
def _(mo):
    mo.md(r"""## Load Data""")
    return


@app.cell
def load_tables(log, pyCLIF):
    _patient = pyCLIF.load_data('clif_patient')
    _hospitalization = pyCLIF.load_data('clif_hospitalization')
    _adt = pyCLIF.load_data('clif_adt')

    # ensure id variable is of dtype character
    _hospitalization['hospitalization_id'] = _hospitalization['hospitalization_id'].astype(str)
    _patient['patient_id'] = _patient['patient_id'].astype(str)
    _adt['hospitalization_id'] = _adt['hospitalization_id'].astype(str)

    # Duplicate check
    patient = pyCLIF.remove_duplicates(_patient, ['patient_id'], 'patient')
    hospitalization = pyCLIF.remove_duplicates(_hospitalization, ['hospitalization_id'], 'hospitalization')
    adt = pyCLIF.remove_duplicates(_adt, ['hospitalization_id', 'hospital_id', 'in_dttm'], 'adt')

    log(f"Total Number of unique encounters in the hospitalization table: {pyCLIF.count_unique_encounters(hospitalization, 'hospitalization_id')}")

    # Standardize all _dttm variables to the same format
    patient = pyCLIF.convert_datetime_columns_to_site_tz(patient, pyCLIF.helper['timezone'])
    hospitalization = pyCLIF.convert_datetime_columns_to_site_tz(hospitalization, pyCLIF.helper['timezone'])
    adt = pyCLIF.convert_datetime_columns_to_site_tz(adt, pyCLIF.helper['timezone'])
    return adt, hospitalization, patient


@app.cell
def _(mo):
    mo.md(r"""## Cohort Identification — (A) Date & Age Filter, (B) Stitch Hospitalizations""")
    return


@app.cell
def steps_a_b(adt, hospitalization, log, pyCLIF):
    # ── STEP A: Date and Age Filter ──
    log("\n=== STEP A: Filter by date range & age ===\n")
    _date_mask = (hospitalization['admission_dttm'] >= '2018-01-01') & \
                 (hospitalization['admission_dttm'] <= '2024-12-31')
    _age_mask = (hospitalization['age_at_admission'] >= 18)

    if pyCLIF.helper['site_name'].lower() == 'mimic':
        hospitalization_cohort = hospitalization[_age_mask].copy()
    else:
        hospitalization_cohort = hospitalization[_date_mask & _age_mask].copy()

    _strobe_ab = {}
    _strobe_ab['A_after_date_age_filter'] = hospitalization_cohort['hospitalization_id'].nunique()
    log(f"Number of unique hospitalizations after date & age filter: {_strobe_ab['A_after_date_age_filter']}")

    # Get total unique hospitalizations without time filter, only age filter
    _age_mask_all = (hospitalization['age_at_admission'] >= 18)
    _total_adult_hospitalizations = hospitalization[_age_mask_all]['hospitalization_id'].nunique()
    _strobe_ab['A_after_age_filter'] = _total_adult_hospitalizations
    log(f"\nTotal number of unique adult hospitalizations (no date filter): {_total_adult_hospitalizations}")

    # ── STEP B: Stitch hospitalizations ──
    _cohort_ids = hospitalization_cohort['hospitalization_id'].unique().tolist()
    _adt_cohort = adt[adt['hospitalization_id'].isin(_cohort_ids)].copy()

    log("\nMissing values in admission_dttm:", hospitalization_cohort['admission_dttm'].isna().sum())
    log("Missing values in discharge_dttm:", hospitalization_cohort['discharge_dttm'].isna().sum())

    log("\n=== STEP B: Stitch encounters ===\n")
    _stitched_cohort = pyCLIF.stitch_encounters(hospitalization_cohort, _adt_cohort, time_interval=6)

    _stitched_unique = _stitched_cohort[['patient_id', 'encounter_block']].drop_duplicates()

    _strobe_ab['B_before_stitching'] = _stitched_cohort['hospitalization_id'].nunique()
    _strobe_ab['B_after_stitching'] = _stitched_unique['encounter_block'].nunique()
    _strobe_ab['B_stitched_hosp_ids'] = _strobe_ab['B_before_stitching'] - _strobe_ab['B_after_stitching']
    log(f"Number of unique hospitalizations before stitching: {_stitched_cohort['hospitalization_id'].nunique()}")
    log(f"Number of unique encounter blocks after stitching: {_strobe_ab['B_after_stitching']}")
    log(f"Number of linked hospitalization ids: {_strobe_ab['B_before_stitching'] - _strobe_ab['B_after_stitching']}")

    # Mapping of patient id, hospitalization id and encounter blocks
    all_ids_base = _stitched_cohort[['patient_id', 'hospitalization_id', 'encounter_block', 'discharge_category', 'discharge_dttm']].drop_duplicates()
    log("\nUnique values in each column:")
    for _col in all_ids_base.columns[:3]:
        log(f"\n{_col}:")
        log(all_ids_base[_col].nunique())

    strobe_ab = _strobe_ab
    return all_ids_base, strobe_ab


@app.cell
def _(mo):
    mo.md(r"""#### (C) Identify Ventilator Usage""")
    return


@app.cell
def step_c(
    RespiratorySupport,
    all_ids_base,
    log,
    outlier_cfg,
    pd,
    pyCLIF,
    rst_required_columns,
):
    log("\n=== STEP C: Load & process respiratory support => Apply Waterfall & Identify IMV usage ===\n")

    # 1) Load respiratory support
    _resp_support_raw = pyCLIF.load_data(
        'clif_respiratory_support',
        columns=rst_required_columns,
        filters={'hospitalization_id': all_ids_base['hospitalization_id'].unique().tolist()}
    )

    _resp_support = _resp_support_raw.copy()
    _resp_support['device_category'] = _resp_support['device_category'].str.lower()
    _resp_support['mode_category'] = _resp_support['mode_category'].str.lower()
    _resp_support['lpm_set'] = pd.to_numeric(_resp_support['lpm_set'], errors='coerce')
    _resp_support['resp_rate_set'] = pd.to_numeric(_resp_support['resp_rate_set'], errors='coerce')
    _resp_support['peep_set'] = pd.to_numeric(_resp_support['peep_set'], errors='coerce')
    _resp_support['resp_rate_obs'] = pd.to_numeric(_resp_support['resp_rate_obs'], errors='coerce')
    _resp_support = _resp_support.sort_values(['hospitalization_id', 'recorded_dttm'])

    log("\n=== Apply outlier thresholds ===\n")
    _resp_support['fio2_set'] = pd.to_numeric(_resp_support['fio2_set'], errors='coerce')
    _fio2_mean = _resp_support['fio2_set'].mean(skipna=True)
    if _fio2_mean and _fio2_mean > 1.0:
        _resp_support.loc[_resp_support['fio2_set'] > 1, 'fio2_set'] = \
            _resp_support.loc[_resp_support['fio2_set'] > 1, 'fio2_set'] / 100
        log("Updated fio2_set to be between 0.21 and 1")
    else:
        log("FIO2_SET mean=", _fio2_mean, "is within the required range")

    # ── Respiratory Support Summary ──
    _results_list = []
    _group_cols = 'device_category'
    _numeric_cols = ['fio2_set', 'peep_set', 'lpm_set', 'resp_rate_set', 'resp_rate_obs']

    for _col in _numeric_cols:
        _tmp = pyCLIF.create_summary_table(
            df=_resp_support,
            numeric_col=_col,
            group_by_cols=_group_cols
        )
        _tmp['variable'] = _col
        if isinstance(_group_cols, str):
            _group_cols_list = [_group_cols]
        else:
            _group_cols_list = _group_cols
        _front_cols = _group_cols_list + ['variable']
        _rest_cols = [c for c in _tmp.columns if c not in _front_cols]
        _new_cols = _front_cols + _rest_cols
        _tmp = _tmp[_new_cols]
        _results_list.append(_tmp)

    _final_summary_resp_support = pd.concat(_results_list, ignore_index=True)
    _final_summary_resp_support.to_csv(f'{pyCLIF.project_root}/output/final/summary_respiratory_support_by_device.csv', index=False)

    _results_list = []
    _group_cols = ['device_category', 'mode_category']
    _numeric_cols = ['fio2_set', 'peep_set', 'lpm_set', 'resp_rate_set', 'resp_rate_obs']

    for _col in _numeric_cols:
        _tmp = pyCLIF.create_summary_table(
            df=_resp_support,
            numeric_col=_col,
            group_by_cols=_group_cols
        )
        _tmp['variable'] = _col
        if isinstance(_group_cols, str):
            _group_cols_list = [_group_cols]
        else:
            _group_cols_list = _group_cols
        _front_cols = _group_cols_list + ['variable']
        _rest_cols = [c for c in _tmp.columns if c not in _front_cols]
        _new_cols = _front_cols + _rest_cols
        _tmp = _tmp[_new_cols]
        _results_list.append(_tmp)

    _final_summary_resp_support = pd.concat(_results_list, ignore_index=True)
    _final_summary_resp_support.to_csv(f'{pyCLIF.project_root}/output/final/summary_respiratory_support_by_device_mode.csv', index=False)

    # ── Waterfall ──
    _imv_mask = _resp_support['device_category'].str.contains("imv", case=False, na=False)
    _resp_stitched_imv_ids = _resp_support[_imv_mask][['hospitalization_id']].drop_duplicates()
    _resp_support_filtered = _resp_support[
        _resp_support["hospitalization_id"].isin(_resp_stitched_imv_ids["hospitalization_id"])
    ].reset_index(drop=True)

    # filter all_ids_base to only those with IMV
    _all_ids = all_ids_base[all_ids_base['hospitalization_id'].isin(_resp_support_filtered['hospitalization_id'].unique())].copy()

    # Convert to site tz BEFORE waterfall (handles naive Databricks exports + UTC parquet)
    _resp_support_filtered = pyCLIF.convert_datetime_columns_to_site_tz(_resp_support_filtered, pyCLIF.helper['timezone'])

    # _rs = RespiratorySupport(data=_resp_support_filtered)
    # _processed_resp_support = _rs.waterfall(id_col="hospitalization_id", verbose=True, return_dataframe=True)
    # # Re-convert after waterfall (waterfall preserves input tz; idempotent if already correct)
    # _processed_resp_support = pyCLIF.convert_datetime_columns_to_site_tz(_processed_resp_support, pyCLIF.helper['timezone'])
    # _processed_resp_support.to_parquet(f'{pyCLIF.project_root}/output/intermediate/processed_resp_support.parquet', index=False)
    _waterfall_path = f'{pyCLIF.project_root}/waterfall/processed_resp_support.parquet'
    if os.path.exists(_waterfall_path):
        _processed_resp_support = pd.read_parquet(_waterfall_path)
        _processed_resp_support = pyCLIF.convert_datetime_columns_to_site_tz(_processed_resp_support, pyCLIF.helper['timezone'])
    else:
        _rs = RespiratorySupport(data=_resp_support_filtered)
        _processed_resp_support = _rs.waterfall(id_col="hospitalization_id", verbose=True, return_dataframe=True)
        # Re-convert after waterfall (waterfall preserves input tz; idempotent if already correct)
        _processed_resp_support = pyCLIF.convert_datetime_columns_to_site_tz(_processed_resp_support, pyCLIF.helper['timezone'])
        _processed_resp_support.to_parquet(f'{pyCLIF.project_root}/output/intermediate/processed_resp_support.parquet', index=False)
    
    # Merge to get encounter_block
    _resp_stitched = _processed_resp_support.merge(
        _all_ids[['hospitalization_id', 'encounter_block']],
        on='hospitalization_id', how='right'
    )

    log("Missing values in recorded_dttm:", _resp_stitched['recorded_dttm'].isna().sum())

    pyCLIF.apply_outlier_thresholds(_resp_stitched, 'fio2_set', *outlier_cfg['fio2_set'])
    pyCLIF.apply_outlier_thresholds(_resp_stitched, 'peep_set', *outlier_cfg['peep_set'])
    pyCLIF.apply_outlier_thresholds(_resp_stitched, 'lpm_set', *outlier_cfg['lpm_set'])
    pyCLIF.apply_outlier_thresholds(_resp_stitched, 'resp_rate_set', *outlier_cfg['resp_rate_set'])
    pyCLIF.apply_outlier_thresholds(_resp_stitched, 'resp_rate_obs', *outlier_cfg['resp_rate_obs'])

    # fill values of fio2_set if the device is nasal cannula
    _resp_stitched = pyCLIF.impute_fio2_from_nasal_cannula_flow(_resp_stitched)

    # 4) Identify IMV
    _imv_mask2 = _resp_stitched['device_category'].str.contains("imv", case=False, na=False)
    resp_stitched_imv = _resp_stitched[_imv_mask2].copy()
    resp_stitched_imv['on_vent'] = 1

    # Left join back to full resp_stitched
    resp_stitched_final = _resp_stitched.merge(
        resp_stitched_imv[['hospitalization_id', 'recorded_dttm', 'on_vent']],
        on=['hospitalization_id', 'recorded_dttm'],
        how='left'
    )
    resp_stitched_final['on_vent'] = resp_stitched_final['on_vent'].fillna(0)

    strobe_c = {}
    strobe_c['C_imv_hospitalizations'] = resp_stitched_final['hospitalization_id'].nunique()
    strobe_c['C_imv_encounter_blocks'] = resp_stitched_final['encounter_block'].nunique()
    log(f"Total IMV respiratory support hospitalizations: {strobe_c['C_imv_hospitalizations']}")
    log(f"Total IMV respiratory support encounter blocks: {strobe_c['C_imv_encounter_blocks']}")

    all_ids_imv = _all_ids[_all_ids['encounter_block'].isin(resp_stitched_final['encounter_block'].unique())].copy()
    all_ids_imv = all_ids_imv[all_ids_imv['hospitalization_id'].isin(resp_stitched_final['hospitalization_id'].unique())].copy()

    for _col in all_ids_imv.columns[:3]:
        log(f"\n{_col}:")
        log(all_ids_imv[_col].nunique())

    return all_ids_imv, resp_stitched_final, resp_stitched_imv, strobe_c


@app.cell
def _(mo):
    mo.md(r"""#### (D) Vent Start and End Times""")
    return


@app.cell
def step_d(all_ids_imv, log, resp_stitched_imv):
    log("\n=== STEP D: Determine ventilation times (start/end) at encounter block level ===\n")

    # at the hospitalization id level
    _vent_start_end = resp_stitched_imv.groupby('hospitalization_id').agg(
        vent_start_time=('recorded_dttm', 'min'),
        vent_end_time=('recorded_dttm', 'max')
    ).reset_index()

    _check_same_vent_start_end = _vent_start_end[_vent_start_end['vent_start_time'] == _vent_start_end['vent_end_time']].copy()
    _vent_start_end = _vent_start_end[_vent_start_end['vent_start_time'] != _vent_start_end['vent_end_time']].copy()

    strobe_d = {}
    strobe_d['D_hospitalizations_with_valid_vent'] = _vent_start_end['hospitalization_id'].nunique()
    strobe_d['D_hospitalizations_with_same_vent_start_end'] = _check_same_vent_start_end['hospitalization_id'].nunique()
    log(f"Unique hospitalizations with valid IMV start/end: {strobe_d['D_hospitalizations_with_valid_vent']}")

    # at the block level
    block_vent_times = resp_stitched_imv.groupby('encounter_block', dropna=True).agg(
        block_vent_start_dttm=('recorded_dttm', 'min'),
        block_vent_end_dttm=('recorded_dttm', 'max')
    ).reset_index()

    _block_same_vent = block_vent_times[block_vent_times['block_vent_start_dttm'] == block_vent_times['block_vent_end_dttm']].copy()
    block_vent_times = block_vent_times[block_vent_times['block_vent_start_dttm'] != block_vent_times['block_vent_end_dttm']].copy()

    strobe_d['D_blocks_with_valid_vent'] = block_vent_times['encounter_block'].nunique()
    strobe_d['D_blocks_with_same_vent_start_end'] = _block_same_vent['encounter_block'].nunique()
    log(f"Unique encounter blocks with valid IMV start/end: {strobe_d['D_blocks_with_valid_vent']}")

    _valid_blocks_vent = block_vent_times['encounter_block'].unique()

    # Filter all_ids to only keep rows where encounter_block is in valid_blocks_vent
    all_ids_vent = all_ids_imv[all_ids_imv['encounter_block'].isin(_valid_blocks_vent)].copy()

    for _col in all_ids_vent.columns[:3]:
        log(f"\n{_col}:")
        log(all_ids_vent[_col].nunique())

    return all_ids_vent, block_vent_times, strobe_d


@app.cell
def _(mo):
    mo.md(r"""#### (E) Hourly Sequence""")
    return


@app.cell
def step_e_scaffold(
    all_ids_vent,
    block_vent_times,
    log,
    np,
    outlier_cfg,
    pd,
    pyCLIF,
    resp_stitched_final,
    timedelta,
    vitals_of_interest,
    vitals_required_columns,
    warnings,
):
    log("\n=== STEP E: Hourly sequence generation BLOCK level ===\n")

    # 1) Load vitals
    vitals_cohort = pyCLIF.load_data('clif_vitals',
        columns=vitals_required_columns,
        filters={'hospitalization_id': all_ids_vent['hospitalization_id'].unique().tolist(),
                 'vital_category': vitals_of_interest}
    )
    vitals_cohort = pyCLIF.convert_datetime_columns_to_site_tz(vitals_cohort, pyCLIF.helper['timezone'])
    vitals_cohort['vital_value'] = pd.to_numeric(vitals_cohort['vital_value'], errors='coerce')
    vitals_cohort = vitals_cohort.sort_values(['hospitalization_id', 'recorded_dttm'])

    # Replace outliers with NAs in the vitals table
    _min_hr, _max_hr = outlier_cfg['heart_rate']
    _min_rr, _max_rr = outlier_cfg['respiratory_rate']
    _min_sbp, _max_sbp = outlier_cfg['sbp']
    _min_dbp, _max_dbp = outlier_cfg['dbp']
    _min_map, _max_map = outlier_cfg['map']
    _min_spo2, _max_spo2 = outlier_cfg['spo2']
    _min_weight, _max_weight = outlier_cfg['weight_kg']
    _min_height, _max_height = outlier_cfg['height_cm']

    _is_hr = vitals_cohort['vital_category'] == 'heart_rate'
    vitals_cohort.loc[_is_hr & (vitals_cohort['vital_value'] < _min_hr), 'vital_value'] = np.nan
    vitals_cohort.loc[_is_hr & (vitals_cohort['vital_value'] > _max_hr), 'vital_value'] = np.nan

    _is_rr = vitals_cohort['vital_category'] == 'respiratory_rate'
    vitals_cohort.loc[_is_rr & (vitals_cohort['vital_value'] < _min_rr), 'vital_value'] = np.nan
    vitals_cohort.loc[_is_rr & (vitals_cohort['vital_value'] > _max_rr), 'vital_value'] = np.nan

    _is_sbp = vitals_cohort['vital_category'] == 'sbp'
    vitals_cohort.loc[_is_sbp & (vitals_cohort['vital_value'] < _min_sbp), 'vital_value'] = np.nan
    vitals_cohort.loc[_is_sbp & (vitals_cohort['vital_value'] > _max_sbp), 'vital_value'] = np.nan

    _is_dbp = vitals_cohort['vital_category'] == 'dbp'
    vitals_cohort.loc[_is_dbp & (vitals_cohort['vital_value'] < _min_dbp), 'vital_value'] = np.nan
    vitals_cohort.loc[_is_dbp & (vitals_cohort['vital_value'] > _max_dbp), 'vital_value'] = np.nan

    _is_map = vitals_cohort['vital_category'] == 'map'
    vitals_cohort.loc[_is_map & (vitals_cohort['vital_value'] < _min_map), 'vital_value'] = np.nan
    vitals_cohort.loc[_is_map & (vitals_cohort['vital_value'] > _max_map), 'vital_value'] = np.nan

    _is_spo2 = vitals_cohort['vital_category'] == 'spo2'
    vitals_cohort.loc[_is_spo2 & (vitals_cohort['vital_value'] < _min_spo2), 'vital_value'] = np.nan
    vitals_cohort.loc[_is_spo2 & (vitals_cohort['vital_value'] > _max_spo2), 'vital_value'] = np.nan

    _is_weight = vitals_cohort['vital_category'] == 'weight_kg'
    vitals_cohort.loc[_is_weight & (vitals_cohort['vital_value'] < _min_weight), 'vital_value'] = np.nan
    vitals_cohort.loc[_is_weight & (vitals_cohort['vital_value'] > _max_weight), 'vital_value'] = np.nan

    _is_height = vitals_cohort['vital_category'] == 'height_cm'
    vitals_cohort.loc[_is_height & (vitals_cohort['vital_value'] < _min_height), 'vital_value'] = np.nan
    vitals_cohort.loc[_is_height & (vitals_cohort['vital_value'] > _max_height), 'vital_value'] = np.nan

    _summary_vitals = pyCLIF.create_summary_table(
        df=vitals_cohort,
        numeric_col='vital_value',
        group_by_cols='vital_category'
    )
    _summary_vitals.to_csv(f'{pyCLIF.project_root}/output/final/summary_vitals_by_category.csv', index=False)

    # Merge to get encounter_block on each vital
    vitals_stitched = vitals_cohort.merge(all_ids_vent, on='hospitalization_id', how='left')
    # Group by block => find earliest & latest vital for that block
    _vital_bounds_block = vitals_stitched.groupby('encounter_block', dropna=True)['recorded_dttm'].agg(['min', 'max']).reset_index()
    _vital_bounds_block.columns = ['encounter_block', 'block_first_vital_dttm', 'block_last_vital_dttm']

    # 2) Merge block_vent_times with vital_bounds_block
    final_blocks = block_vent_times.merge(_vital_bounds_block, on='encounter_block', how='inner')

    # 3) Check for bad blocks
    _bad_block = final_blocks[final_blocks['block_last_vital_dttm'] < final_blocks['block_vent_start_dttm']]
    strobe_e = {}
    strobe_e['E_blocks_with_vent_end_before_vital_start'] = _bad_block['encounter_block'].nunique()
    if len(_bad_block) > 0:
        log("Warning: Some blocks have last vital < vent start:\n", len(_bad_block))
    else:
        log("There are no bad blocks! Good job CLIF-ing")

    # 4) Generate the hourly sequence at block level (DuckDB generate_series)
    import time as _time
    _t0 = _time.perf_counter()
    _hourly_seq_block = pyCLIF.generate_hourly_scaffold_duckdb(final_blocks)
    log(f"DuckDB hourly scaffold completed in {_time.perf_counter() - _t0:.1f}s")

    # 6) Combine with actual vent usage by hour
    _resp_stitched_final_local = resp_stitched_final[resp_stitched_final['encounter_block'].isin(all_ids_vent['encounter_block'])].copy()
    _resp_stitched_final_local['recorded_date'] = _resp_stitched_final_local['recorded_dttm'].dt.normalize().dt.tz_localize(None)
    _resp_stitched_final_local['recorded_hour'] = _resp_stitched_final_local['recorded_dttm'].dt.hour

    # Forward fill tracheostomy within each encounter_block BEFORE hourly aggregation
    log("Forward filling tracheostomy within encounter blocks...")
    _resp_stitched_final_local = _resp_stitched_final_local.sort_values(['encounter_block', 'recorded_dttm'])
    _resp_stitched_final_local['tracheostomy_filled'] = (
        _resp_stitched_final_local.groupby('encounter_block')['tracheostomy']
        .transform(lambda x: x.ffill())
    )
    _resp_stitched_final_local['tracheostomy_filled'] = _resp_stitched_final_local['tracheostomy_filled'].fillna(0)

    _before_blocks = _resp_stitched_final_local[_resp_stitched_final_local['tracheostomy'] == 1]['encounter_block'].nunique()
    _after_blocks = _resp_stitched_final_local[_resp_stitched_final_local['tracheostomy_filled'] == 1]['encounter_block'].nunique()
    log(f"Blocks with trach (before forward fill): {_before_blocks}")
    log(f"Blocks with trach (after forward fill): {_after_blocks}")

    _t0 = _time.perf_counter()
    _hourly_vent_block = pyCLIF.aggregate_hourly_vent_duckdb(_resp_stitched_final_local)
    log(f"DuckDB hourly vent aggregation completed in {_time.perf_counter() - _t0:.1f}s")

    # Sanity check
    _seq_blocks = set(_hourly_seq_block['encounter_block'].unique())
    _vent_blocks = set(_hourly_vent_block['encounter_block'].unique())
    _blocks_in_seq_not_vent = _seq_blocks - _vent_blocks
    _blocks_in_vent_not_seq = _vent_blocks - _seq_blocks
    log("Blocks in hourly_seq_block but not in hourly_vent_block:", len(_blocks_in_seq_not_vent))
    if len(_blocks_in_seq_not_vent) > 0:
        log(sorted(list(_blocks_in_seq_not_vent)))
    log("\nBlocks in hourly_vent_block but not in hourly_seq_block:", len(_blocks_in_vent_not_seq))

    # Step 1: Reconstruct timestamps
    _hourly_seq_block['recorded_dttm'] = pd.to_datetime(_hourly_seq_block['recorded_date']) + pd.to_timedelta(_hourly_seq_block['recorded_hour'], unit='h')
    _hourly_vent_block['recorded_dttm'] = pd.to_datetime(_hourly_vent_block['recorded_date']) + pd.to_timedelta(_hourly_vent_block['recorded_hour'], unit='h')

    # Step 2: Get max scaffold time per encounter
    _max_times = (
        _hourly_seq_block.groupby('encounter_block')['recorded_dttm']
        .max().reset_index()
        .rename(columns={'recorded_dttm': 'max_seq_dttm'})
    )

    # Step 3: Identify extra vent rows beyond scaffold
    _vent_plus_max = pd.merge(_hourly_vent_block, _max_times, on='encounter_block', how='left')
    _extra_rows = _vent_plus_max[
        _vent_plus_max['recorded_dttm'] > _vent_plus_max['max_seq_dttm']
    ].copy()

    # Step 4: Create gap-filler rows (O(1) dict lookup instead of O(N) scan)
    _max_times_dict = dict(zip(_max_times['encounter_block'], pd.to_datetime(_max_times['max_seq_dttm'])))
    _gap_rows = []
    for _enc_id, _group in _extra_rows.groupby('encounter_block'):
        _max_time = _max_times_dict[_enc_id]
        _first_extra_time = _group['recorded_dttm'].min()

        if _first_extra_time <= _max_time + timedelta(hours=1):
            continue

        _gap_times = pd.date_range(
            start=_max_time + timedelta(hours=1),
            end=_first_extra_time - timedelta(hours=1),
            freq='H'
        )

        for _dt in _gap_times:
            _gap_rows.append({
                'encounter_block': _enc_id,
                'recorded_date': _dt.normalize().replace(tzinfo=None),
                'recorded_hour': _dt.hour,
                'recorded_dttm': _dt
            })

    _gap_df = pd.DataFrame(_gap_rows)

    # Step 5: Add all required columns to gap_df
    _missing_cols = set(_hourly_vent_block.columns) - set(_gap_df.columns)
    for _c in _missing_cols:
        _gap_df[_c] = np.nan
    if len(_gap_df) > 0:
        _gap_df = _gap_df[_hourly_vent_block.columns]

    # Step 6: Get scaffold rows with vent info via left join
    _scaffold_df = pd.merge(
        _hourly_seq_block.drop(columns='recorded_dttm'),
        _hourly_vent_block.drop(columns='recorded_dttm'),
        on=['encounter_block', 'recorded_date', 'recorded_hour'],
        how='left'
    )

    _gap_df = _gap_df.drop(columns='recorded_dttm', errors='ignore')
    _extra_rows = _extra_rows.drop(columns='recorded_dttm', errors='ignore')
    _extra_rows = _extra_rows.drop(columns='max_seq_dttm', errors='ignore')

    # Step 7: Combine all three
    final_df_block_raw = pd.concat([_scaffold_df, _gap_df, _extra_rows], ignore_index=True)

    # Step 8: Sort
    final_df_block_raw = final_df_block_raw.sort_values(
        by=['encounter_block', 'recorded_date', 'recorded_hour']
    ).reset_index(drop=True)

    # Step 9: Add time_from_vent
    final_df_block_raw['time_from_vent'] = final_df_block_raw.groupby('encounter_block').cumcount()
    final_df_block_raw['time_from_vent_adjusted'] = np.where(
        final_df_block_raw['time_from_vent'] < 4, -1, final_df_block_raw['time_from_vent'] - 4
    )

    _cols = ['encounter_block', 'recorded_date', 'recorded_hour', 'time_from_vent', 'time_from_vent_adjusted']
    _cols += [col for col in final_df_block_raw.columns if col not in _cols]
    final_df_block_raw = final_df_block_raw[_cols]

    log("Final shape:", final_df_block_raw.shape)
    log("Unique encounter_blocks:", final_df_block_raw['encounter_block'].nunique())

    # Save respiratory support summary CSVs already done above
    return final_blocks, final_df_block_raw, strobe_e, vitals_cohort


@app.cell
def _(mo):
    mo.md(r"""#### (F) Exclusion Criteria""")
    return


@app.cell
def exclusions(all_ids_vent, final_df_block_raw, log):
    # Count vent hours per block in first 72 hours
    _first_72_hours = final_df_block_raw[(final_df_block_raw['time_from_vent'] >= 0) & (final_df_block_raw['time_from_vent'] < 72)].copy()
    # Unbounded ffill is clinically appropriate here: IMV patients don't toggle on/off
    # frequently, and gaps represent documentation holes, not extubation events.
    _first_72_hours['hourly_on_vent'] = _first_72_hours.groupby('encounter_block')['hourly_on_vent'].ffill()
    _first_72_hours['hourly_trach'] = _first_72_hours.groupby('encounter_block')['hourly_trach'].ffill()
    _vent_hours_per_block = _first_72_hours.groupby('encounter_block')['hourly_on_vent'].sum()

    # Exclude blocks with imv for less than 4 hours
    _blocks_under_4 = _vent_hours_per_block[_vent_hours_per_block < 4].index
    _final_df_block = final_df_block_raw[~final_df_block_raw['encounter_block'].isin(_blocks_under_4)].copy()

    strobe_excl = {}
    strobe_excl['G_blocks_with_vent_4_or_more'] = _final_df_block['encounter_block'].nunique()
    strobe_excl['G_blocks_with_vent_less_than_4'] = len(_blocks_under_4)
    log(f"Unique encounter blocks with valid IMV start/end: {strobe_excl['G_blocks_with_vent_4_or_more']}")
    log(f"Excluded {len(_blocks_under_4)} encounter blocks with <4 vent hours in first 72 hours of intubation.\n")

    # Exclude blocks with trach at the time of intubation
    _blocks_with_trach_at_intubation = _final_df_block[
        (_final_df_block['time_from_vent'] == 0) &
        (_final_df_block['hourly_trach'] == 1)
    ]['encounter_block'].unique()

    log(f"Blocks with trach at intubation: {len(_blocks_with_trach_at_intubation)}")

    final_df_block_clean = _final_df_block[
        ~_final_df_block['encounter_block'].isin(_blocks_with_trach_at_intubation)
    ].copy()

    strobe_excl['G_final_blocks_with_trach_at_intubation'] = len(_blocks_with_trach_at_intubation)
    strobe_excl['G_final_blocks_without_trach_at_intubation'] = final_df_block_clean['encounter_block'].nunique()

    log(f"Excluded {len(_blocks_with_trach_at_intubation)} blocks with trach at intubation")
    log(f"Final cohort size: {strobe_excl['G_final_blocks_without_trach_at_intubation']}")

    all_ids_excl = all_ids_vent[all_ids_vent['encounter_block'].isin(final_df_block_clean['encounter_block'])].copy()
    log(f"all_ids_excl shape before dedup: {all_ids_excl.shape}")

    # Dedup to one row per encounter_block. Stitched encounters have multiple
    # hospitalization_ids per block; keep the latest discharge info (the actual
    # outcome). patient_id is identical across stitched hospitalizations.
    _n_before = len(all_ids_excl)
    all_ids_excl = (
        all_ids_excl
        .sort_values('discharge_dttm')
        .groupby('encounter_block', as_index=False)
        .last()
    )
    log(f"all_ids_excl shape after dedup: {all_ids_excl.shape} (removed {_n_before - len(all_ids_excl)} duplicate rows from stitched encounters)")

    return all_ids_excl, final_df_block_clean, strobe_excl


@app.cell
def _(mo):
    mo.md(r"""## Build Analysis Dataset""")
    return


@app.cell
def birth_final_df(
    all_ids_excl,
    final_blocks,
    final_df_block_clean,
    log,
    patient,
    pd,
):
    final_df_base = pd.merge(
        final_df_block_clean,
        all_ids_excl,
        on='encounter_block',
        how='left'
    ).reindex(columns=[
        'encounter_block', 'recorded_date', 'recorded_hour',
        'time_from_vent', 'time_from_vent_adjusted',
        'min_fio2_set', 'max_fio2_set', 'min_peep_set', 'max_peep_set',
        'min_lpm_set', 'max_lpm_set', 'min_resp_rate_obs', 'max_resp_rate_obs',
        'hourly_trach', 'hourly_on_vent'
    ])

    # Check for duplicates
    _key_cols = ['encounter_block', 'recorded_date', 'recorded_hour']
    _duplicates = final_df_base.duplicated(subset=_key_cols).sum()
    log(f"Number of duplicate rows: {_duplicates}")

    all_ids_final = all_ids_excl[all_ids_excl['encounter_block'].isin(final_df_base['encounter_block'])].copy()
    log(all_ids_final.shape)

    for _col in all_ids_final.columns[:3]:
        log(f"\n{_col}:")
        log(all_ids_final[_col].nunique())

    # ── Add final outcome dttm ──
    all_ids_w_outcome = pd.merge(
        all_ids_final,
        final_blocks,
        on='encounter_block',
        how='left'
    )

    all_ids_w_outcome = pd.merge(
        all_ids_w_outcome,
        patient[['patient_id', 'death_dttm']],
        on='patient_id',
        how='left'
    )

    all_ids_w_outcome['final_outcome_dttm'] = all_ids_w_outcome['block_last_vital_dttm']
    all_ids_w_outcome['is_dead'] = (all_ids_w_outcome['discharge_category'].str.lower().isin(['expired', 'hospice'])).astype(int)

    _mask_death_before_discharge = all_ids_w_outcome['death_dttm'] < all_ids_w_outcome['discharge_dttm']
    all_ids_w_outcome.loc[_mask_death_before_discharge, 'final_outcome_dttm'] = all_ids_w_outcome['death_dttm']
    all_ids_w_outcome.loc[_mask_death_before_discharge, 'is_dead'] = 1

    # Hospice patients without death_dttm: use discharge_dttm as outcome time
    # (they left the hospital alive to hospice; block_last_vital_dttm may be earlier)
    _mask_hospice_no_death = (
        (all_ids_w_outcome['discharge_category'].str.lower() == 'hospice') &
        (all_ids_w_outcome['death_dttm'].isna())
    )
    all_ids_w_outcome.loc[_mask_hospice_no_death, 'final_outcome_dttm'] = (
        all_ids_w_outcome.loc[_mask_hospice_no_death, ['block_last_vital_dttm', 'discharge_dttm']].max(axis=1)
    )
    log(f"Hospice patients without death_dttm (final_outcome_dttm set to max of last_vital/discharge): {_mask_hospice_no_death.sum()}")

    # Sanity check
    _mask_death_before_vitals = (all_ids_w_outcome['death_dttm'].notna()) & (all_ids_w_outcome['death_dttm'] < all_ids_w_outcome['block_last_vital_dttm'])
    log("Number of blocks where death_dttm is before block_last_vital_dttm:", _mask_death_before_vitals.sum())
    _death_before_vitals_df = all_ids_w_outcome[_mask_death_before_vitals][['patient_id', 'hospitalization_id', 'encounter_block', 'death_dttm', 'block_last_vital_dttm', 'final_outcome_dttm']]
    _death_before_vitals_df['diff_hour'] = (_death_before_vitals_df['death_dttm'] - _death_before_vitals_df['block_last_vital_dttm']).dt.total_seconds() / 3600

    # --- Sanity check: each patient should die at most once ---
    _deaths_per_patient = all_ids_w_outcome[all_ids_w_outcome['is_dead'] == 1].groupby('patient_id')['encounter_block'].nunique()
    _multi_death = _deaths_per_patient[_deaths_per_patient > 1]
    if len(_multi_death) > 0:
        log(f"WARNING: {len(_multi_death)} patients have is_dead=1 on multiple encounter blocks!")
        log(f"  Patient IDs: {_multi_death.index.tolist()[:10]}{'...' if len(_multi_death) > 10 else ''}")
        # Keep is_dead=1 only on the LAST block (by vent start) for patients with multiple dead blocks
        for _pid in _multi_death.index:
            _mask = (all_ids_w_outcome['patient_id'] == _pid) & (all_ids_w_outcome['is_dead'] == 1)
            _rows = all_ids_w_outcome.loc[_mask].sort_values('block_vent_start_dttm')
            # Zero out all but the last block
            _to_zero = _rows.index[:-1]
            all_ids_w_outcome.loc[_to_zero, 'is_dead'] = 0
        log(f"  Fixed: kept is_dead=1 only on last block per patient.")
    else:
        log("PASS: No patient has is_dead=1 on multiple encounter blocks.")

    for _col in all_ids_w_outcome.columns[:3]:
        log(f"\n{_col}:")
        log(all_ids_w_outcome[_col].nunique())

    return all_ids_final, all_ids_w_outcome, final_df_base


@app.cell
def _(mo):
    mo.md(r"""### BMI Extraction""")
    return


@app.cell
def bmi_extraction(
    all_ids_w_outcome,
    all_ids_vent,
    block_vent_times,
    log,
    np,
    outlier_cfg,
    vitals_cohort,
):
    # Merge vitals_cohort with IDs locally (avoids persisting large vitals_stitched in memory)
    _vitals_bmi = vitals_cohort[
        (vitals_cohort['vital_category'].isin(['weight_kg', 'height_cm']))
    ].merge(all_ids_vent[['hospitalization_id', 'encounter_block']], on='hospitalization_id', how='inner')
    _vitals_bmi = _vitals_bmi[
        _vitals_bmi['encounter_block'].isin(all_ids_w_outcome['encounter_block'])
    ].copy()

    # Remove outliers
    _min_height, _max_height = outlier_cfg['height_cm']
    _min_weight, _max_weight = outlier_cfg['weight_kg']

    _is_height = _vitals_bmi['vital_category'] == 'height_cm'
    _height_mask_low = _is_height & (_vitals_bmi['vital_value'] < _min_height)
    _height_mask_high = _is_height & (_vitals_bmi['vital_value'] > _max_height)
    _vitals_bmi.loc[_height_mask_low | _height_mask_high, 'vital_value'] = np.nan

    _is_weight = _vitals_bmi['vital_category'] == 'weight_kg'
    _weight_mask_low = _is_weight & (_vitals_bmi['vital_value'] < _min_weight)
    _weight_mask_high = _is_weight & (_vitals_bmi['vital_value'] > _max_weight)
    _vitals_bmi.loc[_weight_mask_low | _weight_mask_high, 'vital_value'] = np.nan

    # Merge with vent_start_end to get ventilation start time
    _vitals_bmi = _vitals_bmi.merge(
        block_vent_times[['encounter_block', 'block_vent_start_dttm']],
        on='encounter_block',
        how='left'
    )

    _vitals_bmi['time_diff'] = (_vitals_bmi['recorded_dttm'] - _vitals_bmi['block_vent_start_dttm']).dt.total_seconds() / 3600
    _vitals_bmi['before_vent_start'] = (_vitals_bmi['time_diff'] <= 0).astype(int)
    _vitals_bmi['abs_time_diff'] = _vitals_bmi['time_diff'].abs()

    # Restrict to vitals within 30 days of intubation
    _n_before = len(_vitals_bmi)
    _vitals_bmi = _vitals_bmi[_vitals_bmi['abs_time_diff'] <= 30 * 24]
    log(f"BMI vitals: kept {len(_vitals_bmi)}/{_n_before} within 30 days of intubation")

    _vitals_bmi = _vitals_bmi.sort_values(
        ['encounter_block', 'vital_category', 'before_vent_start', 'abs_time_diff'],
        ascending=[True, True, False, True]
    )

    _vitals_bmi = _vitals_bmi.drop_duplicates(subset=['encounter_block', 'vital_category'], keep='first')

    vitals_bmi_pivot = _vitals_bmi.pivot(
        index='encounter_block',
        columns='vital_category',
        values='vital_value'
    ).reset_index()

    vitals_bmi_pivot['bmi'] = vitals_bmi_pivot['weight_kg'] / ((vitals_bmi_pivot['height_cm'] / 100) ** 2)
    log(f"Number of unique encounter blocks with BMI data: {vitals_bmi_pivot['encounter_block'].nunique()}")

    return (vitals_bmi_pivot,)


@app.cell
def _(mo):
    mo.md(r"""## Hourly Vitals""")
    return


@app.cell
def vitals_merge(
    all_ids_w_outcome,
    all_ids_vent,
    final_df_base,
    gc,
    log,
    np,
    pd,
    pyCLIF,
    vitals_cohort,
):
    # Merge vitals_cohort with IDs locally (avoids persisting large vitals_stitched in memory)
    _vitals_stitched = vitals_cohort.merge(
        all_ids_vent[['hospitalization_id', 'encounter_block']], on='hospitalization_id', how='inner'
    )
    _vitals_stitched['recorded_date'] = _vitals_stitched['recorded_dttm'].dt.normalize().dt.tz_localize(None)
    _vitals_stitched['recorded_hour'] = _vitals_stitched['recorded_dttm'].dt.hour
    log(f"Number of unique encounter blocks BEFORE filtering vitals: {_vitals_stitched['encounter_block'].nunique()}")
    _vitals_stitched = _vitals_stitched[_vitals_stitched['encounter_block'].isin(all_ids_w_outcome['encounter_block'])]
    log(f"Number of unique encounter blocks AFTER filtering vitals: {_vitals_stitched['encounter_block'].nunique()}")

    strobe_vitals = {}
    strobe_vitals['final_blocks_with_vitals'] = _vitals_stitched['encounter_block'].nunique()

    # Calculate MAP
    _vitals_stitched = _vitals_stitched[_vitals_stitched['vital_category'] != 'map']
    if 'map' not in _vitals_stitched['vital_category'].unique():
        _sbp_dbp = _vitals_stitched[_vitals_stitched['vital_category'].isin(['sbp', 'dbp'])].copy()
        _sbp_dbp_pivot = _sbp_dbp.pivot_table(
            index=['encounter_block', 'recorded_dttm'],
            columns='vital_category',
            values='vital_value'
        ).reset_index()
        del _sbp_dbp; gc.collect()
        _sbp_dbp_pivot = _sbp_dbp_pivot.dropna(subset=['sbp', 'dbp'])
        _sbp_dbp_pivot['map'] = (_sbp_dbp_pivot['sbp'] + 2 * _sbp_dbp_pivot['dbp']) / 3

        _map_vitals = _sbp_dbp_pivot[['encounter_block', 'recorded_dttm', 'map']].copy()
        del _sbp_dbp_pivot; gc.collect()
        _map_vitals['vital_category'] = 'map'
        _map_vitals['vital_value'] = _map_vitals['map']
        _map_vitals.drop(columns='map', inplace=True)
        _map_vitals['recorded_date'] = _map_vitals['recorded_dttm'].dt.normalize().dt.tz_localize(None)
        _map_vitals['recorded_hour'] = _map_vitals['recorded_dttm'].dt.hour
        _map_vitals.drop(columns='recorded_dttm', inplace=True)
        _vitals_stitched.drop(columns='recorded_dttm', inplace=True)
        _vitals_stitched = pd.concat([_vitals_stitched, _map_vitals], ignore_index=True)
        del _map_vitals; gc.collect()
        log("...map was calculated and appended to vitals_stitched.")
    else:
        log("Map exists in your CLIF database")
        _vitals_stitched.drop(columns='recorded_dttm', inplace=True)

    # Keep only columns needed for the pivot to reduce memory
    _vitals_stitched = _vitals_stitched[['encounter_block', 'recorded_date', 'recorded_hour', 'vital_category', 'vital_value']]

    # DuckDB pivot: groupby + pivot in a single SQL pass — no chunking, no MultiIndex
    import time as _time
    _t0 = _time.perf_counter()
    _vitals_pivot = pyCLIF.pivot_vitals_duckdb(_vitals_stitched)
    _dt = _time.perf_counter() - _t0
    del _vitals_stitched; gc.collect()
    log(f"DuckDB vitals pivot completed in {_dt:.1f}s")

    log("Finished creating block-level min/max/avg vitals pivot:")
    log(_vitals_pivot.columns.tolist())

    _checkpoint_vitals = pyCLIF.remove_duplicates(_vitals_pivot, [
        'encounter_block', 'recorded_date', 'recorded_hour'
    ], 'final_df')
    del _checkpoint_vitals

    # merge vitals with final_df
    final_df_v = pd.merge(final_df_base, _vitals_pivot, on=['encounter_block', 'recorded_date', 'recorded_hour'],
                          how='left')
    log("\n Columns in final_df after merging with vitals:")
    log(final_df_v.columns.tolist())

    # Defensive: ensure expected vital columns exist for downstream (notebook 02)
    _expected_vital_cols = [
        'avg_map', 'min_map', 'max_map',
        'avg_sbp', 'min_sbp', 'max_sbp',
        'avg_dbp', 'min_dbp', 'max_dbp',
        'avg_heart_rate', 'min_heart_rate', 'max_heart_rate',
        'avg_spo2', 'min_spo2', 'max_spo2',
        'avg_respiratory_rate', 'min_respiratory_rate', 'max_respiratory_rate',
    ]
    _missing_vital_cols = [c for c in _expected_vital_cols if c not in final_df_v.columns]
    if _missing_vital_cols:
        log(f"WARNING: Missing expected vital columns (created as NaN): {_missing_vital_cols}")
        for _c in _missing_vital_cols:
            final_df_v[_c] = np.nan

    return final_df_v, strobe_vitals


@app.cell
def _(mo):
    mo.md(r"""## Hourly Meds""")
    return


@app.cell
def meds_merge(
    all_ids_final,
    final_df_v,
    gc,
    log,
    meds_of_interest,
    meds_required_columns,
    np,
    outlier_cfg,
    pd,
    plt,
    pyCLIF,
    vitals_bmi_pivot,
):
    # Import clif continuous meds
    _meds_filters = {
        'hospitalization_id': all_ids_final['hospitalization_id'].unique().tolist(),
        'med_category': meds_of_interest
    }
    _meds = pyCLIF.load_data('clif_medication_admin_continuous', columns=meds_required_columns, filters=_meds_filters)
    _meds = _meds.merge(all_ids_final, on='hospitalization_id', how='left')
    log("Unique encounters in meds", pyCLIF.count_unique_encounters(_meds))

    _meds['hospitalization_id'] = _meds['hospitalization_id'].astype(str)
    _meds['med_dose_unit'] = _meds['med_dose_unit'].str.lower()
    _meds = pyCLIF.convert_datetime_columns_to_site_tz(_meds, pyCLIF.helper['timezone'])
    _meds['med_dose'] = pd.to_numeric(_meds['med_dose'], errors='coerce')
    _meds['recorded_date'] = _meds['admin_dttm'].dt.normalize().dt.tz_localize(None)
    _meds['recorded_hour'] = _meds['admin_dttm'].dt.hour

    # Create summary tables
    _summary_meds = _meds.groupby('med_category').agg(
        total_N=('med_category', 'size'),
        min=('med_dose', 'min'),
        max=('med_dose', 'max'),
        first_quantile=('med_dose', lambda x: x.quantile(0.25)),
        second_quantile=('med_dose', lambda x: x.quantile(0.5)),
        third_quantile=('med_dose', lambda x: x.quantile(0.75)),
        missing_values=('med_dose', lambda x: x.isna().sum())
    ).reset_index()
    _summary_meds.to_csv(f'{pyCLIF.project_root}/output/final/summary_meds_by_category.csv', index=False)

    _summary_meds_cat_dose = _meds.groupby(['med_category', 'med_dose_unit']).agg(
        total_N=('med_category', 'size'),
        min=('med_dose', 'min'),
        max=('med_dose', 'max'),
        first_quantile=('med_dose', lambda x: x.quantile(0.25)),
        second_quantile=('med_dose', lambda x: x.quantile(0.5)),
        third_quantile=('med_dose', lambda x: x.quantile(0.75)),
        missing_values=('med_dose', lambda x: x.isna().sum())
    ).reset_index()
    _summary_meds_cat_dose.to_csv(f'{pyCLIF.project_root}/output/final/summary_meds_by_category_dose_units.csv', index=False)

    # Diagnostic: Check which groups have all NaN values
    log("Groups with all NaN med_dose values:")
    for (_med_category, _med_dose_unit), _group in _meds.groupby(['med_category', 'med_dose_unit']):
        if _group['med_dose'].isna().all():
            log(f"  {_med_category} - {_med_dose_unit}: {len(_group)} rows, all NaN")

    # Plot histograms
    _grouped_data = _meds.groupby(['med_category', 'med_dose_unit'])
    _n_plots = len(_grouped_data.groups.keys())
    _n_cols = 4
    _n_rows = (_n_plots + _n_cols - 1) // _n_cols

    _fig, _axs = plt.subplots(_n_rows, _n_cols, figsize=(20, _n_rows * 5))
    _axs = _axs.flatten()

    _i = 0
    for _i, ((_med_category, _med_dose_unit), _group) in enumerate(_grouped_data):
        _ax = _axs[_i]
        _valid_doses = _group['med_dose'].dropna()
        if len(_valid_doses) > 0:
            _ax.hist(_valid_doses, bins=20, alpha=0.7, label=f"N = {len(_valid_doses)}")
            _ax.set_title(f"{_med_category} - {_med_dose_unit}")
            _ax.set_xlabel('Med Dose')
            _ax.set_ylabel('Frequency')
            _ax.legend()
            _ax.grid(True)
        else:
            _ax.text(0.5, 0.5, 'No valid data', ha='center', va='center', transform=_ax.transAxes)
            _ax.set_title(f"{_med_category} - {_med_dose_unit} (No Data)")

    for _j in range(_i + 1, len(_axs)):
        _axs[_j].axis('off')

    plt.tight_layout()
    plt.savefig(f'{pyCLIF.project_root}/output/final/graphs/meds_histograms.png')
    plt.close(_fig)

    # Sanity checks
    _med_dose_unit_check = _meds.groupby(['med_category', 'med_dose_unit']).size().reset_index(name='count')
    _med_dose_unit_check['unit_validity'] = _med_dose_unit_check.apply(pyCLIF.check_dose_unit, axis=1)
    _invalid_units = _med_dose_unit_check[_med_dose_unit_check['unit_validity'] == 'Not an acceptable unit']
    log("Invalid units. These will be dropped:\n")
    log(_invalid_units)

    # ── Norepinephrine equivalent calculation ──
    _meds_filtered = _meds[~_meds['med_dose'].isnull()].copy()
    _meds_filtered = _meds_filtered[_meds_filtered['med_dose_unit'].apply(pyCLIF.has_per_hour_or_min)].copy()

    _meds_list = [
        "norepinephrine", "epinephrine", "phenylephrine",
        "vasopressin", "dopamine",
        "angiotensin"
    ]

    _ne_df = _meds_filtered[_meds_filtered['med_category'].isin(_meds_list)].copy()
    _ne_df = _ne_df.merge(vitals_bmi_pivot[['encounter_block', 'weight_kg']], on='encounter_block', how='left')
    _ne_df["med_dose_converted"] = _ne_df.apply(pyCLIF.convert_dose, axis=1)

    _ne_df = _ne_df[_ne_df.apply(pyCLIF.is_dose_within_range, axis=1, args=(outlier_cfg,))].copy()

    for _med in _meds_list:
        if _med not in _ne_df['med_category'].unique():
            log(f"{_med} is not in the dataset.")
        else:
            log(f"{_med} is in the dataset.")

    # Pivot and Aggregate (DuckDB: groupby + conditional agg in one pass)
    import time as _time
    _t0 = _time.perf_counter()
    _dose_pivot = pyCLIF.pivot_meds_duckdb(_ne_df, order_col='admin_dttm')
    log(f"DuckDB meds pivot completed in {_time.perf_counter() - _t0:.1f}s")

    _dose_pivot.fillna(0, inplace=True)

    # Calculate NE min
    _dose_pivot['ne_calc_min'] = (
        _dose_pivot.get('min_norepinephrine', 0) +
        _dose_pivot.get('min_epinephrine', 0) +
        _dose_pivot.get('min_phenylephrine', 0) / 10 +
        _dose_pivot.get('min_dopamine', 0) / 100 +
        _dose_pivot.get('min_metaraminol', 0) / 8 +
        _dose_pivot.get('min_vasopressin', 0) * 2.5 +
        _dose_pivot.get('min_angiotensin', 0) * 10
    )

    _dose_pivot['ne_calc_max'] = (
        _dose_pivot.get('max_norepinephrine', 0) +
        _dose_pivot.get('max_epinephrine', 0) +
        _dose_pivot.get('max_phenylephrine', 0) / 10 +
        _dose_pivot.get('max_dopamine', 0) / 100 +
        _dose_pivot.get('max_metaraminol', 0) / 8 +
        _dose_pivot.get('max_vasopressin', 0) * 2.5 +
        _dose_pivot.get('max_angiotensin', 0) * 10
    )

    _dose_pivot['ne_calc_first'] = (
        _dose_pivot.get('first_norepinephrine', 0) +
        _dose_pivot.get('first_epinephrine', 0) +
        _dose_pivot.get('first_phenylephrine', 0) / 10 +
        _dose_pivot.get('first_dopamine', 0) / 100 +
        _dose_pivot.get('first_metaraminol', 0) / 8 +
        _dose_pivot.get('first_vasopressin', 0) * 2.5 +
        _dose_pivot.get('first_angiotensin', 0) * 10
    )

    _dose_pivot['ne_calc_last'] = (
        _dose_pivot.get('last_norepinephrine', 0) +
        _dose_pivot.get('last_epinephrine', 0) +
        _dose_pivot.get('last_phenylephrine', 0) / 10 +
        _dose_pivot.get('last_dopamine', 0) / 100 +
        _dose_pivot.get('last_metaraminol', 0) / 8 +
        _dose_pivot.get('last_vasopressin', 0) * 2.5 +
        _dose_pivot.get('last_angiotensin', 0) * 10
    )

    _ne_calc_df = _dose_pivot[['encounter_block', 'recorded_date',
                               'recorded_hour',
                               'ne_calc_min', 'ne_calc_max',
                               'ne_calc_first', 'ne_calc_last']].drop_duplicates(subset=['encounter_block', 'recorded_date', 'recorded_hour'])

    strobe_meds = {}
    strobe_meds['final_blocks_with_norepi_eq'] = _ne_calc_df['encounter_block'].nunique()

    _encounter_blocks_list = _ne_df['encounter_block'].unique().tolist()
    _hourly_ne = pyCLIF.build_meds_hourly_scaffold(
        _ne_df,
        id_col="encounter_block",
        ids=_encounter_blocks_list,
        timestamp_col="admin_dttm",
        site_tz=pyCLIF.helper['timezone']
    )
    del _ne_df; gc.collect()

    # Coerce scaffold's recorded_date from object to datetime64 to match pivot
    _hourly_ne['recorded_date'] = pd.to_datetime(_hourly_ne['recorded_date'])
    _ne_calc_df = _ne_calc_df.sort_values(by=['encounter_block', 'recorded_date', 'recorded_hour'])
    _hourly_ne_merged = pd.merge(
        _hourly_ne,
        _ne_calc_df,
        on=['encounter_block', 'recorded_date', 'recorded_hour'],
        how='left'
    )
    _cols_to_fill = ['ne_calc_min', 'ne_calc_max', 'ne_calc_first', 'ne_calc_last']
    _hourly_ne_merged[_cols_to_fill] = _hourly_ne_merged.groupby('encounter_block')[_cols_to_fill].ffill()

    def _add_last_ne_6h(_group):
        _group['last_ne_dose_last_6_hours'] = (
            _group['ne_calc_last']
            .shift(6)
            .fillna(0)
        )
        return _group

    _hourly_ne_merged = (
        _hourly_ne_merged
        .groupby('encounter_block', group_keys=False)
        .apply(_add_last_ne_6h)
        .reset_index(drop=True)
    )

    _checkpoint_meds = pyCLIF.remove_duplicates(_hourly_ne_merged, [
        'encounter_block', 'recorded_date', 'recorded_hour'
    ], 'final_df')
    del _checkpoint_meds

    log("final_df shape before merging", final_df_v.shape)
    _final_df_m = pyCLIF.extend_hourly_dataset(
        base_df=final_df_v,
        addon_df=_hourly_ne_merged,
        merge_cols=['encounter_block', 'recorded_date', 'recorded_hour']
    )
    log("final_df shape after merging", _final_df_m.shape)

    # ── Red meds ──
    _red_meds_list = ["nicardipine", "nitroprusside", "clevidipine"]
    _red_meds_df = _meds[_meds['med_category'].isin(_red_meds_list)].copy()

    for _med in _red_meds_list:
        _red_meds_df[_med + '_flag'] = np.where(
            (_red_meds_df['med_category'] == _med) &
            (_red_meds_df['med_dose'] > 0.0) &
            (_red_meds_df['med_dose'].notna()), 1, 0
        ).astype(int)

    _red_meds_flags = _red_meds_df.groupby(['encounter_block', 'recorded_date', 'recorded_hour']).agg(
        {_med + '_flag': 'max' for _med in _red_meds_list}
    ).reset_index()

    _red_meds_flags['red_meds_flag'] = _red_meds_flags[[_med + '_flag' for _med in _red_meds_list]].max(axis=1)

    _red_meds_flags_final = _red_meds_flags[[
        'encounter_block', 'recorded_date', 'recorded_hour',
        'nicardipine_flag', 'nitroprusside_flag',
        'clevidipine_flag', 'red_meds_flag'
    ]].drop_duplicates(subset=['encounter_block', 'recorded_date', 'recorded_hour'])

    _red_meds_flags_final['nicardipine_flag'] = _red_meds_flags_final['nicardipine_flag'].astype(int)
    _red_meds_flags_final['nitroprusside_flag'] = _red_meds_flags_final['nitroprusside_flag'].astype(int)
    _red_meds_flags_final['clevidipine_flag'] = _red_meds_flags_final['clevidipine_flag'].astype(int)
    _red_meds_flags_final['red_meds_flag'] = _red_meds_flags_final['red_meds_flag'].astype(int)

    strobe_meds['final_blocks_with_red_meds'] = _red_meds_flags_final['encounter_block'].nunique()

    _checkpoint_red_meds = pyCLIF.remove_duplicates(_red_meds_flags_final, [
        'encounter_block', 'recorded_date', 'recorded_hour'
    ], 'final_df')
    del _checkpoint_red_meds

    # Red meds flags: simple left merge (no scaffold extension needed —
    # flag rows are a subset of existing scaffold hours)
    log("final_df shape before merging red meds", _final_df_m.shape)
    _final_df_m = _final_df_m.merge(
        _red_meds_flags_final,
        on=['encounter_block', 'recorded_date', 'recorded_hour'],
        how='left'
    )
    log("final_df shape after merging red meds", _final_df_m.shape)
    log("\n Columns in final_df after merging with red_meds_flags_final columns:")
    log(_final_df_m.columns.tolist())

    # ── Paralytics ──
    _paralytics_list = ["cisatracurium", "vecuronium", "rocuronium"]
    _paralytics_df = _meds[_meds['med_category'].isin(_paralytics_list)].copy()

    for _med in _paralytics_list:
        _paralytics_df[_med + '_flag'] = np.where(
            (_paralytics_df['med_category'] == _med) &
            (_paralytics_df['med_dose'] > 0.0) &
            (_paralytics_df['med_dose'].notna()), 1, 0
        ).astype(int)

    _paralytics_flags = _paralytics_df.groupby(['encounter_block', 'recorded_date', 'recorded_hour']).agg(
        {_med + '_flag': 'max' for _med in _paralytics_list}
    ).reset_index()

    _paralytics_flags['paralytics_flag'] = _paralytics_flags[[_med + '_flag' for _med in _paralytics_list]].max(axis=1)

    _paralytics_flags_final = _paralytics_flags[[
        'encounter_block', 'recorded_date', 'recorded_hour',
        'cisatracurium_flag', 'vecuronium_flag',
        'rocuronium_flag', 'paralytics_flag'
    ]].drop_duplicates(subset=['encounter_block', 'recorded_date', 'recorded_hour'])

    _paralytics_flags_final['cisatracurium_flag'] = _paralytics_flags_final['cisatracurium_flag'].astype(int)
    _paralytics_flags_final['vecuronium_flag'] = _paralytics_flags_final['vecuronium_flag'].astype(int)
    _paralytics_flags_final['rocuronium_flag'] = _paralytics_flags_final['rocuronium_flag'].astype(int)
    _paralytics_flags_final['paralytics_flag'] = _paralytics_flags_final['paralytics_flag'].astype(int)

    strobe_meds['final_blocks_with_paralytics'] = _paralytics_flags_final['encounter_block'].nunique()

    _checkpoint_paralytics_meds = pyCLIF.remove_duplicates(_paralytics_flags_final, [
        'encounter_block', 'recorded_date', 'recorded_hour'
    ], 'final_df')
    del _checkpoint_paralytics_meds

    # Paralytic flags: simple left merge (no scaffold extension needed —
    # flag rows are a subset of existing scaffold hours)
    log("final_df shape before merging paralytics", _final_df_m.shape)
    final_df_m = _final_df_m.merge(
        _paralytics_flags_final,
        on=['encounter_block', 'recorded_date', 'recorded_hour'],
        how='left'
    )
    log("final_df shape after merging paralytics", final_df_m.shape)

    log("\n Columns in final_df after merging with paralytics columns:")
    log(final_df_m.columns.tolist())

    # Defensive: ensure expected med columns exist for downstream (notebook 02)
    _expected_med_cols = [
        'ne_calc_min', 'ne_calc_max', 'ne_calc_first', 'ne_calc_last',
        'last_ne_dose_last_6_hours',
        'nicardipine_flag', 'nitroprusside_flag', 'clevidipine_flag', 'red_meds_flag',
        'cisatracurium_flag', 'vecuronium_flag', 'rocuronium_flag', 'paralytics_flag',
    ]
    _missing_med_cols = [c for c in _expected_med_cols if c not in final_df_m.columns]
    if _missing_med_cols:
        log(f"WARNING: Missing expected med columns (created as NaN): {_missing_med_cols}")
        for _c in _missing_med_cols:
            final_df_m[_c] = np.nan

    return final_df_m, strobe_meds


@app.cell
def _(mo):
    mo.md(r"""## Hourly Labs""")
    return


@app.cell
def labs_merge(
    all_ids_final,
    block_vent_times,
    final_df_m,
    labs_of_interest,
    labs_required_columns,
    log,
    pd,
    pyCLIF,
):
    _labs_filters = {
        'hospitalization_id': all_ids_final['hospitalization_id'].unique().tolist(),
        'lab_category': labs_of_interest
    }
    _labs = pyCLIF.load_data('clif_labs', columns=labs_required_columns, filters=_labs_filters)
    log("unique encounters in labs", pyCLIF.count_unique_encounters(_labs))
    _labs['hospitalization_id'] = _labs['hospitalization_id'].astype(str)
    _labs = _labs.merge(all_ids_final, on='hospitalization_id', how='left')
    _labs = _labs.sort_values(by=['encounter_block', 'lab_result_dttm'])

    strobe_labs = {}
    strobe_labs['final_blocks_with_lactate_lab'] = _labs['encounter_block'].nunique()

    _labs = pyCLIF.convert_datetime_columns_to_site_tz(_labs, pyCLIF.helper['timezone'])
    _labs['lab_value_numeric'] = pd.to_numeric(_labs['lab_value_numeric'], errors='coerce')
    _labs['recorded_hour'] = _labs['lab_result_dttm'].dt.hour
    _labs['recorded_date'] = _labs['lab_result_dttm'].dt.normalize().dt.tz_localize(None)

    _lactate_df = pd.merge(_labs, block_vent_times, on='encounter_block', how='left')
    _lactate_df['time_since_vent_start_hours'] = (
        (_lactate_df['lab_result_dttm'] - _lactate_df['block_vent_start_dttm']).dt.total_seconds() / 3600
    )
    _lactate_df['time_diff_hours'] = abs((_lactate_df['lab_result_dttm'] - _lactate_df['block_vent_start_dttm']).dt.total_seconds() / 3600)
    _lactate_df = _lactate_df.sort_values(by=['encounter_block', 'recorded_date', 'recorded_hour', 'time_diff_hours'])

    _closest_lactate_df = _lactate_df.groupby(['encounter_block', 'recorded_date', 'recorded_hour']).first().reset_index()
    _labs_final = _closest_lactate_df[['encounter_block', 'recorded_date', 'recorded_hour', 'lab_value_numeric']].copy()
    _labs_final = _labs_final.rename(columns={'lab_value_numeric': 'lactate'})

    _checkpoint_labs = pyCLIF.remove_duplicates(_labs_final, [
        'encounter_block', 'recorded_date', 'recorded_hour'
    ], 'final_df')
    del _checkpoint_labs

    log("final_df shape before merging", final_df_m.shape)
    final_df = pyCLIF.extend_hourly_dataset(
        base_df=final_df_m,
        addon_df=_labs_final,
        merge_cols=['encounter_block', 'recorded_date', 'recorded_hour']
    )
    log("final_df shape after merging", final_df.shape)

    log(final_df.columns.tolist())

    return final_df, strobe_labs


@app.cell
def _(mo):
    mo.md(r"""## SOFA Scores""")
    return


@app.cell
def sofa_and_blocks(
    adt,
    all_ids_w_outcome,
    hospitalization,
    log,
    patient,
    pd,
    pyCLIF,
    sofa_score,
    vitals_bmi_pivot,
):
    _helper = pyCLIF.load_config()
    _tables_path = _helper['tables_path']

    _sofa_input_df = all_ids_w_outcome[['encounter_block', 'block_vent_start_dttm']].copy()
    _sofa_input_df = _sofa_input_df.rename(columns={'block_vent_start_dttm': 'start_dttm'})
    _sofa_input_df['stop_dttm'] = _sofa_input_df['start_dttm'] + pd.Timedelta(hours=24)
    _id_mappings = all_ids_w_outcome[['encounter_block', 'hospitalization_id']].drop_duplicates()

    sofa_df = sofa_score.compute_sofa(
        ids_w_dttm=_sofa_input_df,
        tables_path=_tables_path,
        use_hospitalization_id=False,
        id_mapping=_id_mappings,
        helper_module=pyCLIF,
        output_filepath=f"{pyCLIF.project_root}/output/intermediate/sofa.parquet"
    )

    final_df_blocks = all_ids_w_outcome.merge(sofa_df, on='encounter_block', how='left')
    final_df_blocks = final_df_blocks.merge(
        hospitalization[['hospitalization_id', 'admission_dttm', 'age_at_admission']],
        on='hospitalization_id', how='left'
    )
    final_df_blocks = final_df_blocks.merge(
        patient[['patient_id', 'race_category', 'ethnicity_category', 'sex_category', 'language_name']],
        on='patient_id', how='left'
    )

    # First join ADT with all_ids to get closest ADT row to vent start
    _adt_with_blocks = pd.merge(
        all_ids_w_outcome[['encounter_block', 'block_vent_start_dttm', 'hospitalization_id']],
        adt,
        on='hospitalization_id'
    )
    _adt_with_blocks['time_diff'] = abs(_adt_with_blocks['block_vent_start_dttm'] - _adt_with_blocks['in_dttm'])

    _closest_adt = (
        _adt_with_blocks
        .sort_values('time_diff')
        .groupby('encounter_block')
        .first()
        .reset_index()
    )

    final_df_blocks = final_df_blocks.merge(
        _closest_adt[['encounter_block', 'location_name', 'location_category', 'in_dttm', 'out_dttm']],
        on='encounter_block',
        how='left'
    )

    # Add BMI data
    final_df_blocks = final_df_blocks.merge(
        vitals_bmi_pivot[['encounter_block', 'height_cm', 'weight_kg', 'bmi']],
        on='encounter_block',
        how='left'
    )

    log(final_df_blocks.columns.tolist())

    return (final_df_blocks,)


@app.cell
def _(mo):
    mo.md(r"""## Write Outputs""")
    return


@app.cell
def write_outputs(
    all_ids_w_outcome,
    final_df,
    final_df_blocks,
    log,
    pd,
    pyCLIF,
    strobe_ab,
    strobe_c,
    strobe_d,
    strobe_e,
    strobe_excl,
    strobe_labs,
    strobe_meds,
    strobe_vitals,
):
    final_df.to_parquet(f'{pyCLIF.project_root}/output/intermediate/final_df_hourly.parquet')
    final_df_blocks.to_parquet(f'{pyCLIF.project_root}/output/intermediate/final_df_blocks.parquet')
    all_ids_w_outcome.to_parquet(f'{pyCLIF.project_root}/output/intermediate/cohort_all_ids_w_outcome.parquet')

    # Merge all strobe dicts
    strobe_counts = {}
    strobe_counts.update(strobe_ab)
    strobe_counts.update(strobe_c)
    strobe_counts.update(strobe_d)
    strobe_counts.update(strobe_e)
    strobe_counts.update(strobe_excl)
    strobe_counts.update(strobe_vitals)
    strobe_counts.update(strobe_meds)
    strobe_counts.update(strobe_labs)

    pd.DataFrame(list(strobe_counts.items()), columns=['Metric', 'Value']).to_csv(
        f'{pyCLIF.project_root}/output/final/strobe_counts.csv', index=False
    )

    log(strobe_counts)

    return (strobe_counts,)


@app.cell
def _(mo):
    mo.md(r"""## STROBE Flow Diagram""")
    return


@app.cell
def strobe_diagram(FancyArrowPatch, Rectangle, log, plt, pyCLIF, strobe_counts):
    _fig, _ax = plt.subplots(figsize=(10, 10))
    _ax.axis('off')

    _boxes = [
        {"text": f"All adult encounters after date filter\n(n = {strobe_counts['A_after_date_age_filter']})", "xy": (0.5, 0.9)},
        {"text": f"Linked Encounter Blocks\n(n = {strobe_counts['B_after_stitching']})", "xy": (0.5, 0.75)},
        {"text": f"Encounter blocks receiving IMV\n(n = {strobe_counts['C_imv_encounter_blocks']})", "xy": (0.5, 0.6)},
        {"text": f"Encounter blocks receiving IMV >= 4 hrs\n(n = {strobe_counts['G_blocks_with_vent_4_or_more']})", "xy": (0.5, 0.45)},
        {"text": f"Encounter blocks not on trach\n(n = {strobe_counts['G_final_blocks_without_trach_at_intubation']})", "xy": (0.5, 0.3)},
    ]

    _exclusions = [
        {"text": f"Linked hospitalizations\n(n = {strobe_counts['B_stitched_hosp_ids']})", "xy": (0.8, 0.825)},
        {"text": f"Excluded: Encounters on vent for <4 hrs\n(n = {strobe_counts['D_blocks_with_same_vent_start_end'] + strobe_counts['E_blocks_with_vent_end_before_vital_start'] + strobe_counts['G_blocks_with_vent_less_than_4']})", "xy": (0.8, 0.525)},
        {"text": f"Excluded: Encounters with Tracheostomy\n(n = {strobe_counts['G_final_blocks_with_trach_at_intubation']})", "xy": (0.8, 0.375)},
    ]

    # Draw main boxes and arrows
    for _i, _box in enumerate(_boxes):
        _x, _y = _box["xy"]
        _ax.add_patch(Rectangle((_x - 0.25, _y - 0.05), 0.5, 0.1, edgecolor='black', facecolor='white'))
        _ax.text(_x, _y, _box["text"], ha='center', va='center', fontsize=10)
        if _i < len(_boxes) - 1:
            _ax.add_patch(FancyArrowPatch((_x, _y - 0.05), (_x, _y - 0.1), arrowstyle='->', mutation_scale=15))

    # Draw exclusion boxes and connectors
    for _excl in _exclusions:
        _x, _y = _excl["xy"]
        _ax.add_patch(Rectangle((_x - 0.20, _y - 0.04), 0.38, 0.08, edgecolor='black', facecolor='#f8d7da'))
        _ax.text(_x, _y, _excl["text"], ha='center', va='center', fontsize=9)

    plt.tight_layout()
    plt.savefig(f'{pyCLIF.project_root}/output/final/graphs/strobe_diagram_{pyCLIF.helper["site_name"]}.png')
    plt.close(_fig)
    log("Created STROBE diagram")
    return


if __name__ == "__main__":
    app.run()
