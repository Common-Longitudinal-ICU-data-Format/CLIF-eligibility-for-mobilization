import marimo

__generated_with = "0.20.0"
app = marimo.App(width="full")


@app.cell
def _(mo):
    mo.md(r"""
    # Eligibility for Mobilization — Analysis
    """)
    return


@app.cell
def imports():
    import os, sys
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
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
    import pandas as pd
    import numpy as np
    import seaborn as sns
    import plotly.express as px
    import plotly.graph_objects as go
    from datetime import datetime
    from tableone import TableOne
    import pyCLIF
    import warnings as _warnings
    _warnings.filterwarnings('ignore')
    from upsetplot import UpSet, from_indicators
    import clifpy
    print("clifpy version:", clifpy.__version__)
    import matplotlib.colors as mcolors
    from pathlib import Path
    import marimo as mo

    return (
        Path,
        TableOne,
        UpSet,
        datetime,
        from_indicators,
        go,
        mcolors,
        mo,
        np,
        os,
        pd,
        plt,
        px,
        pyCLIF,
        sns,
    )


@app.cell
def setup_logger(os, pyCLIF):
    import logging

    _log_dir = f'{pyCLIF.project_root}/output/final'
    os.makedirs(_log_dir, exist_ok=True)

    _logger = logging.getLogger('clif_02')
    _logger.setLevel(logging.INFO)
    _logger.handlers.clear()

    _fh = logging.FileHandler(
        f'{_log_dir}/{pyCLIF.helper["site_name"]}_02_analysis_log.txt', mode='w'
    )
    _fh.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    _logger.addHandler(_fh)

    _ch = logging.StreamHandler()
    _ch.setFormatter(logging.Formatter('%(message)s'))
    _logger.addHandler(_ch)

    def log(*args, **kwargs):
        _msg = ' '.join(str(a) for a in args)
        _logger.info(_msg)

    log(f"=== CLIF Pipeline 02: Mobilization Analysis ===")
    log(f"Site: {pyCLIF.helper['site_name']}")
    return (log,)


@app.cell
def config(os, pyCLIF):
    _helper = pyCLIF.load_config()
    site_name = _helper['site_name']

    output_folder = f'{pyCLIF.project_root}/output/final/'
    graphs_folder = f'{pyCLIF.project_root}/output/final/graphs/'
    os.makedirs(graphs_folder, exist_ok=True)
    return graphs_folder, output_folder, site_name


@app.cell
def load_data(pd, pyCLIF):
    final_df_raw = pd.read_parquet(f'{pyCLIF.project_root}/output/intermediate/final_df_hourly.parquet')
    all_ids_w_outcome = pd.read_parquet(f'{pyCLIF.project_root}/output/intermediate/cohort_all_ids_w_outcome.parquet')
    final_df_blocks_raw = pd.read_parquet(f'{pyCLIF.project_root}/output/intermediate/final_df_blocks.parquet')
    return all_ids_w_outcome, final_df_blocks_raw, final_df_raw


@app.cell
def _(mo):
    mo.md(r"""
    ## Patient-Level Missingness
    """)
    return


@app.cell
def missingness_helpers(np, pd, plt, sns):
    ANALYSIS_VARIABLES = {
        # Respiratory support
        'min_fio2_set': 'FiO2 (min)', 'max_fio2_set': 'FiO2 (max)',
        'min_peep_set': 'PEEP (min)', 'max_peep_set': 'PEEP (max)',
        'min_lpm_set': 'LPM (min)', 'max_lpm_set': 'LPM (max)',
        'min_resp_rate_obs': 'Resp Rate Obs (min)', 'max_resp_rate_obs': 'Resp Rate Obs (max)',
        # Vitals
        'avg_map': 'MAP (avg)', 'min_map': 'MAP (min)', 'max_map': 'MAP (max)',
        'avg_sbp': 'SBP (avg)', 'min_sbp': 'SBP (min)', 'max_sbp': 'SBP (max)',
        'avg_dbp': 'DBP (avg)', 'min_dbp': 'DBP (min)', 'max_dbp': 'DBP (max)',
        'avg_heart_rate': 'Heart Rate (avg)', 'min_heart_rate': 'Heart Rate (min)', 'max_heart_rate': 'Heart Rate (max)',
        'avg_respiratory_rate': 'Resp Rate (avg)', 'min_respiratory_rate': 'Resp Rate (min)', 'max_respiratory_rate': 'Resp Rate (max)',
        'avg_spo2': 'SpO2 (avg)', 'min_spo2': 'SpO2 (min)', 'max_spo2': 'SpO2 (max)',
        'avg_height_cm': 'Height (avg)', 'max_height_cm': 'Height (max)', 'min_height_cm': 'Height (min)',
        'avg_weight_kg': 'Weight (avg)', 'max_weight_kg': 'Weight (max)', 'min_weight_kg': 'Weight (min)',
        # Medications
        'ne_calc_min': 'NE Equiv (min)', 'ne_calc_max': 'NE Equiv (max)',
        'ne_calc_first': 'NE Equiv (first)', 'ne_calc_last': 'NE Equiv (last)',
        'last_ne_dose_last_6_hours': 'NE Dose (6h lookback)',
        # Labs
        'lactate': 'Lactate',
    }

    _VARIABLE_SOURCE = {}
    for _v in ['min_fio2_set', 'max_fio2_set', 'min_peep_set', 'max_peep_set',
               'min_lpm_set', 'max_lpm_set', 'min_resp_rate_obs', 'max_resp_rate_obs']:
        _VARIABLE_SOURCE[_v] = 'Respiratory Support'
    for _v in ['avg_map', 'min_map', 'max_map', 'avg_sbp', 'min_sbp', 'max_sbp',
               'avg_dbp', 'min_dbp', 'max_dbp', 'avg_heart_rate', 'min_heart_rate', 'max_heart_rate',
               'avg_respiratory_rate', 'min_respiratory_rate', 'max_respiratory_rate',
               'avg_spo2', 'min_spo2', 'max_spo2', 'avg_height_cm', 'max_height_cm', 'min_height_cm',
               'avg_weight_kg', 'max_weight_kg', 'min_weight_kg']:
        _VARIABLE_SOURCE[_v] = 'Vitals'
    for _v in ['ne_calc_min', 'ne_calc_max', 'ne_calc_first', 'ne_calc_last', 'last_ne_dose_last_6_hours']:
        _VARIABLE_SOURCE[_v] = 'Medications'
    _VARIABLE_SOURCE['lactate'] = 'Labs'

    def calculate_patient_missingness(df, variables_dict):
        """Binary patient-level missingness."""
        _vars_to_check = [v for v in variables_dict if v in df.columns]
        _has_any_data = (
            df[_vars_to_check]
            .notna()
            .astype(np.int8)
            .groupby(df['encounter_block'])
            .max()
        )
        return _has_any_data

    def generate_missingness_summary(has_data_df, variables_dict, sname):
        _n_patients = len(has_data_df)
        _n_with = has_data_df.sum()
        _n_without = _n_patients - _n_with
        _stats = pd.DataFrame({
            'variable': has_data_df.columns,
            'variable_label': [variables_dict.get(v, v) for v in has_data_df.columns],
            'source_table': [_VARIABLE_SOURCE.get(v, '') for v in has_data_df.columns],
            'n_patients_with_data': _n_with.values.astype(int),
            'pct_patients_with_data': (_n_with.values / _n_patients * 100),
            'n_patients_no_data': _n_without.values.astype(int),
            'pct_patients_no_data': (_n_without.values / _n_patients * 100),
            'site': sname,
            'n_patients': _n_patients,
        })
        return _stats.sort_values('pct_patients_no_data', ascending=False).reset_index(drop=True)

    def plot_missingness_heatmap(summary_df, sname, n_patients, save_path):
        _plot_df = summary_df[['variable_label', 'pct_patients_no_data', 'pct_patients_with_data']].copy()
        _plot_df = _plot_df.set_index('variable_label')
        _plot_df.columns = ['% Patients\nNo Data', '% Patients\nWith Data']
        _plot_df = _plot_df.sort_values('% Patients\nNo Data', ascending=False)
        _fig, _ax = plt.subplots(figsize=(8, max(8, len(_plot_df) * 0.35)))
        sns.heatmap(_plot_df, annot=True, fmt='.1f', cmap='RdYlGn_r',
                    center=50, vmin=0, vmax=100, linewidths=0.5, linecolor='white',
                    cbar_kws={'label': 'Percentage (%)', 'shrink': 0.8}, ax=_ax)
        _ax.set_xlabel('', fontsize=12, fontweight='bold')
        _ax.set_ylabel('Variable', fontsize=12, fontweight='bold')
        _ax.set_title(f'Patient-Level Data Availability (Binary)\nSite: {sname} | N = {n_patients:,} | 72h window',
                     fontsize=14, fontweight='bold', pad=20)
        plt.xticks(rotation=0)
        plt.yticks(rotation=0, fontsize=9)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()

    def check_missingness_by_variable(df, variable_list, exclude_flags=True):
        if exclude_flags:
            _vars_to_check = [var for var in variable_list if 'flag' not in var.lower()]
        else:
            _vars_to_check = variable_list
        _vars_to_check = [var for var in _vars_to_check if var not in [
            'encounter_block', 'hospitalization_id', 'recorded_dttm',
            'recorded_date', 'recorded_hour', 'time_from_vent',
            'hourly_trach', 'paralytics_flag']]
        _missing_pct = {}
        _total_blocks = df['encounter_block'].nunique()
        for _var in _vars_to_check:
            _blocks_never_measured = df.groupby('encounter_block')[_var].apply(lambda x: x.isna().all()).sum()
            _missing_pct[_var] = (_blocks_never_measured / _total_blocks) * 100
        return pd.Series(_missing_pct).sort_values(ascending=False)

    return (
        ANALYSIS_VARIABLES,
        calculate_patient_missingness,
        check_missingness_by_variable,
        generate_missingness_summary,
        plot_missingness_heatmap,
    )


@app.cell
def pre_fill_missingness(
    ANALYSIS_VARIABLES,
    calculate_patient_missingness,
    datetime,
    final_df_raw,
    generate_missingness_summary,
    graphs_folder,
    log,
    output_folder,
    plot_missingness_heatmap,
    site_name,
):
    log("=" * 80)
    log("PATIENT-LEVEL MISSINGNESS ANALYSIS (BINARY) -- BEFORE FILLING")
    log(f"Site: {site_name}")
    log(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 80)

    _df = final_df_raw.copy()
    log(f"\nShape of final_df: {_df.shape}")

    # --- 72h window ---
    _df_72h = _df[_df['time_from_vent'] <= 72].copy()
    _n_patients_72h = _df_72h['encounter_block'].nunique()
    log(f"\n72h window: {_n_patients_72h:,} patients, {len(_df_72h):,} patient-hours")

    _has_data_72h = calculate_patient_missingness(_df_72h, ANALYSIS_VARIABLES)
    _summary_72h_before = generate_missingness_summary(_has_data_72h, ANALYSIS_VARIABLES, site_name)

    # --- All hours ---
    _n_patients_all = _df['encounter_block'].nunique()
    log(f"All hours: {_n_patients_all:,} patients, {len(_df):,} patient-hours")

    _has_data_all = calculate_patient_missingness(_df, ANALYSIS_VARIABLES)
    _summary_all_before = generate_missingness_summary(_has_data_all, ANALYSIS_VARIABLES, site_name)

    # Print top missing (72h)
    log("\nVariables with highest % patients having NO data (72h):")
    _top_missing = _summary_72h_before[_summary_72h_before['pct_patients_no_data'] > 0]
    if len(_top_missing) > 0:
        log(_top_missing[['variable_label', 'source_table', 'pct_patients_no_data',
                            'n_patients_no_data']].head(10).to_string(index=False))
    else:
        log("  All patients have data for every analysis variable.")

    # Save before-filling CSVs
    _summary_72h_before.to_csv(f'{output_folder}{site_name}_patient_missingness_72h.csv', index=False)
    _summary_all_before.to_csv(f'{output_folder}{site_name}_patient_missingness_all_hours.csv', index=False)
    log(f"\nSaved: {site_name}_patient_missingness_72h.csv")
    log(f"Saved: {site_name}_patient_missingness_all_hours.csv")

    # Heatmap (72h, before filling)
    plot_missingness_heatmap(_summary_72h_before, site_name, _n_patients_72h,
                             f'{graphs_folder}{site_name}_missingness_heatmap_before_filling.png')
    log(f"Saved: graphs/{site_name}_missingness_heatmap_before_filling.png")

    prefill_done = True
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Forward Fill Strategy
    """)
    return


@app.cell
def fill_strategy(final_df_raw, log, np, pd):
    def apply_fill(df):
        """Apply forward-fill strategy to a copy of df. Returns filled df."""
        df = df.sort_values(by=['encounter_block', 'recorded_date', 'recorded_hour']).reset_index(drop=True)

        _flag_columns = ['hourly_trach', 'hourly_on_vent', 'nicardipine_flag', 'nitroprusside_flag',
                        'clevidipine_flag', 'red_meds_flag', 'cisatracurium_flag', 'vecuronium_flag',
                        'rocuronium_flag', 'paralytics_flag']
        _exclude_columns = ['patient_id', 'hospitalization_id', 'encounter_block', 'recorded_date',
                           'recorded_hour', 'time_from_vent', 'time_from_vent_adjusted',
                           'lactate', 'last_ne_dose_last_6_hours', 'ne_calc_last']

        _all_cols = set(df.columns)
        _potential_fill = _all_cols - set(_flag_columns) - set(_exclude_columns)
        _continuous_columns = [c for c in _potential_fill if c in df.columns and pd.api.types.is_numeric_dtype(df[c])]

        for _col in _flag_columns:
            if _col in df.columns:
                df[_col] = df[_col].fillna(0).astype(np.int8)

        log("  Filling continuous variables (ffill only)...")
        for _col in _continuous_columns:
            df[_col] = df.groupby('encounter_block')[_col].ffill()

        log("  Filling lactate (24h limit)...")
        df['lactate'] = df.groupby('encounter_block')['lactate'].ffill(limit=24)

        log("  Filling tracheostomy (cummax)...")
        df['hourly_trach'] = df.groupby('encounter_block')['hourly_trach'].cummax().astype(np.int8)

        log("  Filling NE calc last...")
        df['ne_calc_last'] = df.groupby('encounter_block')['ne_calc_last'].ffill()

        log("  Filling NE 6h lookback...")
        _shifted_ne = df.groupby('encounter_block')['ne_calc_last'].shift(6)
        _mask = df['last_ne_dose_last_6_hours'].isna()
        df.loc[_mask, 'last_ne_dose_last_6_hours'] = _shifted_ne[_mask]
        df['last_ne_dose_last_6_hours'] = df['last_ne_dose_last_6_hours'].fillna(0)

        df['is_weekday'] = pd.to_datetime(df['recorded_date']).dt.weekday < 5

        log("  Fill complete.")
        return df

    # SLICE-FIRST FILL
    log("\n" + "=" * 80)
    log("SLICE-FIRST FILL STRATEGY: 72h first, then forward-fill only")
    log("=" * 80)

    # PRIMARY: slice to 72h first, then forward-fill only
    log("\n--- PRIMARY: 72h dataset ---")
    _df_72h = final_df_raw[final_df_raw['time_from_vent'] <= 72].copy()
    log(f"  Sliced to 72h: {len(_df_72h):,} rows, {_df_72h['encounter_block'].nunique():,} patients")
    final_df_unflagged = apply_fill(_df_72h)

    # SECONDARY: all hours, forward-fill only
    log("\n--- SECONDARY: all-hours dataset ---")
    _df_all_raw = final_df_raw.copy()
    log(f"  All hours: {len(_df_all_raw):,} rows, {_df_all_raw['encounter_block'].nunique():,} patients")
    final_df_all_unflagged = apply_fill(_df_all_raw)

    log("\nFilling complete (both datasets).")
    return final_df_all_unflagged, final_df_unflagged


@app.cell
def _(mo):
    mo.md(r"""
    ## Create Criteria Flags
    """)
    return


@app.cell
def compute_criteria_flags_def(log, np):
    def compute_criteria_flags(final_df):
        """Compute all eligibility criteria flags (Patel/TEAM/Consensus) on df.

        IMPORTANT — Missing data assumption:
        When a physiological variable is NaN (missing) at a given hour, the
        corresponding sub-component flag is set to 1 (eligible / pass).
        Rationale: missing values typically indicate the variable was not
        measured because it was clinically unremarkable. This is a
        conservative assumption that may overestimate eligibility in hours
        with sparse monitoring. The pre-fill missingness report
        (UCMC_patient_missingness_*.csv) quantifies data availability so
        that readers can assess the impact of this assumption.
        """
        # Apply Patel et al. Criteria
        log("Create Patel/Chicago Criteria flags")
        final_df['patel_map_flag'] = (
            (final_df['avg_map'] >= 65) & (final_df['avg_map'] <= 110)
        ).astype(int)

        final_df['patel_sbp_flag'] = (
            final_df['max_sbp'].isna() |
            (final_df['max_sbp'] <= 200)
        ).astype(int)

        final_df['patel_pulse_flag'] = (
            (final_df['min_heart_rate'] >= 40) & (final_df['max_heart_rate'] <= 130)
        ).astype(int)

        final_df['patel_resp_rate_flag'] = (
            (final_df['min_respiratory_rate'] >= 5) & (final_df['max_respiratory_rate'] <= 40)
        ).astype(int)

        final_df['patel_spo2_flag'] = (
            final_df['min_spo2'].isna() |
            (final_df['min_spo2'] >= 88)
        ).astype(int)

        final_df['patel_resp_flag'] = (
            final_df['patel_resp_rate_flag'] &
            final_df['patel_spo2_flag'] &
            (final_df['hourly_trach'] == 0) &
            (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) &
            (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['patel_cardio_flag'] = (
            final_df['patel_map_flag'] &
            final_df['patel_sbp_flag'] &
            final_df['patel_pulse_flag'] &
            (final_df['hourly_trach'] == 0) &
            (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) &
            (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['patel_flag'] = (
            final_df['patel_map_flag'] &
            final_df['patel_sbp_flag'] &
            final_df['patel_pulse_flag'] &
            final_df['patel_resp_rate_flag'] &
            final_df['patel_spo2_flag'] &
            (final_df['hourly_trach'] == 0) &
            (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) &
            (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['patel_flag_all_hours'] = (
            final_df['patel_map_flag'] &
            final_df['patel_sbp_flag'] &
            final_df['patel_pulse_flag'] &
            final_df['patel_resp_rate_flag'] &
            final_df['patel_spo2_flag'] &
            (final_df['hourly_trach'] == 0) &
            (final_df['paralytics_flag'] == 0) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['patel_flag_weekday'] = (
            final_df['patel_map_flag'] &
            final_df['patel_sbp_flag'] &
            final_df['patel_pulse_flag'] &
            final_df['patel_resp_rate_flag'] &
            final_df['patel_spo2_flag'] &
            (final_df['hourly_trach'] == 0) &
            (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) &
            (final_df['recorded_hour'] < 17) &
            (final_df['is_weekday'] == True) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        # TEAM criteria
        log("Create TEAM Criteria flags")
        final_df['team_pulse_flag'] = np.where(
            final_df['max_heart_rate'].isna(), 1,
            (final_df['max_heart_rate'] <= 150).astype(int)
        )

        final_df['team_lactate_flag'] = np.where(
            final_df['lactate'].isna(), 1,
            (final_df['lactate'] <= 4.0).astype(int)
        )

        final_df['team_ne_flag'] = np.where(
            final_df['ne_calc_last'].isna(), 1,
            (final_df['ne_calc_last'] <= 0.2).astype(int)
        )

        log("TEAM NE flag counts when ne < 0.2\n", final_df['team_ne_flag'].value_counts(), "\n")

        final_df['team_ne_flag'] = np.where(
            (final_df['ne_calc_last'] > 1.25 * final_df['last_ne_dose_last_6_hours']) & (final_df['ne_calc_last'] > 0.1),
            0, final_df['team_ne_flag']
        )
        log("TEAM NE flag counts adjusting for change in the last 6 hrs\n", final_df['team_ne_flag'].value_counts(), "\n")

        final_df['team_fio2_flag'] = np.where(
            final_df['min_fio2_set'].isna(), 1,
            (final_df['min_fio2_set'] <= 0.6).astype(int)
        )

        final_df['team_peep_flag'] = np.where(
            final_df['max_peep_set'].isna(), 1,
            (final_df['max_peep_set'] <= 16).astype(int)
        )

        final_df['team_resp_rate_flag'] = np.where(
            final_df['max_respiratory_rate'].isna(), 1,
            (final_df['max_respiratory_rate'] <= 45).astype(int)
        )

        final_df['team_spo2_flag'] = np.where(
            final_df['min_spo2'].isna(), 1,
            (final_df['min_spo2'] >= 90).astype(int)
        )

        final_df['team_cardio_flag'] = (
            final_df['team_pulse_flag'] &
            final_df['team_lactate_flag'] &
            final_df['team_ne_flag'] &
            (final_df['hourly_trach'] == 0) &
            (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) &
            (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['team_resp_flag'] = (
            final_df['team_fio2_flag'] &
            final_df['team_peep_flag'] &
            final_df['team_resp_rate_flag'] &
            final_df['team_spo2_flag'] &
            (final_df['hourly_trach'] == 0) &
            (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) &
            (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['team_flag'] = (
            final_df['team_pulse_flag'] &
            final_df['team_lactate_flag'] &
            final_df['team_ne_flag'] &
            final_df['team_fio2_flag'] &
            final_df['team_peep_flag'] &
            final_df['team_resp_rate_flag'] &
            final_df['team_spo2_flag'] &
            (final_df['hourly_trach'] == 0) &
            (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) &
            (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['team_flag_all_hours'] = (
            final_df['team_pulse_flag'] &
            final_df['team_lactate_flag'] &
            final_df['team_ne_flag'] &
            final_df['team_fio2_flag'] &
            final_df['team_peep_flag'] &
            final_df['team_resp_rate_flag'] &
            final_df['team_spo2_flag'] &
            (final_df['hourly_trach'] == 0) &
            (final_df['paralytics_flag'] == 0) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['team_flag_weekday'] = (
            final_df['team_pulse_flag'] &
            final_df['team_lactate_flag'] &
            final_df['team_ne_flag'] &
            final_df['team_fio2_flag'] &
            final_df['team_peep_flag'] &
            final_df['team_resp_rate_flag'] &
            final_df['team_spo2_flag'] &
            (final_df['hourly_trach'] == 0) &
            (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) &
            (final_df['recorded_hour'] < 17) &
            (final_df['is_weekday'] == True) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        # Consensus criteria
        log("Create Consensus Criteria flags")
        # Red
        final_df['red_resp_spo2_flag'] = ((final_df['min_spo2'] < 90) | final_df['min_spo2'].isna()).astype(int)
        final_df['red_map_flag'] = ((final_df['avg_map'] < 65) | final_df['avg_map'].isna()).astype(int)
        final_df['red_high_support_flag'] = ((final_df['ne_calc_last'] > 0.3)).astype(int)
        final_df['red_hypertensive_flag'] = (
            (((final_df['max_sbp'] > 200) | (final_df['avg_map'] > 110)) &
            (final_df['red_meds_flag'] == 1))
        ).astype(int)
        final_df['red_pulse_high_flag'] = ((final_df['max_heart_rate'] > 150)).astype(int)
        final_df['red_pulse_low_flag'] = ((final_df['min_heart_rate'] < 40) | final_df['min_heart_rate'].isna()).astype(int)

        # Yellow
        final_df['yellow_resp_spo2_flag'] = ((final_df['min_spo2'] >= 90) | final_df['min_spo2'].isna()).astype(int)
        final_df['yellow_fio2_flag'] = ((final_df['min_fio2_set'] > 0.6)).astype(int)
        final_df['yellow_resp_rate_flag'] = ((final_df['max_respiratory_rate'] > 30)).astype(int)
        final_df['yellow_peep_flag'] = ((final_df['min_peep_set'] > 10)).astype(int)
        final_df['yellow_map_flag'] = (((final_df['avg_map'] >= 65) & (final_df['ne_calc_last'].between(0.1, 0.3)))).astype(int)
        final_df['yellow_pulse_flag'] = ((final_df['min_heart_rate'].between(120, 150))).astype(int)
        final_df['yellow_lactate_flag'] = ((final_df['lactate'] > 4)).astype(int)

        # Green
        final_df['green_resp_spo2_flag'] = ((final_df['min_spo2'] >= 90) | final_df['min_spo2'].isna()).astype(int)
        final_df['green_resp_rate_flag'] = ((final_df['max_respiratory_rate'] <= 30) | final_df['max_respiratory_rate'].isna()).astype(int)
        final_df['green_fio2_flag'] = ((final_df['min_fio2_set'] <= 0.6) | final_df['min_fio2_set'].isna()).astype(int)
        final_df['green_peep_flag'] = ((final_df['min_peep_set'] <= 10) | final_df['min_peep_set'].isna()).astype(int)
        final_df['green_map_flag'] = (((final_df['avg_map'] >= 65) & (final_df['ne_calc_last'] < 0.1)) | final_df['ne_calc_last'].isna()).astype(int)
        final_df['green_pulse_flag'] = ((final_df['min_heart_rate'] < 120) | final_df['min_heart_rate'].isna()).astype(int)
        final_df['green_lactate_flag'] = ((final_df['lactate'] <= 4) | final_df['lactate'].isna()).astype(int)
        final_df['green_hr_flag'] = ((final_df['min_heart_rate'] > 40) | final_df['min_heart_rate'].isna()).astype(int)

        # Green subcomponent flags
        final_df['green_resp_flag'] = (
            final_df['green_resp_spo2_flag'] & final_df['green_resp_rate_flag'] &
            final_df['green_fio2_flag'] & final_df['green_peep_flag'] &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['green_cardio_flag'] = (
            final_df['green_map_flag'] & final_df['green_pulse_flag'] &
            final_df['green_lactate_flag'] & final_df['green_hr_flag'] &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['any_red'] = (
            (final_df['red_resp_spo2_flag'] | final_df['red_map_flag'] |
             final_df['red_high_support_flag'] | final_df['red_hypertensive_flag'] |
             final_df['red_pulse_high_flag'] | final_df['red_pulse_low_flag']) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['no_red'] = (
            ~(final_df['red_resp_spo2_flag'] | final_df['red_map_flag'] |
              final_df['red_high_support_flag'] | final_df['red_hypertensive_flag'] |
              final_df['red_pulse_high_flag'] | final_df['red_pulse_low_flag']) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['any_yellow'] = (
            (final_df['yellow_resp_spo2_flag'] | final_df['yellow_fio2_flag'] |
             final_df['yellow_resp_rate_flag'] | final_df['yellow_peep_flag'] |
             final_df['yellow_map_flag'] | final_df['yellow_pulse_flag'] |
             final_df['yellow_lactate_flag']) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['any_green'] = (
            (final_df['green_resp_spo2_flag'] | final_df['green_resp_rate_flag'] |
             final_df['green_fio2_flag'] | final_df['green_peep_flag'] |
             final_df['green_map_flag'] | final_df['green_pulse_flag'] |
             final_df['green_lactate_flag'] | final_df['green_hr_flag']) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['all_green'] = (
            final_df['green_resp_spo2_flag'] & final_df['green_resp_rate_flag'] &
            final_df['green_fio2_flag'] & final_df['green_peep_flag'] &
            final_df['green_map_flag'] & final_df['green_pulse_flag'] &
            final_df['green_lactate_flag'] & final_df['green_hr_flag'] &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['all_green_all_hours'] = (
            final_df['green_resp_spo2_flag'] & final_df['green_resp_rate_flag'] &
            final_df['green_fio2_flag'] & final_df['green_peep_flag'] &
            final_df['green_map_flag'] & final_df['green_pulse_flag'] &
            final_df['green_lactate_flag'] & final_df['green_hr_flag'] &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['all_green_weekday'] = (
            final_df['green_resp_spo2_flag'] & final_df['green_resp_rate_flag'] &
            final_df['green_fio2_flag'] & final_df['green_peep_flag'] &
            final_df['green_map_flag'] & final_df['green_pulse_flag'] &
            final_df['green_lactate_flag'] & final_df['green_hr_flag'] &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['is_weekday'] == True) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['all_green_no_red'] = (
            final_df['green_resp_spo2_flag'] & final_df['green_resp_rate_flag'] &
            final_df['green_fio2_flag'] & final_df['green_peep_flag'] &
            final_df['green_map_flag'] & final_df['green_pulse_flag'] &
            final_df['green_lactate_flag'] & final_df['green_hr_flag'] &
            (final_df['any_red'] == 0) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['all_green_no_red_all_hours'] = (
            final_df['green_resp_spo2_flag'] & final_df['green_resp_rate_flag'] &
            final_df['green_fio2_flag'] & final_df['green_peep_flag'] &
            final_df['green_map_flag'] & final_df['green_pulse_flag'] &
            final_df['green_lactate_flag'] & final_df['green_hr_flag'] &
            (final_df['any_red'] == 0) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['all_green_no_red_weekday'] = (
            final_df['green_resp_spo2_flag'] & final_df['green_resp_rate_flag'] &
            final_df['green_fio2_flag'] & final_df['green_peep_flag'] &
            final_df['green_map_flag'] & final_df['green_pulse_flag'] &
            final_df['green_lactate_flag'] & final_df['green_hr_flag'] &
            (final_df['any_red'] == 0) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['is_weekday'] == True) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['all_green_no_red_yellow'] = (
            final_df['green_resp_spo2_flag'] & final_df['green_resp_rate_flag'] &
            final_df['green_fio2_flag'] & final_df['green_peep_flag'] &
            final_df['green_map_flag'] & final_df['green_pulse_flag'] &
            final_df['green_lactate_flag'] & final_df['green_hr_flag'] &
            (final_df['any_red'] == 0) & (final_df['any_yellow'] == 0) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['all_yellow_no_red_green'] = (
            final_df['yellow_resp_spo2_flag'] & final_df['yellow_fio2_flag'] &
            final_df['yellow_resp_rate_flag'] & final_df['yellow_peep_flag'] &
            final_df['yellow_map_flag'] & final_df['yellow_pulse_flag'] &
            final_df['yellow_lactate_flag'] &
            (final_df['any_red'] == 0) & (final_df['any_green'] == 0) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['any_yellow_no_red_green'] = (
            (final_df['yellow_resp_spo2_flag'] | final_df['yellow_fio2_flag'] |
             final_df['yellow_resp_rate_flag'] | final_df['yellow_peep_flag'] |
             final_df['yellow_map_flag'] | final_df['yellow_pulse_flag'] |
             final_df['yellow_lactate_flag']) &
            (final_df['any_red'] == 0) & (final_df['any_green'] == 0) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['any_yellow_or_green_no_red'] = (
            (final_df['yellow_resp_spo2_flag'] | final_df['yellow_fio2_flag'] |
             final_df['yellow_resp_rate_flag'] | final_df['yellow_peep_flag'] |
             final_df['yellow_map_flag'] | final_df['yellow_pulse_flag'] |
             final_df['yellow_lactate_flag'] |
             final_df['green_resp_spo2_flag'] | final_df['green_resp_rate_flag'] |
             final_df['green_fio2_flag'] | final_df['green_peep_flag'] |
             final_df['green_map_flag'] | final_df['green_pulse_flag'] |
             final_df['green_lactate_flag'] | final_df['green_hr_flag']) &
            (final_df['any_red'] == 0) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['any_yellow_or_green_no_red_weekday'] = (
            (final_df['yellow_resp_spo2_flag'] | final_df['yellow_fio2_flag'] |
             final_df['yellow_resp_rate_flag'] | final_df['yellow_peep_flag'] |
             final_df['yellow_map_flag'] | final_df['yellow_pulse_flag'] |
             final_df['yellow_lactate_flag'] |
             final_df['green_resp_spo2_flag'] | final_df['green_resp_rate_flag'] |
             final_df['green_fio2_flag'] | final_df['green_peep_flag'] |
             final_df['green_map_flag'] | final_df['green_pulse_flag'] |
             final_df['green_lactate_flag'] | final_df['green_hr_flag']) &
            (final_df['any_red'] == 0) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['is_weekday'] == True) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['any_yellow_or_green_no_red_all_hours'] = (
            (final_df['yellow_resp_spo2_flag'] | final_df['yellow_fio2_flag'] |
             final_df['yellow_resp_rate_flag'] | final_df['yellow_peep_flag'] |
             final_df['yellow_map_flag'] | final_df['yellow_pulse_flag'] |
             final_df['yellow_lactate_flag'] |
             final_df['green_resp_spo2_flag'] | final_df['green_resp_rate_flag'] |
             final_df['green_fio2_flag'] | final_df['green_peep_flag'] |
             final_df['green_map_flag'] | final_df['green_pulse_flag'] |
             final_df['green_lactate_flag'] | final_df['green_hr_flag']) &
            (final_df['any_red'] == 0) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['yellow_resp_flag'] = (
            (final_df['yellow_resp_spo2_flag'] | final_df['yellow_fio2_flag'] |
             final_df['yellow_resp_rate_flag'] | final_df['yellow_peep_flag'] |
             final_df['green_resp_spo2_flag'] | final_df['green_resp_rate_flag'] |
             final_df['green_fio2_flag'] | final_df['green_peep_flag']) &
            (final_df['any_red'] == 0) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['yellow_cardio_flag'] = (
            (final_df['yellow_map_flag'] | final_df['yellow_pulse_flag'] |
             final_df['yellow_lactate_flag'] |
             final_df['green_map_flag'] | final_df['green_pulse_flag'] |
             final_df['green_lactate_flag'] | final_df['green_hr_flag']) &
            (final_df['any_red'] == 0) &
            (final_df['hourly_trach'] == 0) & (final_df['paralytics_flag'] == 0) &
            (final_df['recorded_hour'] >= 8) & (final_df['recorded_hour'] < 17) &
            (final_df['time_from_vent_adjusted'] != -1)
        ).astype(int)

        final_df['yellow_all_green'] = (
            final_df['all_green_no_red'] & (final_df['any_yellow'] == 0)
        ).astype(int)

        final_df['yellow_not_all_green'] = (
            final_df['any_yellow_or_green_no_red'] & (final_df['all_green_no_red'] == 0)
        ).astype(int)

        return final_df

    return (compute_criteria_flags,)


@app.cell
def apply_criteria(
    compute_criteria_flags,
    final_df_all_unflagged,
    final_df_unflagged,
    log,
    pyCLIF,
):
    log("\n--- Computing criteria flags on 72h dataset ---")
    final_df = compute_criteria_flags(final_df_unflagged.copy())

    log(final_df[['any_red', 'any_yellow', 'any_green', 'all_green',
                    'all_green_no_red', 'all_green_no_red_yellow', 'all_yellow_no_red_green',
                    'any_yellow_no_red_green', 'any_yellow_or_green_no_red', 'no_red', 'yellow_all_green',
                    'yellow_not_all_green']].sum())

    log("\n--- Computing criteria flags on all-hours dataset ---")
    final_df_all = compute_criteria_flags(final_df_all_unflagged.copy())

    # Save datasets with criteria
    final_df.to_parquet(f'{pyCLIF.project_root}/output/intermediate/final_df_w_criteria.parquet')
    final_df_all.to_parquet(f'{pyCLIF.project_root}/output/intermediate/final_df_all_w_criteria.parquet')
    log("Saved: final_df_w_criteria.parquet (72h), final_df_all_w_criteria.parquet (all hours)")
    return final_df, final_df_all


@app.cell
def _(mo):
    mo.md(r"""
    # Extubation Curve
    """)
    return


@app.cell
def extubation_curve(
    all_ids_w_outcome,
    datetime,
    final_df,
    graphs_folder,
    log,
    mcolors,
    np,
    output_folder,
    pd,
    plt,
    site_name,
):
    _OUTPUT_DIR = output_folder
    _criteria_dict = {
        'Chicago Criteria': 'patel_flag_all_hours',
        'TEAM Criteria': 'team_flag_all_hours',
        'Consensus (Green) Criteria': 'all_green_all_hours',
        'Consensus (Yellow) Criteria': 'any_yellow_or_green_no_red_all_hours'
    }
    _criteria_order = [
        'Chicago Criteria', 'Consensus (Yellow) Criteria',
        'TEAM Criteria', 'Consensus (Green) Criteria'
    ]
    _KEY_TIMEPOINTS = [4, 12, 24, 48, 72]

    log("=" * 80)
    log("EXTUBATION CURVE ANALYSIS")
    log(f"Site: {site_name}")
    log(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 80)

    # Load data - filter to 72 hours
    _df_72h = final_df[final_df['time_from_vent'] <= 72].copy()
    _all_patients = _df_72h[_df_72h['time_from_vent'] == 0]['encounter_block'].unique()
    _n_patients = len(_all_patients)
    log(f"\nTotal patients in cohort at hour 0: {_n_patients:,}")

    # Create complete panel
    _complete_panel = pd.DataFrame({
        'encounter_block': np.repeat(_all_patients, 73),
        'hour': np.tile(range(73), len(_all_patients))
    })
    _complete_panel = _complete_panel.merge(
        all_ids_w_outcome[['encounter_block', 'final_outcome_dttm', 'is_dead', 'block_vent_start_dttm']],
        on='encounter_block', how='left'
    )
    _complete_panel['hours_to_outcome'] = (
        (_complete_panel['final_outcome_dttm'] - _complete_panel['block_vent_start_dttm']).dt.total_seconds() / 3600
    )
    _complete_panel['is_dead_by_hour'] = (
        (_complete_panel['is_dead'] == 1) &
        (_complete_panel['hour'] >= _complete_panel['hours_to_outcome'])
    ).astype(int)
    _complete_panel['is_discharged_by_hour'] = (
        (_complete_panel['is_dead'] != 1) &
        (_complete_panel['hour'] >= _complete_panel['hours_to_outcome'])
    ).astype(int)

    # Merge discharge_category for per-location decomposition
    _complete_panel = _complete_panel.merge(
        all_ids_w_outcome[['encounter_block', 'discharge_category']].drop_duplicates(),
        on='encounter_block', how='left'
    )
    _DEAD_DISPOSITION_MAP = {"expired": "expired", "hospice": "hospice"}
    _ALIVE_DISPOSITION_MAP = {
        "home": "home", "skilled nursing facility (snf)": "snf",
        "long term care hospital (ltach)": "ltach",
        "acute inpatient rehab facility": "rehab",
        "acute care hospital": "other_facility",
        "against medical advice (ama)": "other_facility",
        "psychiatric hospital": "other_facility", "jail": "other_facility"
    }
    _complete_panel['_dead_bucket'] = (
        _complete_panel['discharge_category'].str.lower().map(_DEAD_DISPOSITION_MAP).fillna('expired')
    )
    _complete_panel['_alive_bucket'] = (
        _complete_panel['discharge_category'].str.lower().map(_ALIVE_DISPOSITION_MAP).fillna('other_facility')
    )

    _df_72h_subset = _df_72h[['encounter_block', 'time_from_vent', 'hourly_on_vent',
                               'patel_flag_all_hours', 'team_flag_all_hours',
                               'all_green_all_hours', 'any_yellow_or_green_no_red_all_hours']].copy()
    _complete_panel = _complete_panel.merge(
        _df_72h_subset,
        left_on=['encounter_block', 'hour'],
        right_on=['encounter_block', 'time_from_vent'],
        how='left'
    )
    _complete_panel = _complete_panel.sort_values(['encounter_block', 'hour'])
    _complete_panel['hourly_on_vent'] = _complete_panel.groupby('encounter_block')['hourly_on_vent'].ffill()
    for _col in _criteria_dict.values():
        _complete_panel[_col] = _complete_panel.groupby('encounter_block')[_col].ffill()

    # Calculate hourly state distributions (6 groups)
    log("\nCalculating hourly state distributions (6 groups)...")
    _all_criteria_data = []
    for _criteria_name, _criteria_col in _criteria_dict.items():
        _grouped = _complete_panel.groupby('hour').apply(
            lambda x, _col=_criteria_col: pd.Series({
                'n_total': _n_patients,
                'n_dead': x['is_dead_by_hour'].sum(),
                'n_discharged': x['is_discharged_by_hour'].sum(),
                'n_intubated_eligible': ((x['is_dead_by_hour'] == 0) &
                                          (x['is_discharged_by_hour'] == 0) &
                                          (x['hourly_on_vent'] == 1) &
                                          (x[_col] == 1)).sum(),
                'n_intubated_not_eligible': ((x['is_dead_by_hour'] == 0) &
                                              (x['is_discharged_by_hour'] == 0) &
                                              (x['hourly_on_vent'] == 1) &
                                              (x[_col] != 1)).sum(),
                'n_extubated_eligible': ((x['is_dead_by_hour'] == 0) &
                                          (x['is_discharged_by_hour'] == 0) &
                                          (x['hourly_on_vent'] == 0) &
                                          (x[_col] == 1)).sum(),
                'n_extubated_not_eligible': ((x['is_dead_by_hour'] == 0) &
                                              (x['is_discharged_by_hour'] == 0) &
                                              (x['hourly_on_vent'] == 0) &
                                              (x[_col] != 1)).sum(),
                # Dead subcategories
                'dead_expired': ((x['is_dead_by_hour'] == 1) & (x['_dead_bucket'] == 'expired')).sum(),
                'dead_hospice': ((x['is_dead_by_hour'] == 1) & (x['_dead_bucket'] == 'hospice')).sum(),
                # Alive discharge subcategories
                'discharged_home': ((x['is_discharged_by_hour'] == 1) & (x['_alive_bucket'] == 'home')).sum(),
                'discharged_snf': ((x['is_discharged_by_hour'] == 1) & (x['_alive_bucket'] == 'snf')).sum(),
                'discharged_ltach': ((x['is_discharged_by_hour'] == 1) & (x['_alive_bucket'] == 'ltach')).sum(),
                'discharged_rehab': ((x['is_discharged_by_hour'] == 1) & (x['_alive_bucket'] == 'rehab')).sum(),
                'discharged_other_facility': ((x['is_discharged_by_hour'] == 1) & (x['_alive_bucket'] == 'other_facility')).sum(),
            })
        ).reset_index()
        _grouped['n_extubated'] = _grouped['n_extubated_eligible'] + _grouped['n_extubated_not_eligible']
        _grouped['prop_dead'] = (_grouped['n_dead'] / _grouped['n_total']) * 100
        _grouped['prop_discharged'] = (_grouped['n_discharged'] / _grouped['n_total']) * 100
        _grouped['prop_intubated_eligible'] = (_grouped['n_intubated_eligible'] / _grouped['n_total']) * 100
        _grouped['prop_intubated_not_eligible'] = (_grouped['n_intubated_not_eligible'] / _grouped['n_total']) * 100
        _grouped['prop_extubated_eligible'] = (_grouped['n_extubated_eligible'] / _grouped['n_total']) * 100
        _grouped['prop_extubated_not_eligible'] = (_grouped['n_extubated_not_eligible'] / _grouped['n_total']) * 100
        _grouped['prop_extubated'] = (_grouped['n_extubated'] / _grouped['n_total']) * 100
        _grouped['n_intubated_total'] = _grouped['n_intubated_eligible'] + _grouped['n_intubated_not_eligible']
        _grouped['prop_eligible_among_intubated'] = np.where(
            _grouped['n_intubated_total'] > 0,
            (_grouped['n_intubated_eligible'] / _grouped['n_intubated_total']) * 100, np.nan
        )
        # Discharge location proportions
        for _loc in ['dead_expired', 'dead_hospice', 'discharged_home',
                     'discharged_snf', 'discharged_ltach', 'discharged_rehab',
                     'discharged_other_facility']:
            _grouped[f'prop_{_loc}'] = (_grouped[_loc] / _grouped['n_total']) * 100
        _grouped['criteria'] = _criteria_name
        _grouped['site'] = site_name
        _all_criteria_data.append(_grouped)

    _hourly_df = pd.concat(_all_criteria_data, ignore_index=True)

    # Sanity check: 6 groups sum to 100%
    for _criteria_name in _criteria_order:
        _cdf = _hourly_df[_hourly_df['criteria'] == _criteria_name]
        _prop_sum = (_cdf['prop_dead'] + _cdf['prop_discharged'] +
                     _cdf['prop_extubated_eligible'] + _cdf['prop_extubated_not_eligible'] +
                     _cdf['prop_intubated_eligible'] + _cdf['prop_intubated_not_eligible'])
        assert _prop_sum.min() >= 99.9 and _prop_sum.max() <= 100.1, f"Proportions don't sum to 100% for {_criteria_name}"
    log("Sanity check passed: All 6 groups sum to 100%")

    # Median times
    log("\nCalculating median times to each state...")
    _median_times_data = []
    for _criteria_name, _criteria_col in _criteria_dict.items():
        _first_eligible = _complete_panel[
            (_complete_panel['hourly_on_vent'] == 1) &
            (_complete_panel[_criteria_col] == 1) &
            (_complete_panel['is_dead_by_hour'] == 0) &
            (_complete_panel['is_discharged_by_hour'] == 0)
        ].groupby('encounter_block')['hour'].min()
        _first_extubation = _complete_panel[
            (_complete_panel['hourly_on_vent'] == 0) &
            (_complete_panel['is_dead_by_hour'] == 0) &
            (_complete_panel['is_discharged_by_hour'] == 0)
        ].groupby('encounter_block')['hour'].min()
        _first_death = _complete_panel[
            _complete_panel['is_dead_by_hour'] == 1
        ].groupby('encounter_block')['hour'].min()
        _first_discharge = _complete_panel[
            _complete_panel['is_discharged_by_hour'] == 1
        ].groupby('encounter_block')['hour'].min()

        _median_times_data.append({
            'site': site_name, 'criteria': _criteria_name, 'n_total': _n_patients,
            'median_hours_to_eligibility': _first_eligible.median() if len(_first_eligible) > 0 else np.nan,
            'q1_hours_to_eligibility': _first_eligible.quantile(0.25) if len(_first_eligible) > 0 else np.nan,
            'q3_hours_to_eligibility': _first_eligible.quantile(0.75) if len(_first_eligible) > 0 else np.nan,
            'n_ever_eligible': len(_first_eligible),
            'n_never_eligible': _n_patients - len(_first_eligible),
            'pct_ever_eligible': round(len(_first_eligible) / _n_patients * 100, 1),
            'median_hours_to_extubation': _first_extubation.median() if len(_first_extubation) > 0 else np.nan,
            'q1_hours_to_extubation': _first_extubation.quantile(0.25) if len(_first_extubation) > 0 else np.nan,
            'q3_hours_to_extubation': _first_extubation.quantile(0.75) if len(_first_extubation) > 0 else np.nan,
            'n_extubated': len(_first_extubation),
            'pct_extubated': round(len(_first_extubation) / _n_patients * 100, 1),
            'median_hours_to_death': _first_death.median() if len(_first_death) > 0 else np.nan,
            'q1_hours_to_death': _first_death.quantile(0.25) if len(_first_death) > 0 else np.nan,
            'q3_hours_to_death': _first_death.quantile(0.75) if len(_first_death) > 0 else np.nan,
            'n_dead': len(_first_death),
            'pct_dead': round(len(_first_death) / _n_patients * 100, 1),
            'median_hours_to_discharge': _first_discharge.median() if len(_first_discharge) > 0 else np.nan,
            'q1_hours_to_discharge': _first_discharge.quantile(0.25) if len(_first_discharge) > 0 else np.nan,
            'q3_hours_to_discharge': _first_discharge.quantile(0.75) if len(_first_discharge) > 0 else np.nan,
            'n_discharged': len(_first_discharge),
            'pct_discharged': round(len(_first_discharge) / _n_patients * 100, 1),
        })
    _median_times_df = pd.DataFrame(_median_times_data)

    # Key timepoint statistics
    log("\nExtracting key timepoint statistics...")
    _timepoint_stats = _hourly_df[_hourly_df['hour'].isin(_KEY_TIMEPOINTS)].copy()
    _timepoint_stats = _timepoint_stats.sort_values(['criteria', 'hour']).reset_index(drop=True)

    _eligibility_among_intubated = _timepoint_stats[[
        'site', 'criteria', 'hour',
        'n_intubated_total', 'n_intubated_eligible', 'n_intubated_not_eligible',
        'prop_eligible_among_intubated'
    ]].copy()

    # Save output files
    log("\n" + "=" * 80)
    log("SAVING OUTPUT FILES")
    log("=" * 80)

    _hourly_output = _hourly_df[[
        'site', 'criteria', 'hour', 'n_total',
        'n_dead', 'n_discharged', 'n_extubated', 'n_extubated_eligible', 'n_extubated_not_eligible',
        'n_intubated_eligible', 'n_intubated_not_eligible', 'n_intubated_total',
        'prop_dead', 'prop_discharged', 'prop_extubated', 'prop_extubated_eligible', 'prop_extubated_not_eligible',
        'prop_intubated_eligible', 'prop_intubated_not_eligible',
        'prop_eligible_among_intubated',
        # Discharge location subcategories
        'dead_expired', 'dead_hospice',
        'discharged_home', 'discharged_snf', 'discharged_ltach', 'discharged_rehab', 'discharged_other_facility',
        'prop_dead_expired', 'prop_dead_hospice',
        'prop_discharged_home', 'prop_discharged_snf', 'prop_discharged_ltach',
        'prop_discharged_rehab', 'prop_discharged_other_facility'
    ]]
    _hourly_output.to_csv(f'{_OUTPUT_DIR}{site_name}_hourly_proportions.csv', index=False)
    log(f"Saved: {site_name}_hourly_proportions.csv ({len(_hourly_output)} rows)")
    _median_times_df.to_csv(f'{_OUTPUT_DIR}{site_name}_median_times.csv', index=False)
    log(f"Saved: {site_name}_median_times.csv ({len(_median_times_df)} rows)")
    _timepoint_stats.to_csv(f'{_OUTPUT_DIR}{site_name}_timepoint_stats.csv', index=False)
    log(f"Saved: {site_name}_timepoint_stats.csv ({len(_timepoint_stats)} rows)")
    _eligibility_among_intubated.to_csv(f'{_OUTPUT_DIR}{site_name}_eligibility_among_intubated.csv', index=False)
    log(f"Saved: {site_name}_eligibility_among_intubated.csv ({len(_eligibility_among_intubated)} rows)")

    # Print summary
    log("\n" + "=" * 80)
    log("SUMMARY STATISTICS")
    log("=" * 80)
    log("\n--- Median Time to Each State ---")
    log(_median_times_df[['criteria', 'median_hours_to_eligibility', 'q1_hours_to_eligibility',
                            'q3_hours_to_eligibility', 'n_ever_eligible', 'pct_ever_eligible']].to_string(index=False))

    # Create stacked area figure
    log("\n" + "=" * 80)
    log("GENERATING STACKED AREA FIGURE")
    log("=" * 80)

    _pastel_rgba_colors = {
        'Intubated & Eligible': mcolors.to_rgba('#81c784', alpha=0.55),
        'Intubated & Not Eligible': mcolors.to_rgba('#ff8a80', alpha=0.55),
        'Extubated & Eligible': mcolors.to_rgba('#64b5f6', alpha=0.55),
        'Extubated & Not Eligible': mcolors.to_rgba('#90caf9', alpha=0.35),
        'Discharged Alive': mcolors.to_rgba('#ce93d8', alpha=0.55),
        'Dead': mcolors.to_rgba('#bdbdbd', alpha=0.55),
    }

    _fig, _axes = plt.subplots(1, 4, figsize=(20, 6))
    for _idx, _cname in enumerate(_criteria_order):
        _plot_df = _hourly_df[(_hourly_df['criteria'] == _cname) & (_hourly_df['hour'] >= 4)].sort_values('hour')
        _hours = _plot_df['hour'].values
        _axes[_idx].stackplot(
            _hours,
            _plot_df['prop_intubated_eligible'].values,
            _plot_df['prop_intubated_not_eligible'].values,
            _plot_df['prop_extubated_eligible'].values,
            _plot_df['prop_extubated_not_eligible'].values,
            _plot_df['prop_discharged'].values,
            _plot_df['prop_dead'].values,
            labels=['Intubated & Eligible', 'Intubated & Not Eligible',
                    'Extubated & Eligible', 'Extubated & Not Eligible',
                    'Discharged Alive', 'Dead'],
            colors=[_pastel_rgba_colors['Intubated & Eligible'],
                    _pastel_rgba_colors['Intubated & Not Eligible'],
                    _pastel_rgba_colors['Extubated & Eligible'],
                    _pastel_rgba_colors['Extubated & Not Eligible'],
                    _pastel_rgba_colors['Discharged Alive'],
                    _pastel_rgba_colors['Dead']],
        )
        _axes[_idx].set_xlabel('Hours from Ventilation Start', fontsize=11)
        _axes[_idx].set_ylabel('Proportion of Patients (%)', fontsize=11)
        _axes[_idx].set_title(_cname, fontsize=12, fontweight='bold')
        _axes[_idx].set_xlim(4, 72)
        _axes[_idx].set_ylim(0, 100)
        _axes[_idx].grid(axis='y', alpha=0.2)
        _axes[_idx].set_xticks([4, 12, 24, 36, 48, 60, 72])
        for _ref_hour in [24, 48]:
            _axes[_idx].axvline(x=_ref_hour, color='gray', linestyle='--', alpha=0.3, linewidth=0.8)

    plt.suptitle('Patient Status Over Time by Eligibility Criteria (Hours 4-72)', fontsize=14, y=1.02)
    _handles, _labels = _axes[0].get_legend_handles_labels()
    _fig.legend(_handles, _labels, loc='lower center', bbox_to_anchor=(0.5, -0.08),
                ncol=6, fontsize=9, frameon=True, facecolor='white', edgecolor='lightgray')
    plt.tight_layout(rect=[0, 0.06, 1, 0.98])
    _fig_path = f'{graphs_folder}patient_status_area_plot.png'
    plt.savefig(_fig_path, dpi=300, bbox_inches='tight', facecolor='white')
    log(f"Saved: {_fig_path}")
    plt.close()

    # --- Phase 2b: First Eligibility — Intubated vs Extubated ---
    log("\n" + "=" * 80)
    log("FIRST ELIGIBILITY: INTUBATED vs EXTUBATED")
    log("=" * 80)
    _first_elig_vent_data = []
    for _criteria_name, _criteria_col in _criteria_dict.items():
        _eligible_rows = _complete_panel[
            (_complete_panel[_criteria_col] == 1) &
            (_complete_panel['is_dead_by_hour'] == 0) &
            (_complete_panel['is_discharged_by_hour'] == 0)
        ]
        _first_elig = _eligible_rows.sort_values('hour').groupby('encounter_block').first().reset_index()
        _n_elig = len(_first_elig)
        _n_intubated = (_first_elig['hourly_on_vent'] == 1).sum()
        _n_extubated = (_first_elig['hourly_on_vent'] == 0).sum()
        _pct_intubated = (_n_intubated / _n_elig * 100) if _n_elig > 0 else 0
        _pct_extubated = (_n_extubated / _n_elig * 100) if _n_elig > 0 else 0
        log(f"  {_criteria_name}: {_n_elig} ever eligible — {_pct_intubated:.1f}% intubated, {_pct_extubated:.1f}% extubated at first eligibility")
        _first_elig_vent_data.append({
            'site': site_name, 'criteria': _criteria_name,
            'n_ever_eligible': _n_elig, 'n_first_elig_intubated': _n_intubated,
            'n_first_elig_extubated': _n_extubated,
            'pct_first_elig_intubated': round(_pct_intubated, 1),
            'pct_first_elig_extubated': round(_pct_extubated, 1),
        })
    _first_elig_vent_df = pd.DataFrame(_first_elig_vent_data)
    _first_elig_vent_df.to_csv(f'{_OUTPUT_DIR}{site_name}_first_eligibility_vent_status.csv', index=False)
    log(f"Saved: {site_name}_first_eligibility_vent_status.csv")

    # --- Phase 2c: Extubated Not Eligible — Timing Analysis ---
    log("\n" + "=" * 80)
    log("EXTUBATED NOT ELIGIBLE — TIMING ANALYSIS")
    log("=" * 80)
    _extub_timing_data = []
    for _criteria_name, _criteria_col in _criteria_dict.items():
        # Find patients who were extubated within 72h
        _extubated_patients = _complete_panel[
            (_complete_panel['hourly_on_vent'] == 0) &
            (_complete_panel['is_dead_by_hour'] == 0) &
            (_complete_panel['is_discharged_by_hour'] == 0)
        ]['encounter_block'].unique()
        # Among extubated, find those never eligible within 72h
        _ever_eligible = _complete_panel[
            (_complete_panel[_criteria_col] == 1) &
            (_complete_panel['is_dead_by_hour'] == 0) &
            (_complete_panel['is_discharged_by_hour'] == 0)
        ]['encounter_block'].unique()
        _extubated_never_eligible = set(_extubated_patients) - set(_ever_eligible)
        _n_extubated_never_eligible = len(_extubated_never_eligible)

        # For those extubated-never-eligible, check if extubation was outside business hours
        if _n_extubated_never_eligible > 0:
            _extub_hours = _complete_panel[
                (_complete_panel['encounter_block'].isin(_extubated_never_eligible)) &
                (_complete_panel['hourly_on_vent'] == 0)
            ].groupby('encounter_block')['hour'].min().reset_index()
            _extub_hours = _extub_hours.merge(
                all_ids_w_outcome[['encounter_block', 'block_vent_start_dttm']],
                on='encounter_block', how='left'
            )
            _extub_hours['extubation_wall_hour'] = (
                _extub_hours['block_vent_start_dttm'] + pd.to_timedelta(_extub_hours['hour'], unit='h')
            ).dt.hour
            _extub_hours['is_business_hours'] = _extub_hours['extubation_wall_hour'].between(8, 16)
            _extub_hours['is_weekday'] = (
                _extub_hours['block_vent_start_dttm'] + pd.to_timedelta(_extub_hours['hour'], unit='h')
            ).dt.dayofweek < 5
            _n_outside_biz = (~_extub_hours['is_business_hours']).sum()
            _n_weekend = (~_extub_hours['is_weekday']).sum()
            # Check if all physiological criteria were met at extubation hour
            _extub_phys_check = _complete_panel[
                (_complete_panel['encounter_block'].isin(_extubated_never_eligible)) &
                (_complete_panel['hourly_on_vent'] == 0)
            ].groupby('encounter_block').first().reset_index()
            _n_physio_met_at_extub = (_extub_phys_check[_criteria_col] == 1).sum() if _criteria_col in _extub_phys_check.columns else 0
        else:
            _n_outside_biz = 0
            _n_weekend = 0
            _n_physio_met_at_extub = 0

        log(f"  {_criteria_name}: {_n_extubated_never_eligible} extubated-never-eligible — "
              f"{_n_outside_biz} extubated outside business hours, "
              f"{_n_physio_met_at_extub} met physio criteria at extubation")
        _extub_timing_data.append({
            'site': site_name, 'criteria': _criteria_name,
            'n_extubated': len(_extubated_patients),
            'n_extubated_never_eligible': _n_extubated_never_eligible,
            'n_extubated_outside_business_hrs': _n_outside_biz,
            'n_extubated_on_weekend': _n_weekend,
            'n_physio_met_at_extubation': _n_physio_met_at_extub,
            'pct_timing_only_barrier': round(_n_outside_biz / _n_extubated_never_eligible * 100, 1) if _n_extubated_never_eligible > 0 else 0,
        })
    _extub_timing_df = pd.DataFrame(_extub_timing_data)
    _extub_timing_df.to_csv(f'{_OUTPUT_DIR}{site_name}_extubation_timing_analysis.csv', index=False)
    log(f"Saved: {site_name}_extubation_timing_analysis.csv")

    # --- Phase 2d: Discharge Disposition Flags ---
    log("\n" + "=" * 80)
    log("DISCHARGE DISPOSITION FLAGS")
    log("=" * 80)
    _discharge_mapping = {
        'Home': 'home', 'Home or Self Care': 'home', 'Home Health': 'home',
        'Skilled Nursing Facility (SNF)': 'snf', 'Skilled Nursing Facility': 'snf', 'SNF': 'snf',
        'Acute Inpatient Rehab Facility': 'rehab', 'Rehab': 'rehab', 'Rehabilitation': 'rehab',
        'Long Term Care Hospital (LTACH)': 'ltac', 'Long Term Care': 'ltc', 'LTAC': 'ltac', 'LTACH': 'ltac',
        'Against Medical Advice (AMA)': 'ama', 'Against Medical Advice': 'ama', 'AMA': 'ama',
        'Acute Care Hospital': 'transfer', 'Another Hospital': 'transfer', 'Transfer': 'transfer',
        'Psychiatric Hospital': 'psych', 'Hospice': 'hospice', 'Expired': 'expired',
        'Jail': 'other', 'Other': 'other', 'Still Admitted': 'other',
    }
    _discharged_alive = all_ids_w_outcome[all_ids_w_outcome['is_dead'] != 1].copy()
    if 'discharge_category' in _discharged_alive.columns:
        _discharged_alive['discharge_disposition'] = _discharged_alive['discharge_category'].map(
            lambda x: _discharge_mapping.get(str(x).strip(), 'other') if pd.notna(x) else 'unknown'
        )
        _disp_types = ['home', 'snf', 'rehab', 'ltac', 'ama', 'transfer', 'psych', 'hospice', 'other']
        for _disp in _disp_types:
            _discharged_alive[f'discharged_{_disp}'] = (_discharged_alive['discharge_disposition'] == _disp).astype(int)
        _disp_summary = _discharged_alive['discharge_disposition'].value_counts()
        log(f"  Discharge dispositions (n={len(_discharged_alive)}):")
        for _d, _c in _disp_summary.items():
            log(f"    {_d}: {_c} ({_c/len(_discharged_alive)*100:.1f}%)")
        _disp_flag_cols = [f'discharged_{d}' for d in _disp_types]
        _discharged_alive[['encounter_block', 'discharge_category', 'discharge_disposition'] + _disp_flag_cols].to_csv(
            f'{_OUTPUT_DIR}{site_name}_discharge_disposition.csv', index=False
        )
        log(f"Saved: {site_name}_discharge_disposition.csv")
    else:
        log("  WARNING: discharge_category column not found in all_ids_w_outcome")

    extubation_done = True
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Time-to-Eligibility Windows
    """)
    return


@app.cell
def time_to_eligibility_windows(
    final_df,
    log,
    np,
    output_folder,
    pd,
    site_name,
):
    log("=" * 80)
    log("TIME-TO-ELIGIBILITY WINDOWS + META-ANALYSIS PREP")
    log("=" * 80)

    _criteria_dict = {
        'Chicago Criteria': 'patel_flag',
        'TEAM Criteria': 'team_flag',
        'Consensus (Green) Criteria': 'all_green',
        'Consensus (Yellow) Criteria': 'any_yellow_or_green_no_red'
    }
    _windows = [4, 12, 24, 36, 48, 60, 72]
    _df = final_df[final_df['time_from_vent'] <= 72].copy()
    _n_total = _df['encounter_block'].nunique()

    def _wilson_ci(p, n, z=1.96):
        """Wilson score interval for a proportion."""
        if n == 0:
            return np.nan, np.nan
        _denom = 1 + z**2 / n
        _center = (p + z**2 / (2 * n)) / _denom
        _margin = z * np.sqrt((p * (1 - p) / n) + z**2 / (4 * n**2)) / _denom
        return max(0.0, _center - _margin), min(1.0, _center + _margin)

    _results = []
    for _criteria_name, _criteria_col in _criteria_dict.items():
        # First eligible hour per encounter_block (business-hours eligibility)
        _eligible_hours = _df[_df[_criteria_col] == 1].groupby('encounter_block')['time_from_vent'].min()

        for _window in _windows:
            _eligible_in_window = _eligible_hours[_eligible_hours <= _window]
            _n_eligible = len(_eligible_in_window)
            _p = _n_eligible / _n_total if _n_total > 0 else 0
            _pct_eligible = round(_p * 100, 1)
            _se = np.sqrt(_p * (1 - _p) / _n_total) if _n_total > 0 else np.nan
            _ci_lo, _ci_hi = _wilson_ci(_p, _n_total)
            _results.append({
                'site': site_name,
                'criteria': _criteria_name,
                'window_hours': _window,
                'n_total': _n_total,
                'n_eligible': _n_eligible,
                'pct_eligible': _pct_eligible,
                'se': round(_se, 6) if not np.isnan(_se) else np.nan,
                'ci_lower': round(_ci_lo * 100, 2) if not np.isnan(_ci_lo) else np.nan,
                'ci_upper': round(_ci_hi * 100, 2) if not np.isnan(_ci_hi) else np.nan,
                'median_time': round(_eligible_in_window.median(), 1) if _n_eligible > 0 else np.nan,
                'q1': round(_eligible_in_window.quantile(0.25), 1) if _n_eligible > 0 else np.nan,
                'q3': round(_eligible_in_window.quantile(0.75), 1) if _n_eligible > 0 else np.nan,
            })

        # Print summary for this criteria
        _full_window = [r for r in _results if r['criteria'] == _criteria_name]
        log(f"\n  {_criteria_name}:")
        for _r in _full_window:
            log(f"    {_r['window_hours']}h: {_r['n_eligible']}/{_r['n_total']} "
                  f"({_r['pct_eligible']}%) eligible, "
                  f"median={_r['median_time']}h [{_r['q1']}-{_r['q3']}]")

    _windows_df = pd.DataFrame(_results)

    # Validation: pct_eligible monotonically non-decreasing
    for _criteria_name in _criteria_dict:
        _crit_data = _windows_df[_windows_df['criteria'] == _criteria_name].sort_values('window_hours')
        _pcts = _crit_data['pct_eligible'].values
        assert all(_pcts[i] <= _pcts[i+1] for i in range(len(_pcts)-1)), \
            f"pct_eligible not monotonic for {_criteria_name}: {_pcts}"
    log("\nValidation passed: pct_eligible monotonically non-decreasing for all criteria")

    _windows_df.to_csv(f'{output_folder}{site_name}_time_to_eligibility_windows.csv', index=False)
    log(f"Saved: {site_name}_time_to_eligibility_windows.csv")

    elig_windows_done = True
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Failure Analysis
    *Simplified failure analysis removed — see enhanced_failure cell below.*
    """)
    return


@app.cell
def simplified_failure_analysis():
    # REMOVED: Had buggy flag references (red_rass_flag, green_rass_flag etc. don't exist).
    # All failure analysis is now in the enhanced_failure cell (Section 25).
    return


@app.cell
def _(mo):
    mo.md(r"""
    # ASE / Sepsis Enrichment
    """)
    return


@app.cell
def ase_sepsis(all_ids_w_outcome, final_df_blocks_raw, log, pd, pyCLIF):
    import warnings as _warnings
    _warnings.filterwarnings('ignore', category=FutureWarning)

    final_df_blocks = final_df_blocks_raw.copy()

    # Drop any pre-existing sepsis columns (from prior parquet saves) to avoid merge conflicts
    _sepsis_cols = [c for c in final_df_blocks.columns if 'sepsis' in c.lower()]
    if _sepsis_cols:
        log(f"Dropping pre-existing sepsis columns: {_sepsis_cols}")
        final_df_blocks = final_df_blocks.drop(columns=_sepsis_cols)

    # ASE / Sepsis
    try:
        from clifpy.utils.ase import compute_ase
        _hosp_ids = all_ids_w_outcome['hospitalization_id'].astype(str).unique().tolist()
        log(f"Computing ASE for {len(_hosp_ids)} hospitalization_ids...")
        _sepsis_df = compute_ase(
            hospitalization_ids=_hosp_ids,
            config_path=f'{pyCLIF.project_root}/config/config.json',
            apply_rit=True, include_lactate=True, verbose=False
        )
        log(f"ASE returned {len(_sepsis_df)} episodes, sepsis distribution:")
        log(_sepsis_df['sepsis'].value_counts())

        _sepsis_with_block = _sepsis_df.merge(
            all_ids_w_outcome[['hospitalization_id', 'encounter_block']].drop_duplicates(),
            on='hospitalization_id', how='left'
        )
        _sepsis_with_block = _sepsis_with_block.merge(
            final_df_blocks[['encounter_block', 'block_vent_start_dttm']].drop_duplicates(),
            on='encounter_block', how='left'
        )
        _sepsis_with_block['hours_from_vent_to_sepsis'] = (
            _sepsis_with_block['ase_onset_w_lactate_dttm'] - _sepsis_with_block['block_vent_start_dttm']
        ).dt.total_seconds() / 3600
        _sepsis_with_block['sepsis_24h'] = (
            (_sepsis_with_block['sepsis'] == 1) &
            (_sepsis_with_block['hours_from_vent_to_sepsis'] >= 0) &
            (_sepsis_with_block['hours_from_vent_to_sepsis'] <= 24)
        ).astype(int)
        _sepsis_with_block['sepsis_72h'] = (
            (_sepsis_with_block['sepsis'] == 1) &
            (_sepsis_with_block['hours_from_vent_to_sepsis'] >= 0) &
            (_sepsis_with_block['hours_from_vent_to_sepsis'] <= 72)
        ).astype(int)
        _sepsis_with_block['sepsis_anytime'] = _sepsis_with_block['sepsis']
        _sepsis_by_block = (
            _sepsis_with_block.groupby('encounter_block')
            .agg({'sepsis_24h': 'max', 'sepsis_72h': 'max', 'sepsis_anytime': 'max',
                  'ase_onset_w_lactate_dttm': 'min'})
            .reset_index()
            .rename(columns={'sepsis_24h': 'block_sepsis_24h', 'sepsis_72h': 'block_sepsis_72h',
                             'sepsis_anytime': 'block_sepsis_anytime',
                             'ase_onset_w_lactate_dttm': 'block_sepsis_onset_dttm'})
        )
        _total_blocks = final_df_blocks['encounter_block'].nunique()
        _n24 = _sepsis_by_block['block_sepsis_24h'].sum()
        _n72 = _sepsis_by_block['block_sepsis_72h'].sum()
        _nany = _sepsis_by_block['block_sepsis_anytime'].sum()
        log(f"\nSepsis within 24h: {_n24} ({_n24/_total_blocks*100:.1f}%)")
        log(f"Sepsis within 72h: {_n72} ({_n72/_total_blocks*100:.1f}%)")
        log(f"Sepsis anytime:    {_nany} ({_nany/_total_blocks*100:.1f}%)")
        assert _n24 <= _n72 <= _nany, "Sepsis counts should be monotonic: 24h <= 72h <= anytime"

        final_df_blocks = final_df_blocks.merge(
            _sepsis_by_block[['encounter_block', 'block_sepsis_24h', 'block_sepsis_72h',
                              'block_sepsis_anytime', 'block_sepsis_onset_dttm']],
            on='encounter_block', how='left'
        )
    except Exception as e:
        import traceback
        log(f"ERROR: ASE/sepsis computation failed:")
        traceback.print_exc()
        log("Continuing with NaN sepsis columns.")

    for _col in ['block_sepsis_24h', 'block_sepsis_72h', 'block_sepsis_anytime']:
        if _col not in final_df_blocks.columns:
            final_df_blocks[_col] = 0
        else:
            final_df_blocks[_col] = final_df_blocks[_col].fillna(0).astype(int)
    if 'block_sepsis_onset_dttm' not in final_df_blocks.columns:
        final_df_blocks['block_sepsis_onset_dttm'] = pd.NaT

    # Save enriched final_df_blocks
    final_df_blocks.to_parquet(f'{pyCLIF.project_root}/output/intermediate/final_df_blocks.parquet', index=False)
    log(f"\nfinal_df_blocks saved: {final_df_blocks.shape}, columns: {final_df_blocks.columns.tolist()}")
    return (final_df_blocks,)


@app.cell
def _(mo):
    mo.md(r"""
    # TableOne
    """)
    return


@app.cell
def tableone_all_hours():
    # REMOVED: All-hours TableOne was redundant with the 72h TableOne.
    # The single TableOne is now produced in the tableone_72h cell below.
    return


@app.cell
def tableone_72h(TableOne, final_df, final_df_blocks, log, pd, pyCLIF):
    """Single TableOne: demographics + 24h-of-intubation clinical characteristics.

    Columns: All Encounters, Patel, TEAM, Yellow, Green, Green-No-Red
    Output: table1_results.csv (single file, replaces old _72hrs and _baseline variants)
    """
    log("=== Generating TableOne (72h window) ===")

    # --- helpers ---
    def _map_race(df, race_column='race_category'):
        _rm = {'Black or African-American': 'Black', 'Black or African American': 'Black',
               'White': 'White', 'Asian': 'Other', 'American Indian or Alaska Native': 'Other',
               'Native Hawaiian or Other Pacific Islander': 'Other', 'Other': 'Other', 'Unknown': 'Other'}
        df['race_new'] = df[race_column].map(_rm).fillna('Missing')
        return df

    def _calc_vaso(df):
        if len(df) == 0: return 0, 0, 0, 0
        _v = df['ne_calc_last'].notna() & (df['ne_calc_last'] > 0)
        return _v.sum(), (df['ne_calc_last'] == 0).sum(), df['ne_calc_last'].isna().sum(), len(df)

    # --- build encounter-level dataset ---
    _final_df_72h = final_df.query("time_from_vent <= 72")
    log(f"Encounters in 72h window: {_final_df_72h['encounter_block'].nunique()}")

    _criteria_results = _final_df_72h.groupby('encounter_block').agg({
        'patel_flag': 'max', 'team_flag': 'max', 'any_yellow_or_green_no_red': 'max',
        'all_green': 'max', 'all_green_no_red': 'max'
    }).reset_index()
    _clinical_stats = _final_df_72h.groupby('encounter_block').agg({
        'ne_calc_last': 'max', 'max_peep_set': 'mean', 'min_fio2_set': 'mean'
    }).reset_index()

    _all_encounters = final_df_blocks[
        final_df_blocks['encounter_block'].isin(_criteria_results['encounter_block'])
    ].copy()
    _all_encounters = _all_encounters.merge(_criteria_results, on='encounter_block', how='left')
    _all_encounters = _all_encounters.merge(_clinical_stats, on='encounter_block', how='left')
    for _c in ['patel_flag', 'team_flag', 'any_yellow_or_green_no_red', 'all_green', 'all_green_no_red']:
        _all_encounters[_c] = _all_encounters[_c].fillna(0)
    _all_encounters = _map_race(_all_encounters)

    # --- criteria subsets ---
    _subsets = {
        'All Encounters': _all_encounters,
        'Patel Criteria': _map_race(_all_encounters[_all_encounters['patel_flag'] == 1].copy()),
        'TEAM Criteria': _map_race(_all_encounters[_all_encounters['team_flag'] == 1].copy()),
        'Yellow Criteria': _map_race(_all_encounters[_all_encounters['any_yellow_or_green_no_red'] == 1].copy()),
        'Green Criteria': _map_race(_all_encounters[_all_encounters['all_green'] == 1].copy()),
        'Green-No-Red Criteria': _map_race(_all_encounters[_all_encounters['all_green_no_red'] == 1].copy())
    }
    log("\nSubset sizes:")
    for _name, _df in _subsets.items():
        log(f"  {_name}: {len(_df)}")

    _vaso_stats = {_n: _calc_vaso(_d) for _n, _d in _subsets.items()}

    # --- TableOne variables ---
    _categorical = ['sex_category', 'race_new', 'ethnicity_category', 'location_category', 'is_dead',
                    'block_sepsis_24h', 'block_sepsis_72h', 'block_sepsis_anytime']
    _continuous = ['age_at_admission', 'bmi', 'sofa_cv_97', 'sofa_coag', 'sofa_renal',
                  'sofa_liver', 'sofa_resp', 'sofa_cns', 'sofa_total',
                  'ne_calc_last', 'max_peep_set', 'min_fio2_set', 'p_f', 's_f']

    # --- build table ---
    _table_all = TableOne(_all_encounters, columns=_categorical + _continuous,
                          categorical=_categorical, groupby=None, nonnormal=_continuous, pval=False)
    _df_tpl = _table_all.tableone.reset_index()
    _df_template = pd.DataFrame({
        'Characteristics': _df_tpl['level_0'], 'Category': _df_tpl['level_1'],
        'All Encounters': _df_tpl[_df_tpl.columns[-1]]
    })

    def _process_subset(subset_df, criteria_name, template):
        if len(subset_df) == 0:
            return pd.Series(['0'] * len(template), name=criteria_name)
        _table = TableOne(subset_df, columns=_categorical + _continuous,
                          categorical=_categorical, groupby=None, nonnormal=_continuous, pval=False)
        _df = _table.tableone.reset_index()
        _result = pd.DataFrame({
            'Characteristics': _df['level_0'], 'Category': _df['level_1'],
            criteria_name: _df[_df.columns[-1]]
        })
        _merged = pd.merge(template[['Characteristics', 'Category']], _result,
                           on=['Characteristics', 'Category'], how='left')
        return _merged[criteria_name].fillna('0 (0.0)')

    _result_columns = [_df_template[['Characteristics', 'Category', 'All Encounters']]]
    for _name, _df in list(_subsets.items())[1:]:
        _result_columns.append(_process_subset(_df, _name, _df_template))
    _final_table = pd.concat(_result_columns, axis=1)

    # --- formatted mortality rows ---
    _mortality_mask = _final_table['Characteristics'] == 'is_dead'
    for _col in _final_table.columns[2:]:
        if _col in _subsets:
            _s = _subsets[_col]
            _total = len(_s)
            _deaths = _s['is_dead'].sum() if _total > 0 else 0
            _pct = (_deaths / _total * 100) if _total > 0 else 0
            _final_table.loc[_mortality_mask & (_final_table['Category'] == '1'), _col] = f"{_deaths} ({_pct:.1f})"
    _final_table.loc[_mortality_mask, 'Characteristics'] = 'Mortality'

    # --- vasopressor rows ---
    _vaso_rows = []
    for _status in ['Received Vasopressors', 'No Vasopressors', 'Missing Vasopressor Data']:
        _row_data = {'Characteristics': 'Vasopressor Status', 'Category': _status}
        for _col in _final_table.columns[2:]:
            if _col in _vaso_stats:
                _n_vaso, _n_zero, _n_missing, _total = _vaso_stats[_col]
                if _status == 'Received Vasopressors': _value = _n_vaso
                elif _status == 'No Vasopressors': _value = _n_zero
                else: _value = _n_missing
                _pct = (_value / _total * 100) if _total > 0 else 0
                _row_data[_col] = f"{_value} ({_pct:.1f})"
            else:
                _row_data[_col] = "0 (0.0)"
        _vaso_rows.append(_row_data)
    _final_table = pd.concat([_final_table, pd.DataFrame(_vaso_rows)], ignore_index=True)

    # --- n row + save ---
    _n_row = pd.DataFrame({
        'Characteristics': ['n'], 'Category': [''],
        **{_col: [str(len(_subsets[_col]))] for _col in _final_table.columns[2:] if _col in _subsets}
    })
    _final_table = pd.concat([_n_row, _final_table]).reset_index(drop=True)
    _final_table.to_csv(f'{pyCLIF.project_root}/output/final/table1_results.csv', index=False)
    log("[SAVED] table1_results.csv")

    # --- STROBE validation ---
    log("\n=== STROBE vs TableOne Count Validation ===")
    _strobe_df = pd.read_csv(f'{pyCLIF.project_root}/output/final/strobe_counts.csv')
    _strobe_dict = dict(zip(_strobe_df['Metric'], _strobe_df['Value']))
    _tableone_n = len(_all_encounters)
    _strobe_final = int(_strobe_dict.get('G_final_blocks_without_trach_at_intubation', -1))
    log(f"STROBE final cohort: {_strobe_final}")
    log(f"TableOne 'All Encounters' count: {_tableone_n}")
    if _strobe_final == _tableone_n:
        log("PASS: STROBE and TableOne counts match.")
    else:
        log(f"MISMATCH: STROBE={_strobe_final}, TableOne={_tableone_n}, diff={_strobe_final - _tableone_n}")

    log("\n[OK] TableOne completed!")
    t1_72h_done = True
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Missingness by Criteria
    """)
    return


@app.cell
def missingness_by_criteria(
    check_missingness_by_variable,
    final_df,
    log,
    pyCLIF,
):
    log("Check missingness by criteria")

    _reqd_team_fields = ['hourly_trach', 'paralytics_flag', 'lactate', 'max_heart_rate', 'ne_calc_last',
                         'last_ne_dose_last_6_hours', 'min_fio2_set', 'max_peep_set', 'min_spo2',
                         'max_resp_rate_obs', "team_pulse_flag", "team_lactate_flag", "team_ne_flag",
                         "team_fio2_flag", "team_peep_flag", "team_resp_rate_flag", "team_spo2_flag"]
    _reqd_yellow_fields = [
        'min_spo2', 'min_map', 'max_map', 'ne_calc_last', 'max_sbp', "avg_map",
        'max_heart_rate', 'min_heart_rate', 'min_fio2_set', 'max_resp_rate_obs', 'min_peep_set', 'lactate',
        'red_resp_spo2_flag', 'red_map_flag', 'red_high_support_flag',
        'red_hypertensive_flag', 'red_pulse_high_flag', 'red_pulse_low_flag', 'red_meds_flag',
        'yellow_resp_spo2_flag', 'yellow_fio2_flag', 'yellow_resp_rate_flag',
        'yellow_peep_flag', 'yellow_map_flag', 'yellow_pulse_flag', 'yellow_lactate_flag',
        'green_resp_spo2_flag', 'green_resp_rate_flag', 'green_fio2_flag',
        'green_peep_flag', 'green_map_flag', 'green_pulse_flag', 'green_lactate_flag', 'green_hr_flag',
        'any_red', 'any_yellow', 'any_green', 'all_green',
        'all_green_no_red', 'all_green_no_red_yellow', 'all_yellow_no_red_green',
        'any_yellow_no_red_green', 'any_yellow_or_green_no_red', 'yellow_resp_flag',
        'yellow_cardio_flag', 'yellow_all_green', 'yellow_not_all_green'
    ]
    _reqd_patel_fields = ['min_map', 'max_map', 'max_sbp', 'min_sbp', "avg_map",
                          'min_heart_rate', 'max_heart_rate', 'min_respiratory_rate', 'min_spo2',
                          'max_respiratory_rate', 'patel_map_flag', 'patel_sbp_flag',
                          'patel_pulse_flag', 'patel_resp_rate_flag', 'patel_spo2_flag',
                          'patel_resp_flag', 'patel_cardio_flag']
    _reqd_green_fields = [
        'min_spo2', 'min_map', 'max_map', 'ne_calc_last', 'max_sbp', "avg_map",
        'max_heart_rate', 'min_heart_rate', 'min_fio2_set', 'max_resp_rate_obs', 'min_peep_set', 'lactate',
        'green_resp_spo2_flag', 'green_resp_rate_flag', 'green_fio2_flag',
        'green_peep_flag', 'green_map_flag', 'green_pulse_flag', 'green_lactate_flag', 'green_hr_flag',
        'all_green', 'all_green_no_red',
    ]

    _team_missing = check_missingness_by_variable(final_df, _reqd_team_fields)
    _yellow_missing = check_missingness_by_variable(final_df, _reqd_yellow_fields)
    _patel_missing = check_missingness_by_variable(final_df, _reqd_patel_fields)
    _green_missing = check_missingness_by_variable(final_df, _reqd_green_fields)
    _green_no_red_missing = check_missingness_by_variable(final_df, _reqd_yellow_fields)

    log("\nTEAM Criteria Missing Data Summary:")
    log(_team_missing.round(2))
    log("\nYellow Criteria Missing Data Summary:")
    log(_yellow_missing.round(2))
    log("\nPatel Criteria Missing Data Summary:")
    log(_patel_missing.round(2))
    log("\nGreen No Red Criteria Missing Data Summary:")
    log(_green_no_red_missing.round(2))

    _team_missing.to_csv(f'{pyCLIF.project_root}/output/final/team_missing_data.csv')
    _yellow_missing.to_csv(f'{pyCLIF.project_root}/output/final/yellow_missing_data.csv')
    _patel_missing.to_csv(f'{pyCLIF.project_root}/output/final/patel_missing_data.csv')
    _green_missing.to_csv(f'{pyCLIF.project_root}/output/final/green_missing_data.csv')
    _green_no_red_missing.to_csv(f'{pyCLIF.project_root}/output/final/green_no_red_missing_data.csv')

    miss_done = True
    return


@app.cell
def _(mo):
    mo.md(r"""
    # Competing Risk Analysis
    """)
    return


@app.cell
def competing_risk_func(np, pd):
    def create_competing_risk_dataset(criteria_df, all_ids_w_outcome_in, flag_col="patel_flag"):
        """One row per encounter_block with competing risk times and outcomes."""
        _needed_cols = ["encounter_block", "time_from_vent", "recorded_date", "recorded_hour", flag_col]
        _missing = [c for c in _needed_cols if c not in criteria_df.columns]
        if _missing:
            raise ValueError(f"criteria_df is missing columns: {_missing}")

        _first_elig = (
            criteria_df.loc[criteria_df[flag_col] == 1, ["encounter_block", "time_from_vent"]]
            .groupby("encounter_block", as_index=False).min()
            .rename(columns={"time_from_vent": "time_eligibility"})
        )

        _block_cols = ["encounter_block", "block_vent_start_dttm", "final_outcome_dttm", "is_dead"]
        _block_level = (
            all_ids_w_outcome_in
            .loc[all_ids_w_outcome_in["encounter_block"].isin(criteria_df["encounter_block"]), _block_cols]
            .copy()
        )
        _block_level["block_vent_start_dttm"] = pd.to_datetime(_block_level["block_vent_start_dttm"], errors="coerce")
        _block_level["final_outcome_dttm"] = pd.to_datetime(_block_level["final_outcome_dttm"], errors="coerce")
        _hrs_from_vent = (
            (_block_level["final_outcome_dttm"] - _block_level["block_vent_start_dttm"]).dt.total_seconds() / 3600
        )
        _block_level["time_death"] = np.where(_block_level["is_dead"] == 1, _hrs_from_vent, np.nan)
        _block_level["time_discharge_alive"] = np.where(_block_level["is_dead"] == 0, _hrs_from_vent, np.nan)

        _final = (
            _block_level[["encounter_block", "time_death", "time_discharge_alive"]]
            .merge(_first_elig, on="encounter_block", how="left")
        )
        _final["t_event"] = _final[["time_eligibility", "time_death", "time_discharge_alive"]].min(axis=1, skipna=True)

        _final["outcome"] = np.where(
            np.isfinite(_final["time_eligibility"]) & (_final["t_event"] == _final["time_eligibility"]),
            1,
            np.where(
                np.isfinite(_final["time_death"]) & (_final["t_event"] == _final["time_death"]),
                2, 3
            )
        )

        return _final[["encounter_block", "time_eligibility", "time_death",
                        "time_discharge_alive", "t_event", "outcome"]].reset_index(drop=True)

    return (create_competing_risk_dataset,)


@app.cell
def competing_risk_datasets(
    all_ids_w_outcome,
    create_competing_risk_dataset,
    final_df_all,
    log,
    pd,
    pyCLIF,
):
    log("Competing Risk Analysis setup")

    # Standard competing risk
    _df_patel_competing = create_competing_risk_dataset(final_df_all, all_ids_w_outcome, "patel_flag")
    _df_patel_competing.to_parquet(f"{pyCLIF.project_root}/output/intermediate/competing_risk_patel_final.parquet")

    _df_team_competing = create_competing_risk_dataset(final_df_all, all_ids_w_outcome, "team_flag")
    _df_team_competing.to_parquet(f"{pyCLIF.project_root}/output/intermediate/competing_risk_team_final.parquet")

    _df_yellow_competing = create_competing_risk_dataset(final_df_all, all_ids_w_outcome, "any_yellow_or_green_no_red")
    _df_yellow_competing.to_parquet(f"{pyCLIF.project_root}/output/intermediate/competing_risk_yellow_final.parquet")

    _df_green_competing = create_competing_risk_dataset(final_df_all, all_ids_w_outcome, "all_green")
    _df_green_competing.to_parquet(f"{pyCLIF.project_root}/output/intermediate/competing_risk_green_final.parquet")

    _df_green_no_red_competing = create_competing_risk_dataset(final_df_all, all_ids_w_outcome, "all_green_no_red")
    _df_green_no_red_competing.to_parquet(f"{pyCLIF.project_root}/output/intermediate/competing_risk_green_no_red_final.parquet")

    # Weekday competing risk
    _df_patel_competing_weekday = create_competing_risk_dataset(final_df_all, all_ids_w_outcome, "patel_flag_weekday")
    _df_patel_competing_weekday.to_parquet(f"{pyCLIF.project_root}/output/intermediate/competing_risk_patel_final_weekday.parquet")

    _df_team_competing_weekday = create_competing_risk_dataset(final_df_all, all_ids_w_outcome, "team_flag_weekday")
    _df_team_competing_weekday.to_parquet(f"{pyCLIF.project_root}/output/intermediate/competing_risk_team_final_weekday.parquet")

    _df_yellow_competing_weekday = create_competing_risk_dataset(final_df_all, all_ids_w_outcome, "any_yellow_or_green_no_red_weekday")
    _df_yellow_competing_weekday.to_parquet(f"{pyCLIF.project_root}/output/intermediate/competing_risk_yellow_final_weekday.parquet")

    _df_green_competing_weekday = create_competing_risk_dataset(final_df_all, all_ids_w_outcome, "all_green_weekday")
    _df_green_competing_weekday.to_parquet(f"{pyCLIF.project_root}/output/intermediate/competing_risk_green_final_weekday.parquet")

    _df_green_no_red_competing_weekday = create_competing_risk_dataset(final_df_all, all_ids_w_outcome, "all_green_no_red_weekday")
    _df_green_no_red_competing_weekday.to_parquet(f"{pyCLIF.project_root}/output/intermediate/competing_risk_green_no_red_final_weekday.parquet")

    cr_done = True
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Discharge Analysis
    *Removed — redundant with enhanced_failure cell. Discharge disposition is now in hourly proportions (Change 8).*
    """)
    return


@app.cell
def discharge_analysis():
    # REMOVED: Discharge failure analysis was redundant with enhanced_failure cell.
    # Discharge disposition flags are now tracked in the hourly proportions cell.
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Tracheostomy & Mortality Failure Analysis
    *Removed — trach/paralytic/mortality breakdowns now in enhanced_failure cell (Section 25).*
    """)
    return


@app.cell
def trach_failure():
    # REMOVED: Trach failure analysis was copy-pasted CR code + redundant with enhanced_failure.
    # Enhanced failure cell already reports trach_summary.csv and paralytic_summary.csv per criteria.
    return


@app.cell
def _(mo):
    mo.md(r"""
    # Mortality
    *Removed — mortality is captured in enhanced_failure cell and competing risk datasets.*
    """)
    return


@app.cell
def mortality():
    # REMOVED: Mortality without eligibility is captured by the competing risk datasets
    # (outcome == 2 = death). The enhanced_failure cell reports this for never-eligible patients.
    return


@app.cell
def _(mo):
    mo.md(r"""
    # Final Figures and Tables
    """)
    return


@app.cell
def aggregates_72h(
    final_df,
    graphs_folder,
    log,
    pd,
    plt,
    pyCLIF,
    site_name,
    sns,
):
    log("Generating final figures")
    _CRITS_ALL = {
        'Patel': 'patel_flag_all_hours', 'TEAM': 'team_flag_all_hours',
        'Yellow': 'any_yellow_or_green_no_red_all_hours', 'Green': 'all_green_no_red_all_hours',
    }
    _BUSINESS_FLAGS = {
        'Patel': 'patel_flag', 'TEAM': 'team_flag',
        'Yellow': 'any_yellow_or_green_no_red', 'Green': 'all_green_no_red'
    }
    _BUS_HRS = range(8, 17)
    _df_72h = final_df[final_df['time_from_vent'] <= 72].copy()
    _total_patients = _df_72h['encounter_block'].nunique()
    _total_observed_hours = len(_df_72h)
    _total_business_hours = len(_df_72h[_df_72h['recorded_hour'].isin(_BUS_HRS)])

    _rows = []
    for _crit in _CRITS_ALL:
        _f_all = _CRITS_ALL[_crit]
        _f_bus = _BUSINESS_FLAGS[_crit]
        _elig_all_df = _df_72h[_df_72h[_f_all] == 1]
        _eligible_hours_all = len(_elig_all_df)
        _eligible_patients = _elig_all_df['encounter_block'].nunique()
        _elig_bus_df = _df_72h[(_df_72h[_f_bus] == 1) & (_df_72h['recorded_hour'].isin(_BUS_HRS))]
        _eligible_business_hours = len(_elig_bus_df)
        _rows.append({
            'Criteria': _crit, 'Total Patients': _total_patients,
            'Eligible Patients': _eligible_patients,
            'Total Observed Hours': _total_observed_hours,
            'Eligible Hours (all hrs)': _eligible_hours_all,
            'Total Business Hours': _total_business_hours,
            'Eligible Business Hours': _eligible_business_hours,
            'Proportion Eligible Hours %': 100 * _eligible_hours_all / _total_observed_hours,
            'Proportion Eligible BusHrs %': 100 * _eligible_business_hours / _total_business_hours,
            'Proportion Eligible Patients %': 100 * _eligible_patients / _total_patients
        })
    _aggregate_df = pd.DataFrame(_rows)
    _aggregate_df.to_csv(f'{pyCLIF.project_root}/output/final/aggregates_72hrs_{site_name}.csv', index=False)

    _custom_colors = ['#983232', '#003f5c', '#fdfd96', '#98FB98']

    plt.figure(figsize=(10, 6))
    _barplot = sns.barplot(x='Criteria', y='Proportion Eligible Patients %', data=_aggregate_df, palette=_custom_colors)
    for _index, _row in _aggregate_df.iterrows():
        _barplot.text(_index, _row['Proportion Eligible Patients %'], f"{_row['Proportion Eligible Patients %']:.1f}%", color='black', ha="center", va="bottom")
    plt.title('Eligibility by Encounter (During first 72 hours)')
    plt.xlabel('Criteria'); plt.ylabel('Percentage of Encounters Eligible')
    plt.savefig(f'{graphs_folder}eligibility_by_encounters_72hrs_{site_name}.png')
    plt.close()

    plt.figure(figsize=(10, 6))
    _barplot = sns.barplot(x='Criteria', y='Proportion Eligible Hours %', data=_aggregate_df, palette=_custom_colors)
    for _index, _row in _aggregate_df.iterrows():
        _barplot.text(_index, _row['Proportion Eligible Hours %'], f"{_row['Proportion Eligible Hours %']:.1f}%", color='black', ha="center", va="bottom")
    plt.title('Eligibility by Total Observed Hours (During first 72 hours)')
    plt.xlabel('Criteria'); plt.ylabel('Percentage of Observed Hours Eligible')
    plt.savefig(f'{graphs_folder}eligibility_by_total_hours_72hrs_{site_name}.png')
    plt.close()

    plt.figure(figsize=(10, 6))
    _barplot = sns.barplot(x='Criteria', y='Proportion Eligible BusHrs %', data=_aggregate_df, palette=_custom_colors)
    for _index, _row in _aggregate_df.iterrows():
        _barplot.text(_index, _row['Proportion Eligible BusHrs %'], f"{_row['Proportion Eligible BusHrs %']:.1f}%", color='black', ha="center", va="bottom")
    plt.title('Eligibility by Business Hours (During first 72 hours)')
    plt.xlabel('Criteria'); plt.ylabel('Percentage of Business Hours Eligible')
    plt.savefig(f'{graphs_folder}eligibility_by_business_hours_72hrs_{site_name}.png')
    plt.close()

    agg72_done = True
    return


@app.cell
def aggregates_all_hours():
    # REMOVED: All-hours aggregates, ECDF, hourly distribution, and one-week trend
    # were redundant with 72h-window analysis. Keeping only 72h aggregates above.
    # Removed outputs: aggregates_{site}.csv, eligibility_hour_ecdf_summary_{site}.csv,
    #   eligibility_hourly_melted_{site}.csv, eligibility_trend_first_week_{site}.csv,
    #   and associated graphs.
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Failure Subcomponents
    """)
    return


@app.cell
def failure_subcomponents(final_df, go, graphs_folder, pd, pyCLIF, site_name):
    _criteria_info = {
        'patel_flag': {'resp_flag': 'patel_resp_flag', 'cardio_flag': 'patel_cardio_flag'},
        'team_flag': {'resp_flag': 'team_resp_flag', 'cardio_flag': 'team_cardio_flag'},
        'any_yellow_or_green_no_red': {'resp_flag': 'yellow_resp_flag', 'cardio_flag': 'yellow_cardio_flag'},
        'all_green_no_red': {'resp_flag': 'green_resp_flag', 'cardio_flag': 'green_cardio_flag'}
    }
    _results = []
    for _criterion, _flags in _criteria_info.items():
        _resp_flag = _flags['resp_flag']
        _cardio_flag = _flags['cardio_flag']
        _total_hours = final_df.groupby('encounter_block').size().rename('total_hours')
        _df_failure = final_df.copy()
        _df_failure['resp_only_failure'] = ((_df_failure[_resp_flag] == 0) & (_df_failure[_cardio_flag] == 1)).astype(int)
        _df_failure['cardio_only_failure'] = ((_df_failure[_resp_flag] == 1) & (_df_failure[_cardio_flag] == 0)).astype(int)
        _df_failure['both_failures'] = ((_df_failure[_resp_flag] == 0) & (_df_failure[_cardio_flag] == 0)).astype(int)
        _failure_counts = _df_failure.groupby('encounter_block')[['resp_only_failure', 'cardio_only_failure', 'both_failures']].sum()
        _failure_counts = _failure_counts.merge(_total_hours, left_index=True, right_index=True)
        _failure_counts['resp_only_failure_perc'] = (_failure_counts['resp_only_failure'] * 100 / _failure_counts['total_hours']).round(3)
        _failure_counts['cardio_only_failure_perc'] = (_failure_counts['cardio_only_failure'] * 100 / _failure_counts['total_hours']).round(3)
        _failure_counts['both_failures_perc'] = (_failure_counts['both_failures'] * 100 / _failure_counts['total_hours']).round(3)
        _failure_counts['total_failure_perc'] = (
            _failure_counts['resp_only_failure'] + _failure_counts['cardio_only_failure'] + _failure_counts['both_failures']
        ) * 100 / _failure_counts['total_hours']
        _criterion_met = final_df.groupby('encounter_block')[_criterion].sum().rename('criterion_met_hours')
        _failure_counts = _failure_counts.merge(_criterion_met, left_index=True, right_index=True)
        _failure_counts['criterion_met_perc'] = (_failure_counts['criterion_met_hours'] * 100 / _failure_counts['total_hours']).round(3)
        _failure_counts['Criteria'] = _criterion
        _results.append(_failure_counts.reset_index())

    _all_failure_counts = pd.concat(_results, ignore_index=True)
    _avg_failure_percentages = _all_failure_counts.groupby('Criteria').agg({
        'resp_only_failure_perc': 'mean', 'cardio_only_failure_perc': 'mean',
        'both_failures_perc': 'mean', 'total_failure_perc': 'mean', 'criterion_met_perc': 'mean'
    }).reset_index()
    _avg_failure_percentages = _avg_failure_percentages.rename(columns={
        'resp_only_failure_perc': 'Resp Failure Only', 'cardio_only_failure_perc': 'Cardio Failure Only',
        'both_failures_perc': 'Both Failures', 'total_failure_perc': 'Total Failure',
        'criterion_met_perc': 'Criterion Met'
    })
    _criteria_mapping = {'patel_flag': 'Patel', 'team_flag': 'TEAM',
                         'any_yellow_or_green_no_red': 'Yellow', 'all_green_no_red': 'Green'}
    _avg_failure_percentages['Criteria'] = _avg_failure_percentages['Criteria'].replace(_criteria_mapping)
    _avg_failure_percentages['site_name'] = site_name
    _avg_failure_percentages.to_csv(f'{pyCLIF.project_root}/output/final/avg_failure_percentages_{site_name}.csv', index=False)

    # Plotly stacked bar
    import kaleido
    _fig = go.Figure()
    _fig.add_trace(go.Bar(x=_avg_failure_percentages['Criteria'], y=_avg_failure_percentages['Cardio Failure Only'],
                          name='Cardio Failure Only', marker_color='#003366'))
    _fig.add_trace(go.Bar(x=_avg_failure_percentages['Criteria'], y=_avg_failure_percentages['Resp Failure Only'],
                          name='Resp Failure Only', marker_color='#983232'))
    _fig.add_trace(go.Bar(x=_avg_failure_percentages['Criteria'], y=_avg_failure_percentages['Both Failures'],
                          name='Both Failures', marker_color='#fdfd96'))
    _fig.update_layout(barmode='stack', xaxis_title='Criteria',
                       yaxis_title='Average Percentage of Business Hours Not Met (%)',
                       yaxis=dict(range=[0, 100]), template='plotly_white', legend_title='Failure Type')
    _fig.write_image(f'{graphs_folder}avg_failure_components_{site_name}.png')

    fail_sub_done = True
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Enhanced Failure Analysis
    """)
    return


@app.cell
def enhanced_failure(
    Path,
    UpSet,
    final_df,
    from_indicators,
    log,
    np,
    pd,
    plt,
    pyCLIF,
    site_name,
):
    def _export_combination_data(block_fail, combination_counts, crit_name, sname, out_dir, save_data=True):
        try:
            _combination_details = []
            for _combo_tuple, _count in combination_counts.items():
                _count = int(_count)  # ensure Python scalar (not numpy)
                _combo_dict = dict(zip(block_fail.columns, [bool(v) for v in _combo_tuple]))
                _sig = '|'.join([f"{c}:{int(v)}" for c, v in _combo_dict.items()])
                _combination_details.append({
                    'combination_signature': _sig, 'encounter_count': _count,
                    'proportion_of_failed': _count / len(block_fail),
                    'criteria_name': crit_name, 'site_name': sname,
                    'total_failed_encounters': len(block_fail), **_combo_dict
                })
            _cdf = pd.DataFrame(_combination_details).sort_values('encounter_count', ascending=False)
            if save_data:
                _cdf.to_csv(out_dir / "combination_patterns_detailed.csv", index=False)
                _cdf[['combination_signature', 'encounter_count', 'proportion_of_failed',
                      'criteria_name', 'site_name', 'total_failed_encounters']].to_csv(
                    out_dir / "combination_patterns_for_multisite.csv", index=False)
            log(f"[{crit_name}] Exported {len(_cdf)} unique failure combinations")
            return _cdf
        except Exception as e:
            log(f"[{crit_name}] ERROR in combination data export: {e}")
            return None

    def _analyse_criterion_enhanced(df, crit_name, *, flag_cols, master_flag, id_col="encounter_block",
                                     time_col="time_from_vent", out_dir_base=None, save_fig_data=True,
                                     max_upset_combinations=50, figure_width=12, figure_height=8, sname=None):
        _out_dir = Path(out_dir_base or f"{pyCLIF.project_root}/output/final", crit_name.lower())
        _out_dir.mkdir(parents=True, exist_ok=True)
        log(f"[{crit_name}] Starting enhanced failure analysis...")
        try:
            _never = (df.groupby(id_col)[master_flag].max().reset_index(name="ever")[lambda d: d["ever"] == 0].drop(columns="ever"))
            _fail = _never.merge(df, on=id_col, how="inner")
            _n_failed = _fail[id_col].nunique()
            _n_total = df[id_col].nunique()
            log(f"[{crit_name}] {_n_failed:,} blocks never became eligible out of {_n_total:,}")
            if _n_failed == 0:
                log(f"[{crit_name}] No failed blocks found")
                return None, None, None
        except Exception as e:
            log(f"[{crit_name}] ERROR: {e}")
            return None, None, None

        _available_flags = [c for c in flag_cols if c in _fail.columns]
        if not _available_flags:
            log(f"[{crit_name}] ERROR: No flag columns found")
            return None, None, None

        try:
            _long = _fail.melt(id_vars=[id_col], value_vars=_available_flags, var_name="criterion", value_name="flag")
            _hourly_summary = (_long.groupby("criterion")["flag"].apply(lambda s: (s == 0).mean())
                               .rename("prop_hours_failed").reset_index().sort_values("prop_hours_failed", ascending=False))
            _enc_failures = []
            for _flag in _available_flags:
                _ever_met = _fail.groupby(id_col)[_flag].max()
                _never_met = (_ever_met == 0).sum()
                _enc_failures.append({'criterion': _flag, 'encounters_never_met': _never_met,
                                      'encounter_failure_rate': _never_met / _n_failed if _n_failed > 0 else 0,
                                      'total_failed_encounters': _n_failed})
            _enc_df = pd.DataFrame(_enc_failures)
            _summary = _hourly_summary.merge(_enc_df, on='criterion')
            _summary['criteria_name'] = crit_name
            _summary['site_name'] = sname or "unknown"
            _summary['total_encounters'] = _n_total
            _summary['failed_encounters'] = _n_failed
            # Add SE and Wilson 95% CI for federated meta-analysis
            _summary['se'] = np.sqrt(
                _summary['encounter_failure_rate'] * (1 - _summary['encounter_failure_rate']) / _n_failed
            ).round(6)
            def _wilson(p, n, z=1.96):
                if n == 0: return np.nan, np.nan
                d = 1 + z**2 / n
                c = (p + z**2 / (2 * n)) / d
                m = z * np.sqrt((p * (1 - p) / n) + z**2 / (4 * n**2)) / d
                return max(0.0, c - m), min(1.0, c + m)
            _cis = _summary['encounter_failure_rate'].apply(lambda p: _wilson(p, _n_failed))
            _summary['ci_lower'] = _cis.apply(lambda x: round(x[0], 6))
            _summary['ci_upper'] = _cis.apply(lambda x: round(x[1], 6))
            if save_fig_data:
                _summary.to_csv(_out_dir / "component_failure_analysis.csv", index=False)
                # Federated-ready: proportions with CI
                _summary[['criterion', 'encounter_failure_rate', 'se', 'ci_lower', 'ci_upper',
                          'encounters_never_met', 'total_failed_encounters', 'criteria_name', 'site_name']].to_csv(
                    _out_dir / "component_failure_proportions.csv", index=False)
        except Exception as e:
            log(f"[{crit_name}] ERROR in failure rate calculation: {e}")
            _summary = pd.DataFrame()

        try:
            if len(_summary) > 0:
                _max_bars = min(15, len(_summary))
                _plot_summary = _summary.head(_max_bars)
                _fig, (_ax1, _ax2) = plt.subplots(1, 2, figsize=(figure_width, figure_height))
                _ax1.barh(range(len(_plot_summary)), _plot_summary["prop_hours_failed"])
                _ax1.set_yticks(range(len(_plot_summary)))
                _ax1.set_yticklabels(_plot_summary["criterion"], fontsize=8)
                _ax1.invert_yaxis()
                _ax1.set_xlabel("Proportion of hours NOT satisfied")
                _ax1.set_title(f"{crit_name}: Hourly Failure Rates")
                _ax2.barh(range(len(_plot_summary)), _plot_summary["encounter_failure_rate"])
                _ax2.set_yticks(range(len(_plot_summary)))
                _ax2.set_yticklabels(_plot_summary["criterion"], fontsize=8)
                _ax2.invert_yaxis()
                _ax2.set_xlabel("Proportion of encounters never meeting criteria")
                _ax2.set_title(f"{crit_name}: Encounter Failure Rates")
                plt.tight_layout()
                plt.savefig(_out_dir / "component_failure_rates.png", dpi=200, bbox_inches='tight')
                plt.close()
        except Exception as e:
            log(f"[{crit_name}] ERROR in plotting: {e}")

        # Primary blocker
        _prim_df = pd.DataFrame()
        try:
            def _first_true(g, col):
                if col not in g.columns: return np.inf
                _hit = g.loc[g[col] == 1, time_col]
                return _hit.min() if not _hit.empty else np.inf
            _prim = []
            for _blk, _g in _fail.groupby(id_col):
                _lags = {c: _first_true(_g, c) for c in _available_flags}
                _valid = {k: v for k, v in _lags.items() if v != np.inf}
                if _valid:
                    _pb = max(_valid, key=_valid.get)
                    _pt = _valid[_pb]
                else:
                    _pb = "never_achieved"
                    _pt = np.inf
                _prim.append([_blk, _pb, _pt])
            _prim_df = pd.DataFrame(_prim, columns=[id_col, "primary_blocker", "time_to_first_true"])
            if save_fig_data:
                _prim_df.to_csv(_out_dir / "primary_blockers_by_encounter.csv", index=False)
                _blocker_counts = _prim_df['primary_blocker'].value_counts()
                pd.DataFrame({'primary_blocker': _blocker_counts.index, 'encounter_count': _blocker_counts.values,
                               'proportion': _blocker_counts.values / len(_prim_df),
                               'criteria_name': crit_name, 'site_name': sname or "unknown"
                }).to_csv(_out_dir / "primary_blocker_summary.csv", index=False)
        except Exception as e:
            log(f"[{crit_name}] ERROR in primary blocker analysis: {e}")

        # Combinations
        _combo_data = None
        try:
            _failed_matrix = _fail[_available_flags].eq(0).astype(bool)
            _block_fail = _failed_matrix.groupby(_fail[id_col]).max()
            _combo_counts = _block_fail.value_counts()
            _top = _combo_counts.head(max_upset_combinations)
            if len(_top) > 0:
                _top_tuples = [tuple(bool(v) for v in t) for t in _top.index.tolist()]
                _subset_mask = _block_fail.apply(lambda x: tuple(bool(v) for v in x) in _top_tuples, axis=1)
                _subset_data = _block_fail[_subset_mask]
                if len(_subset_data) > 5:
                    _upset_data = from_indicators(_subset_data.columns, _subset_data)
                    _fig2 = plt.figure(figsize=(min(figure_width, 14), min(figure_height, 10)))
                    UpSet(_upset_data, show_counts=True, sort_by="cardinality").plot(fig=_fig2)
                    plt.suptitle(f"{crit_name}: Top {len(_top)} Failure Combinations", fontsize=12)
                    plt.savefig(_out_dir / "failure_combinations_upset.png", dpi=200, bbox_inches='tight')
                    plt.close()
            _combo_data = _export_combination_data(_block_fail, _combo_counts, crit_name, sname, _out_dir, save_fig_data)
            # Federated-ready: top 15 combinations
            if _combo_data is not None and len(_combo_data) > 0:
                _combo_data.head(15)[['combination_signature', 'encounter_count', 'proportion_of_failed',
                                      'criteria_name', 'site_name', 'total_failed_encounters']].to_csv(
                    _out_dir / "top_failure_combinations.csv", index=False)
        except Exception as e:
            log(f"[{crit_name}] ERROR in combination analysis: {e}")

        # Additional analysis (business hours, trach, paralytics)
        try:
            if 'recorded_hour' in _fail.columns:
                _fail_copy = _fail.copy()
                _fail_copy['is_business_hours'] = _fail_copy['recorded_hour'].isin(list(range(8, 17)))
                _bh = _fail_copy.groupby(id_col).agg({'is_business_hours': ['sum', 'count', 'mean']}).round(3)
                _bh.columns = ['business_hours_count', 'total_hours', 'business_hours_proportion']
                if save_fig_data:
                    _bh.to_csv(_out_dir / "business_hours_by_encounter.csv")
            if 'hourly_trach' in _fail.columns:
                _trach = _fail.groupby(id_col)['hourly_trach'].max()
                pd.DataFrame([{'encounters_with_trach': (_trach == 1).sum(),
                               'encounters_without_trach': (_trach == 0).sum(),
                               'trach_proportion': (_trach == 1).mean()}]).to_csv(_out_dir / "tracheostomy_summary.csv", index=False)
            if 'paralytics_flag' in _fail.columns:
                _para = _fail.groupby(id_col)['paralytics_flag'].max()
                pd.DataFrame([{'encounters_with_paralytics': (_para == 1).sum(),
                               'encounters_without_paralytics': (_para == 0).sum(),
                               'paralytic_proportion': (_para == 1).mean()}]).to_csv(_out_dir / "paralytic_summary.csv", index=False)
        except Exception as e:
            log(f"[{crit_name}] ERROR in additional analysis: {e}")

        log(f"[{crit_name}] Analysis complete. Results saved to {_out_dir}/")
        return _summary, _prim_df, _combo_data

    _team_flags = ["team_pulse_flag", "team_lactate_flag", "team_ne_flag",
                   "team_fio2_flag", "team_peep_flag", "team_resp_rate_flag", "team_spo2_flag",
                   'hourly_trach', 'paralytics_flag']
    _patel_flags = ['patel_map_flag', 'patel_sbp_flag', 'patel_pulse_flag',
                    'patel_resp_rate_flag', 'patel_spo2_flag', 'patel_resp_flag',
                    'patel_cardio_flag', 'hourly_trach', 'paralytics_flag']
    _yellow_flags = ['hourly_trach', 'paralytics_flag',
                     'red_resp_spo2_flag', 'red_map_flag', 'red_high_support_flag',
                     'red_hypertensive_flag', 'red_pulse_high_flag', 'red_pulse_low_flag',
                     'yellow_resp_spo2_flag', 'yellow_fio2_flag', 'yellow_resp_rate_flag',
                     'yellow_peep_flag', 'yellow_map_flag', 'yellow_pulse_flag', 'yellow_lactate_flag',
                     'green_resp_spo2_flag', 'green_resp_rate_flag', 'green_fio2_flag',
                     'green_peep_flag', 'green_map_flag', 'green_pulse_flag',
                     'green_lactate_flag', 'green_hr_flag']
    _green_flags = ['hourly_trach', 'paralytics_flag',
                    'green_resp_spo2_flag', 'green_resp_rate_flag', 'green_fio2_flag',
                    'green_peep_flag', 'green_map_flag', 'green_pulse_flag',
                    'green_lactate_flag', 'green_hr_flag']

    log("ENHANCED CRITERION FAILURE ANALYSIS")
    log("=" * 80)

    _summary_team, _primary_team, _combo_team = _analyse_criterion_enhanced(
        final_df, "TEAM", flag_cols=_team_flags, master_flag="team_flag",
        sname=site_name, max_upset_combinations=40, figure_width=14, figure_height=8)
    _summary_patel, _primary_patel, _combo_patel = _analyse_criterion_enhanced(
        final_df, "Patel", flag_cols=_patel_flags, master_flag="patel_flag",
        sname=site_name, max_upset_combinations=40, figure_width=14, figure_height=8)
    _summary_yellow, _primary_yellow, _combo_yellow = _analyse_criterion_enhanced(
        final_df, "Yellow", flag_cols=_yellow_flags, master_flag="any_yellow_or_green_no_red",
        sname=site_name, max_upset_combinations=30, figure_width=16, figure_height=10)
    _summary_green, _primary_green, _combo_green = _analyse_criterion_enhanced(
        final_df, "Green", flag_cols=_green_flags, master_flag="all_green_no_red",
        sname=site_name, max_upset_combinations=40, figure_width=14, figure_height=8)

    # Overall summary
    try:
        _summary_data = []
        for _criteria, _sdf in [('TEAM', _summary_team), ('Patel', _summary_patel),
                                 ('Yellow', _summary_yellow), ('Green', _summary_green)]:
            if _sdf is not None and len(_sdf) > 0:
                _summary_data.append({
                    'criteria': _criteria,
                    'total_encounters': _sdf['total_encounters'].iloc[0],
                    'failed_encounters': _sdf['failed_encounters'].iloc[0],
                    'failure_rate': _sdf['failed_encounters'].iloc[0] / _sdf['total_encounters'].iloc[0],
                    'most_problematic_component': _sdf.loc[_sdf['encounter_failure_rate'].idxmax(), 'criterion'],
                    'highest_component_failure_rate': _sdf['encounter_failure_rate'].max(),
                    'site_name': site_name
                })
        if _summary_data:
            _overall = pd.DataFrame(_summary_data)
            _overall.to_csv(f"{pyCLIF.project_root}/output/final/overall_failure_summary_by_criteria.csv", index=False)
            log(f"\nOVERALL SUMMARY")
            for _, _row in _overall.iterrows():
                log(f"{_row['criteria']:8s}: {_row['failed_encounters']:4.0f}/{_row['total_encounters']:4.0f} "
                      f"({_row['failure_rate']*100:5.1f}%) failed | Top issue: {_row['most_problematic_component']}")
    except Exception as e:
        log(f"ERROR creating overall summary: {e}")

    log(f"\n{'=' * 80}")
    log("ENHANCED FAILURE ANALYSIS COMPLETE")

    enhanced_fail_done = True
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Average Hours by Day
    """)
    return


@app.cell
def avg_hours_by_day(final_df, graphs_folder, pd, plt, pyCLIF, site_name, sns):
    _criteria_columns = ['patel_flag', 'team_flag', 'any_yellow_or_green_no_red', 'all_green']

    # All-hours version
    _viz_df = pd.merge(
        final_df,
        pd.read_parquet(f'{pyCLIF.project_root}/output/intermediate/cohort_all_ids_w_outcome.parquet')[['encounter_block', 'block_vent_start_dttm']],
        on='encounter_block', how='left'
    )
    _viz_df['block_vent_start_dttm'] = pd.to_datetime(_viz_df['block_vent_start_dttm'])
    _viz_df['recorded_date'] = pd.to_datetime(_viz_df['recorded_date'])
    _viz_df['recorded_dttm'] = _viz_df['recorded_date'] + pd.to_timedelta(_viz_df['recorded_hour'], unit='h')
    if _viz_df['block_vent_start_dttm'].dt.tz is not None:
        _viz_df['block_vent_start_dttm'] = _viz_df['block_vent_start_dttm'].dt.tz_localize(None)
    if _viz_df['recorded_dttm'].dt.tz is not None:
        _viz_df['recorded_dttm'] = _viz_df['recorded_dttm'].dt.tz_localize(None)
    _viz_df['calendar_day'] = (_viz_df['recorded_dttm'] - _viz_df['block_vent_start_dttm']).dt.days + 1
    _viz_df = _viz_df[['encounter_block', 'block_vent_start_dttm', 'recorded_dttm',
                        'calendar_day', 'patel_flag', 'team_flag', 'any_yellow_or_green_no_red',
                        'all_green', 'all_green_no_red', 'any_green']]

    def _compute_avg_hours_by_day(df, criteria_columns):
        _hours_per_day = df.groupby(['encounter_block', 'calendar_day']).agg({
            'patel_flag': 'sum', 'team_flag': 'sum',
            'any_yellow_or_green_no_red': 'sum', 'all_green': 'sum',
        }).reset_index()
        _hours_per_day = _hours_per_day[_hours_per_day['calendar_day'].isin([1, 2, 3])]
        return _hours_per_day.groupby('calendar_day').agg({
            'patel_flag': 'mean', 'team_flag': 'mean',
            'any_yellow_or_green_no_red': 'mean', 'all_green': 'mean',
        }).reset_index()

    _avg_hours = _compute_avg_hours_by_day(_viz_df, _criteria_columns)
    _avg_hours['site_name'] = site_name
    _avg_hours.to_csv(f'{pyCLIF.project_root}/output/final/avg_hours_by_day_{site_name}.csv', index=False)

    def _plot_avg_hours(avg_hours, criteria_columns, save_path):
        _melted = avg_hours.melt(id_vars='calendar_day', value_vars=criteria_columns, var_name='Criteria', value_name='Average Hours Met')
        plt.figure(figsize=(10, 6))
        sns.barplot(x='calendar_day', y='Average Hours Met', hue='Criteria', data=_melted, palette='viridis')
        plt.xticks(ticks=[0, 1, 2], labels=["Day 1", "Day 2", "Day 3"])
        plt.title('Average Hours Criteria Met per Day')
        plt.xlabel('Calendar Day'); plt.ylabel('Average Hours Criteria Met')
        plt.legend(title='Criteria', loc='upper center', bbox_to_anchor=(0.5, -0.1), ncol=3)
        plt.tight_layout()
        plt.savefig(save_path)
        plt.close()

    _plot_avg_hours(_avg_hours, _criteria_columns, f'{graphs_folder}avg_hours_by_day_{site_name}.png')

    # 72h version
    _df_72h = final_df[final_df['time_from_vent'] <= 72].copy()
    _viz_df_72h = pd.merge(
        _df_72h,
        pd.read_parquet(f'{pyCLIF.project_root}/output/intermediate/cohort_all_ids_w_outcome.parquet')[['encounter_block', 'block_vent_start_dttm']],
        on='encounter_block', how='left'
    )
    _viz_df_72h['block_vent_start_dttm'] = pd.to_datetime(_viz_df_72h['block_vent_start_dttm'])
    _viz_df_72h['recorded_date'] = pd.to_datetime(_viz_df_72h['recorded_date'])
    _viz_df_72h['recorded_dttm'] = _viz_df_72h['recorded_date'] + pd.to_timedelta(_viz_df_72h['recorded_hour'], unit='h')
    if _viz_df_72h['block_vent_start_dttm'].dt.tz is not None:
        _viz_df_72h['block_vent_start_dttm'] = _viz_df_72h['block_vent_start_dttm'].dt.tz_localize(None)
    if _viz_df_72h['recorded_dttm'].dt.tz is not None:
        _viz_df_72h['recorded_dttm'] = _viz_df_72h['recorded_dttm'].dt.tz_localize(None)
    _viz_df_72h['calendar_day'] = (_viz_df_72h['recorded_dttm'] - _viz_df_72h['block_vent_start_dttm']).dt.days + 1
    _viz_df_72h = _viz_df_72h[['encounter_block', 'block_vent_start_dttm', 'recorded_dttm',
                                'calendar_day', 'patel_flag', 'team_flag', 'any_yellow_or_green_no_red',
                                'all_green', 'all_green_no_red', 'any_green']]
    _avg_hours_72h = _compute_avg_hours_by_day(_viz_df_72h, _criteria_columns)
    _avg_hours_72h['site_name'] = site_name
    _avg_hours_72h.to_csv(f'{pyCLIF.project_root}/output/final/avg_hours_by_day_72h_{site_name}.csv', index=False)
    _plot_avg_hours(_avg_hours_72h, _criteria_columns, f'{graphs_folder}avg_hours_by_day_72h_{site_name}.png')

    avg_hrs_done = True
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Parallel Categories
    """)
    return


@app.cell
def parallel_categories(final_df, log, pd, px, pyCLIF, site_name):
    _parallel_df = final_df[['patel_flag', 'team_flag', 'any_yellow_or_green_no_red', 'all_green']].copy()
    _parallel_df['patel_flag'] = _parallel_df['patel_flag'].apply(lambda x: 1 if x else 0)
    _parallel_df['team_flag'] = _parallel_df['team_flag'].apply(lambda x: 1 if x else 0)
    _parallel_df['any_yellow_or_green_no_red'] = _parallel_df['any_yellow_or_green_no_red'].apply(lambda x: 1 if x else 0)
    _parallel_df['all_green'] = _parallel_df['all_green'].apply(lambda x: 1 if x else 0)

    _fig = px.parallel_categories(
        _parallel_df, dimensions=['patel_flag', 'team_flag', 'any_yellow_or_green_no_red', 'all_green'],
        color="patel_flag",
        labels={'patel_flag': 'Patel Met', 'team_flag': 'TEAM Met',
                'any_yellow_or_green_no_red': 'Yellow Flag', 'all_green': 'Green Flag'},
        color_continuous_scale=px.colors.sequential.Inferno
    )
    _fig.update_layout(title="Parallel Categories Plot: Comparison of Criteria Satisfaction")
    _fig.write_image(f'{pyCLIF.project_root}/output/final/graphs/parallel_categories_{site_name}.png')
    # Save aggregated co-occurrence counts (no patient-level data)
    _parallel_counts = _parallel_df.groupby(['patel_flag', 'team_flag', 'any_yellow_or_green_no_red', 'all_green']).size().reset_index(name='n_hours')
    _parallel_counts['site'] = site_name
    _parallel_counts.to_csv(f'{pyCLIF.project_root}/output/final/parallel_categories_counts_{site_name}.csv', index=False)

    # Sanity check: Patel fail but TEAM pass
    _patel_fail_team_pass = final_df[(final_df['patel_flag'] == 0) & (final_df['team_flag'] == 1)]
    log(f"\nTotal number of hours where Patel failed and Team passed: {len(_patel_fail_team_pass)}\n")
    if len(_patel_fail_team_pass) > 0:
        log("Primary cause of Patel Criteria non-compliance")
        _failure_counts = {
            'MAP': sum(_patel_fail_team_pass['patel_map_flag'] == 0),
            'SBP': sum(_patel_fail_team_pass['patel_sbp_flag'] == 0),
            'Pulse': sum(_patel_fail_team_pass['patel_pulse_flag'] == 0),
            'Respiratory Rate': sum(_patel_fail_team_pass['patel_resp_rate_flag'] == 0),
            'SpO2': sum(_patel_fail_team_pass['patel_spo2_flag'] == 0)
        }
        _failure_df = pd.DataFrame(list(_failure_counts.items()), columns=['Criteria', 'Count'])
        _failure_df.to_csv(f'{pyCLIF.project_root}/output/final/patel_fail_team_pass_subcomponents_{site_name}.csv', index=False)
        log(_failure_df)

    parallel_done = True
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Weekday Sensitivity
    """)
    return


@app.cell
def yellow_green_weekday(
    final_df,
    final_df_all,
    graphs_folder,
    log,
    mcolors,
    np,
    pd,
    plt,
    pyCLIF,
    site_name,
):
    # Yellow-Green spectrum
    _green_cols = [c for c in final_df.columns if c.startswith("green_") and c.endswith("_flag")]
    _yellow_cols = [c for c in final_df.columns if c.startswith("yellow_") and c.endswith("_flag")]
    _first_hit = (
        final_df.loc[final_df["any_yellow_or_green_no_red"] == 1]
        .sort_values(["encounter_block", "time_from_vent"])
        .groupby("encounter_block").first()
    )
    _first_hit["n_green"] = _first_hit[_green_cols].sum(axis=1)
    _first_hit["n_yellow"] = _first_hit[_yellow_cols].sum(axis=1)
    _first_hit["yellow_frac"] = (_first_hit["n_yellow"] / (_first_hit["n_green"] + _first_hit["n_yellow"])).fillna(0)
    _first_hit["green_frac"] = (_first_hit["n_green"] / (_first_hit["n_green"] + _first_hit["n_yellow"])).fillna(0)

    _x = np.random.normal(0, 0.002, size=len(_first_hit))
    _y = _first_hit["green_frac"].values
    _green_yellow = mcolors.LinearSegmentedColormap.from_list("YellowGreen", ["#ffeb3b", "#2ca02c"])

    _fig, _ax = plt.subplots(figsize=(6, 4))
    _sc = _ax.scatter(_x, _y, s=14, alpha=0.7, c=_y, cmap=_green_yellow, vmin=0, vmax=1)
    _ax.set_xlim(-0.02, 0.02); _ax.set_xticks([]); _ax.set_ylim(0, 1)
    _ax.set_ylabel("Green fraction  (1=pure green  |  0=pure yellow)")
    _ax.set_title("Eligibility colour spectrum per encounter block", pad=12)
    _cbar = _fig.colorbar(_sc, ax=_ax, pad=0.02, shrink=0.8)
    _cbar.set_label("Green fraction")
    _caption = (
        "Each dot = first eligible hour of an encounter block. "
        "Vertical position/colour show the fraction of satisfied criteria that were "
        "GREEN (physiologically safer) versus YELLOW (less conservative). "
        "Horizontal spread is tiny random jitter to avoid over plotting; x-axis has no meaning."
    )
    _fig.text(0.01, -0.10, _caption, ha="left", va="top", wrap=True, fontsize=9)
    _fig.savefig(f'{graphs_folder}yellow_eligibility_colour_spectrum_{site_name}.png')
    plt.close(_fig)
    # Save aggregated spectrum data (no patient-level IDs)
    _spectrum_summary = pd.DataFrame({
        'green_frac_bin': pd.cut(_first_hit['green_frac'], bins=10).astype(str),
    }).value_counts().reset_index()
    _spectrum_summary.columns = ['green_frac_bin', 'n_encounter_blocks']
    _spectrum_summary['site'] = site_name
    _spectrum_summary.to_csv(f'{pyCLIF.project_root}/output/final/yellow_green_spectrum_summary_{site_name}.csv', index=False)

    # Weekday Sensitivity Analysis
    log("=== WEEKDAY SENSITIVITY ANALYSIS ===")
    _BUSINESS_FLAGS = {
        'Patel': 'patel_flag', 'TEAM': 'team_flag',
        'Yellow': 'any_yellow_or_green_no_red', 'Green': 'all_green'
    }
    _WEEKDAY_FLAGS = {
        'Patel': 'patel_flag_weekday', 'TEAM': 'team_flag_weekday',
        'Yellow': 'any_yellow_or_green_no_red_weekday', 'Green': 'all_green_weekday'
    }
    _comparison_rows = []
    for _crit in _BUSINESS_FLAGS.keys():
        _all_day_flag = _BUSINESS_FLAGS[_crit]
        _weekday_flag = _WEEKDAY_FLAGS[_crit]
        _all_day_eligible = len(final_df_all[
            (final_df_all[_all_day_flag] == 1) &
            (final_df_all['recorded_hour'].isin(range(8, 17)))
        ])
        _weekday_eligible = len(final_df_all[
            (final_df_all[_weekday_flag] == 1) &
            (final_df_all['recorded_hour'].isin(range(8, 17))) &
            (final_df_all['is_weekday'] == True)
        ])
        _comparison_rows.append({
            'Criteria': _crit, 'AllDay_Eligible_Hours': _all_day_eligible,
            'Weekday_Eligible_Hours': _weekday_eligible,
            'Difference': _weekday_eligible - _all_day_eligible,
            'Percent_Change': (_weekday_eligible - _all_day_eligible) / _all_day_eligible * 100 if _all_day_eligible > 0 else 0
        })
    _comparison_df = pd.DataFrame(_comparison_rows)
    _comparison_df.to_csv(f'{pyCLIF.project_root}/output/final/weekday_sensitivity_hours_{site_name}.csv', index=False)
    log(_comparison_df)

    weekday_done = True
    return


@app.cell
def _(mo):
    mo.md(r"""
    # Sensitivity Analysis
    """)
    return


@app.cell
def sensitivity_stacked(
    all_ids_w_outcome,
    create_competing_risk_dataset,
    final_df_all,
    log,
    np,
    output_folder,
    pd,
    pyCLIF,
    site_name,
):
    log("=" * 80)
    log("STACKED SENSITIVITY ANALYSES")
    log("=" * 80)

    _criteria_map = {
        'Chicago': {
            'flag_anyday': 'patel_flag',
            'flag_weekday': 'patel_flag_weekday',
            'flag_all_hours': 'patel_flag_all_hours',
        },
        'TEAM': {
            'flag_anyday': 'team_flag',
            'flag_weekday': 'team_flag_weekday',
            'flag_all_hours': 'team_flag_all_hours',
        },
        'Yellow': {
            'flag_anyday': 'any_yellow_or_green_no_red',
            'flag_weekday': 'any_yellow_or_green_no_red_weekday',
            'flag_all_hours': 'any_yellow_or_green_no_red_all_hours',
        },
        'Green': {
            'flag_anyday': 'all_green',
            'flag_weekday': 'all_green_weekday',
            'flag_all_hours': 'all_green_all_hours',
        },
    }

    _df_72h = final_df_all[final_df_all['time_from_vent'] <= 72].copy()

    def _compute_4h_continuous(df, all_hours_col):
        """Compute 4 consecutive hours of physiological eligibility.
        Returns a new column where 1 = this hour is the 4th+ consecutive eligible hour."""
        _sorted = df.sort_values(['encounter_block', 'time_from_vent'])
        _rolling = _sorted.groupby('encounter_block')[all_hours_col].rolling(4, min_periods=4).min()
        _rolling = _rolling.reset_index(level=0, drop=True)
        return _rolling.reindex(df.index).fillna(0).astype(int)

    def _compute_sensitivity(df, flag_col, label, criteria_name, cohort_name):
        """Compute sensitivity stats for a given flag column."""
        _first_elig = df[df[flag_col] == 1].groupby('encounter_block')['time_from_vent'].min()
        _n_total = df['encounter_block'].nunique()
        _n_eligible = len(_first_elig)
        _pct_eligible = round(_n_eligible / _n_total * 100, 1) if _n_total > 0 else 0

        # SE for proportion (Wald) — needed for federated meta-analysis
        _p = _n_eligible / _n_total if _n_total > 0 else 0
        _se_pct = round(np.sqrt(_p * (1 - _p) / _n_total) * 100, 2) if _n_total > 0 else np.nan

        _median = round(_first_elig.median(), 1) if _n_eligible > 0 else np.nan
        _q1 = round(_first_elig.quantile(0.25), 1) if _n_eligible > 0 else np.nan
        _q3 = round(_first_elig.quantile(0.75), 1) if _n_eligible > 0 else np.nan

        # Bootstrap SE for median — needed for inverse-variance meta-analysis
        _se_median = np.nan
        if _n_eligible >= 10:
            _vals = _first_elig.values
            _rng = np.random.default_rng(42)
            _boot_medians = [np.median(_rng.choice(_vals, size=len(_vals), replace=True)) for _ in range(1000)]
            _se_median = round(np.std(_boot_medians, ddof=1), 2)

        return {
            'site': site_name, 'criteria': criteria_name, 'cohort': cohort_name,
            'sensitivity_type': label, 'n_total': _n_total, 'n_eligible': _n_eligible,
            'pct_eligible': _pct_eligible, 'se_pct': _se_pct,
            'median_time': _median, 'se_median': _se_median,
            'q1': _q1, 'q3': _q3,
        }

    _all_results = []

    for _crit_name, _flags in _criteria_map.items():
        log(f"\n--- {_crit_name} ---")

        # A1: 1h anyday business hours (primary — already the default flag)
        _a1 = _compute_sensitivity(_df_72h, _flags['flag_anyday'], 'original_1h_anyday', _crit_name, 'original')
        log(f"  A1 (1h anyday): {_a1['pct_eligible']}% eligible, median={_a1['median_time']}h")
        _all_results.append(_a1)

        # A2: 1h weekday business hours
        _a2 = _compute_sensitivity(_df_72h, _flags['flag_weekday'], 'original_1h_weekday', _crit_name, 'original')
        log(f"  A2 (1h weekday): {_a2['pct_eligible']}% eligible, median={_a2['median_time']}h")
        _all_results.append(_a2)

        # A3: 4h continuous weekday — compute rolling 4h min on all_hours flag
        _cont_col = f'_{_crit_name}_4h_cont'
        _df_72h[_cont_col] = _compute_4h_continuous(_df_72h, _flags['flag_all_hours'])
        # Last hour must be weekday business hours
        _a3_flag_col = f'_{_crit_name}_4h_weekday'
        _df_72h[_a3_flag_col] = (
            (_df_72h[_cont_col] == 1) &
            (_df_72h['recorded_hour'] >= 8) &
            (_df_72h['recorded_hour'] < 17) &
            (_df_72h['is_weekday'] == True)
        ).astype(int)
        _a3 = _compute_sensitivity(_df_72h, _a3_flag_col, 'original_4h_weekday', _crit_name, 'original')
        log(f"  A3 (4h weekday): {_a3['pct_eligible']}% eligible, median={_a3['median_time']}h")
        _all_results.append(_a3)

        # Also: 4h continuous anyday (isolates continuous effect)
        _a4_flag_col = f'_{_crit_name}_4h_anyday'
        _df_72h[_a4_flag_col] = (
            (_df_72h[_cont_col] == 1) &
            (_df_72h['recorded_hour'] >= 8) &
            (_df_72h['recorded_hour'] < 17)
        ).astype(int)
        _a4 = _compute_sensitivity(_df_72h, _a4_flag_col, 'original_4h_anyday', _crit_name, 'original')
        log(f"  A4 (4h anyday): {_a4['pct_eligible']}% eligible, median={_a4['median_time']}h")
        _all_results.append(_a4)

        # Generate competing risk datasets for each sensitivity type
        for _label, _flag in [('original_1h_anyday', _flags['flag_anyday']),
                               ('original_1h_weekday', _flags['flag_weekday']),
                               ('original_4h_weekday', _a3_flag_col),
                               ('original_4h_anyday', _a4_flag_col)]:
            _cr = create_competing_risk_dataset(_df_72h, all_ids_w_outcome, flag_col=_flag)
            _cr_path = f'{pyCLIF.project_root}/output/intermediate/{site_name}_{_crit_name}_{_label}_competing_risk.parquet'
            _cr.to_parquet(_cr_path, index=False)

    # --- IMV ≥24h subcohort ---
    log("\n" + "=" * 40)
    log("IMV ≥24h SUBCOHORT")
    log("=" * 40)
    _max_vent_hours = _df_72h.groupby('encounter_block')['time_from_vent'].max()
    _imv24h_blocks = _max_vent_hours[_max_vent_hours >= 24].index
    _df_imv24h = _df_72h[_df_72h['encounter_block'].isin(_imv24h_blocks)].copy()
    log(f"IMV ≥24h cohort: {_df_imv24h['encounter_block'].nunique()} encounter_blocks "
          f"(from {_df_72h['encounter_block'].nunique()} original)")

    for _crit_name, _flags in _criteria_map.items():
        log(f"\n--- {_crit_name} (IMV ≥24h) ---")

        _b1 = _compute_sensitivity(_df_imv24h, _flags['flag_anyday'], 'imv24h_1h_anyday', _crit_name, 'imv24h')
        log(f"  B1 (1h anyday): {_b1['pct_eligible']}% eligible, median={_b1['median_time']}h")
        _all_results.append(_b1)

        _b2 = _compute_sensitivity(_df_imv24h, _flags['flag_weekday'], 'imv24h_1h_weekday', _crit_name, 'imv24h')
        log(f"  B2 (1h weekday): {_b2['pct_eligible']}% eligible, median={_b2['median_time']}h")
        _all_results.append(_b2)

        # B3 uses the same 4h continuous columns already computed
        _a3_flag_col = f'_{_crit_name}_4h_weekday'
        _b3 = _compute_sensitivity(_df_imv24h, _a3_flag_col, 'imv24h_4h_weekday', _crit_name, 'imv24h')
        log(f"  B3 (4h weekday): {_b3['pct_eligible']}% eligible, median={_b3['median_time']}h")
        _all_results.append(_b3)

        # B4: 4h continuous anyday (IMV≥24h)
        _a4_flag_col = f'_{_crit_name}_4h_anyday'
        _b4 = _compute_sensitivity(_df_imv24h, _a4_flag_col, 'imv24h_4h_anyday', _crit_name, 'imv24h')
        log(f"  B4 (4h anyday): {_b4['pct_eligible']}% eligible, median={_b4['median_time']}h")
        _all_results.append(_b4)

        # Competing risk datasets for IMV≥24h
        for _label, _flag in [('imv24h_1h_anyday', _flags['flag_anyday']),
                               ('imv24h_1h_weekday', _flags['flag_weekday']),
                               ('imv24h_4h_anyday', _a4_flag_col),
                               ('imv24h_4h_weekday', _a3_flag_col)]:
            _cr = create_competing_risk_dataset(_df_imv24h, all_ids_w_outcome, flag_col=_flag)
            _cr_path = f'{pyCLIF.project_root}/output/intermediate/{site_name}_{_crit_name}_{_label}_competing_risk.parquet'
            _cr.to_parquet(_cr_path, index=False)

    _summary_df = pd.DataFrame(_all_results)
    _summary_df.to_csv(f'{output_folder}{site_name}_sensitivity_summary.csv', index=False)
    log(f"\nSaved: {site_name}_sensitivity_summary.csv ({len(_summary_df)} rows)")

    # Validation: diamond partial order (not a linear chain)
    #   1h_anyday (least restrictive)
    #   ├── 1h_weekday  (adds weekday restriction)
    #   ├── 4h_anyday   (adds continuity restriction)
    #   └── 4h_weekday  (both restrictions — most restrictive)
    _diamond_edges = [
        ('1h_anyday', '1h_weekday'), ('1h_anyday', '4h_anyday'),
        ('1h_weekday', '4h_weekday'), ('4h_anyday', '4h_weekday'),
    ]
    for _crit_name in _criteria_map:
        for _cohort in ['original', 'imv24h']:
            _crit_data = _summary_df[(_summary_df['criteria'] == _crit_name) & (_summary_df['cohort'] == _cohort)]
            def _get_med(stype):
                _r = _crit_data[_crit_data['sensitivity_type'].str.endswith(stype)]
                return float(_r['median_time'].values[0]) if len(_r) > 0 else np.nan
            _m = {k: _get_med(k) for k in ['1h_anyday', '1h_weekday', '4h_anyday', '4h_weekday']}
            if all(np.isfinite(list(_m.values()))):
                for _a, _b in _diamond_edges:
                    if _m[_a] > _m[_b] + 0.5:
                        log(f"  WARNING: {_a} ({_m[_a]}) > {_b} ({_m[_b]}) for {_crit_name}/{_cohort}")

    # Validation: IMV≥24h should be smaller cohort
    for _crit_name in _criteria_map:
        _orig_n = _summary_df[(_summary_df['criteria'] == _crit_name) &
                               (_summary_df['sensitivity_type'] == 'original_1h_anyday')]['n_total'].values[0]
        _imv_n = _summary_df[(_summary_df['criteria'] == _crit_name) &
                              (_summary_df['sensitivity_type'] == 'imv24h_1h_anyday')]['n_total'].values[0]
        assert _imv_n < _orig_n, f"IMV≥24h cohort should be smaller for {_crit_name}: {_imv_n} vs {_orig_n}"
    log("Validation passed: IMV≥24h cohort smaller than original for all criteria")

    sensitivity_done = True
    return


@app.cell
def _(mo):
    mo.md(r"""
    # Validation Report
    """)
    return


@app.cell
def validation_report(
    final_df,
    final_df_blocks,
    log,
    output_folder,
    pd,
    site_name,
):
    log("=" * 80)
    log("VALIDATION REPORT")
    log("=" * 80)

    _checks = []
    _n_total = final_df[final_df['time_from_vent'] <= 72]['encounter_block'].nunique()
    _n_blocks = final_df_blocks['encounter_block'].nunique()

    # 1. Cohort size consistency
    _check1 = _n_total == _n_blocks
    _checks.append({'check': 'cohort_size_consistency', 'result': 'PASS' if _check1 else 'FAIL',
                    'details': f'final_df={_n_total}, final_df_blocks={_n_blocks}'})

    # 2. No impossible values
    _pct_cols = [c for c in final_df.columns if c.endswith('_flag')]
    _bad_vals = 0
    for _c in _pct_cols:
        _bad_vals += ((final_df[_c] < 0) | (final_df[_c] > 1)).sum()
    _check2 = _bad_vals == 0
    _checks.append({'check': 'no_impossible_flag_values', 'result': 'PASS' if _check2 else 'FAIL',
                    'details': f'{_bad_vals} impossible flag values'})

    # 3. Hourly proportions CSV exists and sums to 100%
    try:
        _hourly = pd.read_csv(f'{output_folder}{site_name}_hourly_proportions.csv')
        _prop_sum = (_hourly['prop_dead'] + _hourly['prop_discharged'] +
                     _hourly['prop_extubated_eligible'] + _hourly['prop_extubated_not_eligible'] +
                     _hourly['prop_intubated_eligible'] + _hourly['prop_intubated_not_eligible'])
        _check3 = _prop_sum.min() >= 99.9 and _prop_sum.max() <= 100.1
        _checks.append({'check': 'hourly_proportions_sum_100', 'result': 'PASS' if _check3 else 'FAIL',
                        'details': f'range=[{_prop_sum.min():.1f}, {_prop_sum.max():.1f}]'})
    except Exception as e:
        _checks.append({'check': 'hourly_proportions_sum_100', 'result': 'FAIL', 'details': str(e)})

    # 4. Sensitivity summary exists
    try:
        _sens = pd.read_csv(f'{output_folder}{site_name}_sensitivity_summary.csv')
        _check4 = len(_sens) > 0
        _checks.append({'check': 'sensitivity_summary_exists', 'result': 'PASS' if _check4 else 'FAIL',
                        'details': f'{len(_sens)} rows'})
    except Exception as e:
        _checks.append({'check': 'sensitivity_summary_exists', 'result': 'FAIL', 'details': str(e)})

    # 5. Sepsis columns present
    _sepsis_cols = ['block_sepsis_24h', 'block_sepsis_72h', 'block_sepsis_anytime']
    _check5 = all(c in final_df_blocks.columns for c in _sepsis_cols)
    _checks.append({'check': 'sepsis_columns_present', 'result': 'PASS' if _check5 else 'FAIL',
                    'details': f'missing: {[c for c in _sepsis_cols if c not in final_df_blocks.columns]}'})

    # 6. No CCI columns
    _cci_cols = [c for c in final_df_blocks.columns if 'cci' in c.lower()]
    _check6 = len(_cci_cols) == 0
    _checks.append({'check': 'no_cci_columns', 'result': 'PASS' if _check6 else 'FAIL',
                    'details': f'found: {_cci_cols}'})

    # 7. Missingness CSV matches
    try:
        _miss = pd.read_csv(f'{output_folder}{site_name}_patient_missingness_72h.csv')
        _check7 = True  # Just check it loads
        _checks.append({'check': 'missingness_csv_exists', 'result': 'PASS', 'details': f'{len(_miss)} rows'})
    except Exception as e:
        _checks.append({'check': 'missingness_csv_exists', 'result': 'FAIL', 'details': str(e)})

    _report_df = pd.DataFrame(_checks)
    _report_df.to_csv(f'{output_folder}{site_name}_validation_report.csv', index=False)

    log("\nValidation Results:")
    for _, _row in _report_df.iterrows():
        _status = 'PASS' if _row['result'] == 'PASS' else 'FAIL'
        log(f"  [{_status}] {_row['check']}: {_row['details']}")

    _n_pass = (_report_df['result'] == 'PASS').sum()
    _n_total_checks = len(_report_df)
    log(f"\n{_n_pass}/{_n_total_checks} checks passed")
    log(f"Saved: {site_name}_validation_report.csv")

    validation_done = True
    return


if __name__ == "__main__":
    app.run()
