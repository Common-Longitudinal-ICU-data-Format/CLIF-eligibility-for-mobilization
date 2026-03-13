import pandas as pd
import numpy as np
import json
import os
import duckdb
import seaborn as sns
import matplotlib.pyplot as plt
import pytz
from datetime import datetime
from typing import Union
from functools import reduce

# module-level duckdb is imported above; connections are created per-operation

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def load_config():
    json_path = os.path.join(project_root, 'config', 'config.json')
    
    with open(json_path, 'r', encoding='utf-8') as file:
        config = json.load(file)
    print("Loaded configuration from config.json")
    return config

helper = load_config()

def _cast_id_cols_to_string(df):
    id_cols = [c for c in df.columns if c.endswith("_id")]
    if id_cols:                                   # no-op if none found
        df[id_cols] = df[id_cols].astype("string")
    return df

def load_parquet_with_tz(file_path, columns=None, filters=None, sample_size=None):
    con = duckdb.connect()
    # DuckDB >=0.9 understands the original zone if we ask for TIMESTAMPTZ
    con.execute("SET timezone = 'UTC';")          # read & return in UTC
    con.execute("SET pandas_analyze_sample=0;")   # avoid sampling issues

    sel = "*" if columns is None else ", ".join(columns)
    query = f"SELECT {sel} FROM parquet_scan('{file_path}')"

    if filters:                                  # optional WHERE clause
        clauses = []
        for col, val in filters.items():
            if isinstance(val, list):
                vals = ", ".join([f"'{v}'" for v in val])
                clauses.append(f"{col} IN ({vals})")
            else:
                clauses.append(f"{col} = '{val}'")
        query += " WHERE " + " AND ".join(clauses)
    if sample_size:
        query += f" LIMIT {sample_size}"

    df = con.execute(query).fetchdf()            # pandas DataFrame
    con.close()
    df = _cast_id_cols_to_string(df)         # cast id columns to string
    return df

def load_data(table, sample_size=None, columns=None, filters=None):
    """
    Load data from a file in the specified directory with the option to select specific columns and apply filters.

    Parameters:
        table (str): The name of the table to load.
        sample_size (int, optional): Number of rows to load.
        columns (list of str, optional): List of column names to load.
        filters (dict, optional): Dictionary of filters to apply.

    Returns:
        pd.DataFrame: DataFrame containing the requested data.
    """
    # Determine the file path based on the directory and filetype
    file_name = f"{table}.{helper['file_type']}"
    file_path = os.path.join(helper['tables_path'], file_name)
    
    # Load the data based on filetype
    if os.path.exists(file_path):
        if helper['file_type'] == 'csv':
            # For CSV, we can use DuckDB to read specific columns and apply filters efficiently
            con = duckdb.connect()
            # Build the SELECT clause
            select_clause = "*" if not columns else ", ".join(columns)
            # Start building the query
            query = f"SELECT {select_clause} FROM read_csv_auto('{file_path}')"
            # Apply filters
            if filters:
                filter_clauses = []
                for column, values in filters.items():
                    if isinstance(values, list):
                        # Escape single quotes and wrap values in quotes
                        values_list = ', '.join(["'" + str(value).replace("'", "''") + "'" for value in values])
                        filter_clauses.append(f"{column} IN ({values_list})")
                    else:
                        value = str(values).replace("'", "''")
                        filter_clauses.append(f"{column} = '{value}'")
                if filter_clauses:
                    query += " WHERE " + " AND ".join(filter_clauses)
            # Apply sample size limit
            if sample_size:
                query += f" LIMIT {sample_size}"
            # Execute the query and fetch the data
            df = con.execute(query).fetchdf()
            con.close()
        elif helper['file_type'] == 'parquet':
            df = load_parquet_with_tz(file_path, columns, filters, sample_size)
        else:
            raise ValueError("Unsupported filetype. Only 'csv' and 'parquet' are supported.")
        print(f"Data loaded successfully from {file_path}")
        df = _cast_id_cols_to_string(df) # Cast id columns to string
        return df
    else:
        raise FileNotFoundError(f"The file {file_path} does not exist in the specified directory.")

def convert_datetime_columns_to_site_tz(df, site_tz_str, verbose=True):
    """
    Convert all datetime columns in the DataFrame to the specified site timezone.

    Parameters:
    - df (pd.DataFrame): Input DataFrame.
    - site_tz_str (str): Timezone string, e.g., "America/New_York". or "US/Central"
    - verbose (bool): Whether to print detailed output (default: True).

    Returns:
    - pd.DataFrame: Modified DataFrame with datetime columns converted.
    """
    site_tz = pytz.timezone(site_tz_str)

    # Identify datetime-related columns
    dttm_columns = [col for col in df.columns if 'dttm' in col]

    for col in dttm_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            current_tz = df[col].dt.tz
            if current_tz == site_tz:
                if verbose:
                    print(f"{col}: Already in your timezone ({current_tz}), no conversion needed.")
            elif current_tz == pytz.UTC:
                print(f"{col}: null count before conversion= {df[col].isna().sum()}")
                df[col] = df[col].dt.tz_convert(site_tz)
                if verbose:
                    print(f"{col}: Converted from UTC to your timezone ({site_tz}).")
                    print(f"{col}: null count after conversion= {df[col].isna().sum()}")
            else:
                print(f"{col}: null count before conversion= {df[col].isna().sum()}")
                df[col] = df[col].dt.tz_convert(site_tz)
                if verbose:
                    print(f"{col}: Your timezone is {current_tz}, Converting to your site timezone ({site_tz}).")
                    print(f"{col}: null count after conversion= {df[col].isna().sum()}")
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            print(f"WARNING: {col}: Naive datetime detected — localizing to {site_tz}. Please check your ETL!")
            df[col] = df[col].dt.tz_localize(site_tz, ambiguous=True, nonexistent='shift_forward')
        else:
            if verbose:
                print(f"WARNING: {col}: Not a datetime column. Please check ETL and run again!")
    return df



def run_duckdb_batched(df, query_fn, *, batch_col='encounter_block',
                       batch_size=None, min_rows_to_batch=10_000_000,
                       register_as='src'):
    """
    Run a DuckDB query in batches to limit peak memory.

    Parameters:
        df: Input DataFrame.
        query_fn: Callable(con, table_name) -> SQL string.
        batch_col: Column to batch on (unique values split into chunks).
        batch_size: Blocks per batch. Defaults to config 'duckdb_batch_size' or 500.
        min_rows_to_batch: Only batch if df has more rows than this (default 10M).
                           Set to 0 to always batch.
        register_as: Name to register the DataFrame as in DuckDB.

    Returns:
        pd.DataFrame: Concatenated results from all batches.
    """
    if batch_size is None:
        batch_size = helper.get('duckdb_batch_size', 500)

    blocks = df[batch_col].unique()

    # Skip batching for small data or when blocks fit in one batch
    if len(blocks) <= batch_size or len(df) < min_rows_to_batch:
        con = duckdb.connect()
        con.register(register_as, df)
        sql = query_fn(con, register_as)
        result = con.execute(sql).fetchdf()
        con.unregister(register_as)
        con.close()
        return result

    # Batch processing
    chunks = [blocks[i:i + batch_size] for i in range(0, len(blocks), batch_size)]
    parts = []
    for chunk in chunks:
        batch_df = df[df[batch_col].isin(chunk)]
        con = duckdb.connect()
        con.register(register_as, batch_df)
        sql = query_fn(con, register_as)
        parts.append(con.execute(sql).fetchdf())
        con.unregister(register_as)
        con.close()

    return pd.concat(parts, ignore_index=True)


def pivot_vitals_duckdb(df):
    """
    Pivot long-format vitals to wide using DuckDB with batching.

    Parameters:
        df (pd.DataFrame): Long-format vitals with columns:
            encounter_block, recorded_date, recorded_hour, vital_category, vital_value

    Returns:
        pd.DataFrame: Wide-format with columns like min_heart_rate, max_heart_rate, avg_heart_rate, etc.
    """
    # Discover categories from full dataset first
    con = duckdb.connect()
    con.register('_cat_scan', df)
    categories = [row[0] for row in
                  con.execute("SELECT DISTINCT vital_category FROM _cat_scan ORDER BY vital_category").fetchall()]
    con.unregister('_cat_scan')
    con.close()

    agg_parts = []
    for cat in categories:
        agg_parts.append(f"MIN(CASE WHEN vital_category = '{cat}' THEN vital_value END) AS min_{cat}")
        agg_parts.append(f"MAX(CASE WHEN vital_category = '{cat}' THEN vital_value END) AS max_{cat}")
        agg_parts.append(f"AVG(CASE WHEN vital_category = '{cat}' THEN vital_value END) AS avg_{cat}")

    def _query(con, tbl):
        return f"""
            SELECT encounter_block, recorded_date, recorded_hour,
                   {', '.join(agg_parts)}
            FROM {tbl}
            GROUP BY encounter_block, recorded_date, recorded_hour
            ORDER BY encounter_block, recorded_date, recorded_hour
        """

    return run_duckdb_batched(df, _query)


def generate_hourly_scaffold_duckdb(final_blocks,
                                     start_col='block_vent_start_dttm',
                                     end_col='block_last_vital_dttm',
                                     block_col='encounter_block'):
    """
    Generate an hourly scaffold (one row per block per hour) using DuckDB generate_series.

    Parameters:
        final_blocks: DataFrame with one row per encounter_block, containing start/end timestamps.
        start_col: Column name for scaffold start time.
        end_col: Column name for scaffold end time.
        block_col: Column name for block identifier.

    Returns:
        pd.DataFrame: Columns [encounter_block, recorded_date, recorded_hour].
    """
    con = duckdb.connect()
    con.register('blocks_tbl', final_blocks)

    query = f"""
        SELECT
            b.{block_col} AS encounter_block,
            CAST(DATE_TRUNC('day', ts) AS DATE) AS recorded_date,
            CAST(EXTRACT(HOUR FROM ts) AS INTEGER) AS recorded_hour
        FROM blocks_tbl b,
             LATERAL UNNEST(
                 generate_series(b.{start_col}, b.{end_col}, INTERVAL '1 HOUR')
             ) AS t(ts)
        GROUP BY b.{block_col}, recorded_date, recorded_hour
        ORDER BY b.{block_col}, recorded_date, recorded_hour
    """

    result = con.execute(query).fetchdf()
    con.unregister('blocks_tbl')
    con.close()

    # Convert recorded_date to tz-naive datetime to match pandas convention
    result['recorded_date'] = pd.to_datetime(result['recorded_date']).dt.tz_localize(None)

    return result


def aggregate_hourly_vent_duckdb(df):
    """
    Aggregate respiratory support data to hourly min/max using DuckDB with batching.

    Parameters:
        df: DataFrame with columns: encounter_block, recorded_date, recorded_hour,
            fio2_set, peep_set, lpm_set, resp_rate_obs, tracheostomy_filled, on_vent.

    Returns:
        pd.DataFrame: Hourly aggregated vent data.
    """
    def _query(con, tbl):
        return f"""
            SELECT encounter_block, recorded_date, recorded_hour,
                   MIN(fio2_set) AS min_fio2_set,
                   MAX(fio2_set) AS max_fio2_set,
                   MIN(peep_set) AS min_peep_set,
                   MAX(peep_set) AS max_peep_set,
                   MIN(lpm_set) AS min_lpm_set,
                   MAX(lpm_set) AS max_lpm_set,
                   MIN(resp_rate_obs) AS min_resp_rate_obs,
                   MAX(resp_rate_obs) AS max_resp_rate_obs,
                   MAX(tracheostomy_filled) AS hourly_trach,
                   MAX(on_vent) AS hourly_on_vent
            FROM {tbl}
            GROUP BY encounter_block, recorded_date, recorded_hour
            ORDER BY encounter_block, recorded_date, recorded_hour
        """

    return run_duckdb_batched(df, _query)


def pivot_meds_duckdb(df, dose_col='med_dose_converted', category_col='med_category',
                      order_col='recorded_dttm'):
    """
    Pivot long-format meds to wide using DuckDB with batching.

    Parameters:
        df: DataFrame with columns including encounter_block, recorded_date, recorded_hour,
            med_category, med_dose_converted, and an ordering column (recorded_dttm).
        dose_col: Column containing dose values.
        category_col: Column containing med category names.
        order_col: Column to determine first/last ordering within each hour.

    Returns:
        pd.DataFrame: Wide-format with columns like min_norepinephrine, max_norepinephrine,
                       first_norepinephrine, last_norepinephrine, etc.
    """
    # Discover categories from full dataset
    con = duckdb.connect()
    con.register('_cat_scan', df)
    categories = [row[0] for row in
                  con.execute(f"SELECT DISTINCT {category_col} FROM _cat_scan ORDER BY {category_col}").fetchall()]
    con.unregister('_cat_scan')
    con.close()

    agg_parts = []
    for cat in categories:
        agg_parts.append(
            f"MIN(CASE WHEN {category_col} = '{cat}' THEN {dose_col} END) AS min_{cat}")
        agg_parts.append(
            f"MAX(CASE WHEN {category_col} = '{cat}' THEN {dose_col} END) AS max_{cat}")
        agg_parts.append(
            f"ARG_MIN(CASE WHEN {category_col} = '{cat}' THEN {dose_col} END, "
            f"CASE WHEN {category_col} = '{cat}' THEN {order_col} END) AS first_{cat}")
        agg_parts.append(
            f"ARG_MAX(CASE WHEN {category_col} = '{cat}' THEN {dose_col} END, "
            f"CASE WHEN {category_col} = '{cat}' THEN {order_col} END) AS last_{cat}")

    def _query(con, tbl):
        return f"""
            SELECT encounter_block, recorded_date, recorded_hour,
                   {', '.join(agg_parts)}
            FROM {tbl}
            GROUP BY encounter_block, recorded_date, recorded_hour
            ORDER BY encounter_block, recorded_date, recorded_hour
        """

    return run_duckdb_batched(df, _query)


def count_unique_encounters(df, encounter_column='hospitalization_id'):
    """
    Counts the unique encounters in a DataFrame.
    
    Parameters:
    df (DataFrame): The DataFrame to analyze.
    encounter_column (str): The name of the column containing encounter IDs (default is 'hospitalization_id').
    
    Returns:
    int: The number of unique encounters.
    """
    return df[encounter_column].nunique()

def generate_facetgrid_histograms(data, category_column, value_column):
    """
    Generate histograms using seaborn's FacetGrid.

    Parameters:
        data (DataFrame): DataFrame containing the data.
        category_column (str): Name of the column containing categories.
        value_column (str): Name of the column containing values.

    Returns:
        FacetGrid: Seaborn FacetGrid object containing the generated histograms.
    """
    # Create a FacetGrid
    g = sns.FacetGrid(data, col=category_column, col_wrap=6, sharex=False, sharey=False)
    g.map(sns.histplot, value_column, bins=30, color='blue', edgecolor='black')

    # Set titles and labels
    g.set_titles('{col_name}')
    g.set_axis_labels(value_column, 'Frequency')

    # Adjust layout
    plt.subplots_adjust(top=0.9)
    g.fig.suptitle(f'Histograms of {value_column} by {category_column}', fontsize=16)

    return g

def map_race_column(df, race_column='race'):
    """
    Function to map race values to simplified categories.

    Args:
    - df (pandas.DataFrame): Input DataFrame containing the race data.
    - race_column (str): The name of the race column. Default is 'race'.

    Returns:
    - pandas.DataFrame: DataFrame with a new column 'race_new' containing the mapped race values.
    """
    # Define the mapping
    race_mapping = {
        'black or african-american': 'Black',
        'black or african american': 'Black',
        'white': 'White',
        'asian': 'Other',
        'american indian or alaska native': 'Other',
        'native hawaiian or other pacific islander': 'Other',
        'other': 'Other',
        'unknown': 'Other'
    }

    # Normalize to lowercase before mapping (some sites store different cases)
    df['race_new'] = df[race_column].str.lower().map(race_mapping).fillna('Missing')

    return df


def remove_duplicates(df, columns, df_name):
    """
    Checks for and removes duplicate rows in a DataFrame based on the combination of specified columns.

    Parameters:
        df (DataFrame): The DataFrame to clean.
        columns (list): A list of columns to use for identifying duplicates.
        df_name (str): The name of the DataFrame (for display purposes).

    Returns:
        DataFrame: The DataFrame with duplicates removed.
    """
    # Check for duplicates based on the combination of specified columns
    initial_count = len(df)
    duplicates = df[df.duplicated(subset=columns, keep=False)]
    
    print(f"Processing DataFrame: {df_name}")
    
    if not duplicates.empty:
        num_duplicates = len(duplicates)
        print(f"Found {num_duplicates} duplicate rows based on columns: {columns}")
        
        # Drop duplicates, keeping the first occurrence
        df_cleaned = df.drop_duplicates(subset=columns, keep='first')
        final_count = len(df_cleaned)
        duplicates_dropped = initial_count - final_count
        
        print(f"Dropped {duplicates_dropped} duplicate rows. New DataFrame has {final_count} rows.")
    else:
        df_cleaned = df
        print(f"No duplicates found based on columns: {columns}.")
    
    return df_cleaned

def apply_outlier_thresholds(df, col_name, min_val, max_val):
    """
    Helper function to clamp column values between min and max thresholds, 
    setting values outside range to NaN.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing the column to process
        col_name (str): Name of the column to apply thresholds to
        min_val (float): Minimum allowed value (inclusive)
        max_val (float): Maximum allowed value (inclusive)
        
    Returns:
        None: Modifies the DataFrame in place by updating the specified column
    """
    df[col_name] = df[col_name].where(df[col_name].between(min_val, 
                                                           max_val, 
                                                           inclusive='both'), 
                                                           np.nan)

def stitch_encounters(hospitalization, adt, time_interval=6):
    """
    Stitches together related hospital encounters that occur within a specified time interval of each other.
    
    Args:
        hospitalization (pd.DataFrame): Hospitalization table with required columns
        adt (pd.DataFrame): ADT table with required columns
        time_interval (int, optional): Number of hours between encounters to consider them linked. Defaults to 6.
        
    Returns:
        pd.DataFrame: Stitched encounters with encounter blocks
    """
    hospitalization_filtered = hospitalization[["patient_id","hospitalization_id","admission_dttm",
                                                "discharge_dttm","age_at_admission",  "discharge_category"]].copy()
    hospitalization_filtered['admission_dttm'] = pd.to_datetime(hospitalization_filtered['admission_dttm'])
    hospitalization_filtered['discharge_dttm'] = pd.to_datetime(hospitalization_filtered['discharge_dttm'])

    hosp_adt_join = pd.merge(hospitalization_filtered[["patient_id","hospitalization_id","age_at_admission",
                                                       "admission_dttm","discharge_dttm",
                                                        "discharge_category"]], 
                      adt[["hospitalization_id","in_dttm","out_dttm","location_category","hospital_id"]],
                 on="hospitalization_id",how="left")

    hospital_cat = hosp_adt_join[["hospitalization_id","in_dttm","out_dttm","hospital_id"]]

    # Step 1: Sort by patient_id and admission_dttm
    hospital_block = hosp_adt_join[["patient_id","hospitalization_id","admission_dttm","discharge_dttm", "age_at_admission",  "discharge_category"]]
    hospital_block = hospital_block.drop_duplicates()
    hospital_block = hospital_block.sort_values(by=["patient_id", "admission_dttm"]).reset_index(drop=True)
    hospital_block = hospital_block[["patient_id","hospitalization_id","admission_dttm","discharge_dttm", "age_at_admission",  "discharge_category"]]

    # Step 2: Calculate time between discharge and next admission
    hospital_block["next_admission_dttm"] = hospital_block.groupby("patient_id")["admission_dttm"].shift(-1)
    hospital_block["discharge_to_next_admission_hrs"] = (
        (hospital_block["next_admission_dttm"] - hospital_block["discharge_dttm"]).dt.total_seconds() / 3600
    )

    # Step 3: Create linked column based on time_interval
    hospital_block["linked6hrs"] = hospital_block["discharge_to_next_admission_hrs"] < time_interval

    # Sort values to ensure correct order
    hospital_block = hospital_block.sort_values(by=["patient_id", "admission_dttm"]).reset_index(drop=True)

    # Initialize encounter_block with row indices + 1
    hospital_block['encounter_block'] = hospital_block.index + 1

    # Iteratively propagate the encounter_block values
    while True:
        shifted = hospital_block['encounter_block'].shift(-1)
        mask = hospital_block['linked6hrs'] & (hospital_block['patient_id'] == hospital_block['patient_id'].shift(-1))
        hospital_block.loc[mask, 'encounter_block'] = shifted[mask]
        if hospital_block['encounter_block'].equals(hospital_block['encounter_block'].bfill()):
            break

    hospital_block['encounter_block'] = hospital_block['encounter_block'].bfill(downcast='int')
    hospital_block = pd.merge(hospital_block,hospital_cat,how="left",on="hospitalization_id")
    hospital_block = hospital_block.sort_values(by=["patient_id", "admission_dttm","in_dttm","out_dttm"]).reset_index(drop=True)
    hospital_block = hospital_block.drop_duplicates()

    hospital_block2 = hospital_block.groupby(['patient_id','encounter_block']).agg(
        admission_dttm=pd.NamedAgg(column='admission_dttm', aggfunc='min'),
        discharge_dttm=pd.NamedAgg(column='discharge_dttm', aggfunc='max'),
        # admission_type_category=pd.NamedAgg(column='admission_type_category', aggfunc='first'),
        discharge_category=pd.NamedAgg(column='discharge_category', aggfunc='last'),
        hospital_id = pd.NamedAgg(column='hospital_id', aggfunc='last'),
        age_at_admission=pd.NamedAgg(column='age_at_admission', aggfunc='last'),
        list_hospitalization_id=pd.NamedAgg(column='hospitalization_id', aggfunc=lambda x: sorted(x.unique()))
    ).reset_index()

    df = pd.merge(hospital_block[["patient_id",
                                  "hospitalization_id",
                                  "encounter_block"]].drop_duplicates(),
             hosp_adt_join[["hospitalization_id","location_category","in_dttm","out_dttm"]], on="hospitalization_id",how="left")

    df = pd.merge(df,hospital_block2[["encounter_block",
                                      "admission_dttm",
                                      "discharge_dttm",
                                      "discharge_category",
                                      "hospital_id",
                                     "list_hospitalization_id"]],on="encounter_block",how="left")
    df = df.drop_duplicates(subset=["patient_id","encounter_block","in_dttm","out_dttm","location_category"])
    
    return df

def create_summary_table(
    df, 
    numeric_col, 
    group_by_cols=None
):
    """
    Create a summary table for a given numeric column in a DataFrame, optionally grouped.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame containing your data.
    numeric_col : str
        The name of the numeric column to summarize (e.g., 'fio2_set').
    group_by_cols : str or list of str, optional
        Column name(s) to group by (e.g., 'device_category', ['device_category','mode_category'], etc.).
        If None, the function provides a single overall summary for the entire df.

    Returns
    -------
    pd.DataFrame
        A DataFrame with columns for each statistic: 
        ['N', 'missing', 'min', 'q25', 'median', 'q75', 'mean', 'max'].
        If group_by_cols is provided, those columns appear first in the output.
    """

    # 1) Define helper functions for quartiles & missing
    def q25(x):
        return x.quantile(0.25)
    q25.__name__ = 'q25'

    def q50(x):
        return x.quantile(0.50)
    q50.__name__ = 'median'

    def q75(x):
        return x.quantile(0.75)
    q75.__name__ = 'q75'

    def missing_count(x):
        return x.isna().sum()
    missing_count.__name__ = 'missing'

    # 2) Build an aggregation dictionary for the chosen numeric_col
    #    It includes N (count of non-null), missing, min, q25, median, q75, mean, max
    agg_dict = {
        numeric_col: [
            'count',       # Non-missing count
            missing_count, # Missing
            'min',
            q25,
            q50,
            q75,
            'mean',
            'max'
        ]
    }

    # 3) Perform groupby if group_by_cols is provided, else do a global summary
    if group_by_cols is not None:
        # Accept a single string or a list of strings
        if isinstance(group_by_cols, str):
            group_by_cols = [group_by_cols]
        summary = df.groupby(group_by_cols).agg(agg_dict)
    else:
        # No grouping => just aggregate the entire DataFrame
        summary = df.agg(agg_dict)

    # 4) The result is a multi-level column index. Flatten it.
    #    We'll get something like:
    #       (numeric_col, 'count'), (numeric_col, '<lambda>'), ...
    #    After flattening, rename the stats to a friendlier label.
    summary.columns = summary.columns.droplevel(0)  # drop the numeric_col level
    # summary now has columns like: ['count','missing','min','q25','median','q75','mean','max']

    # 5) Optionally, reorder columns in a nice sequence
    #    We'll define the exact ordering we want:
    desired_order = ['count','missing','min','q25','median','q75','mean','max']
    # Some columns might have <lambda> instead of 'missing' if the function name wasn't recognized
    # We can do a manual rename if needed.
    rename_map = {}
    for col in summary.columns:
        if '<lambda>' in col:
            rename_map[col] = 'missing'  # rename the lambda to 'missing'
    summary.rename(columns=rename_map, inplace=True)

    # Now reorder columns if they all exist
    existing_cols = [c for c in desired_order if c in summary.columns]
    summary = summary[existing_cols]  # reorder if possible

    # 6) If we had group_by_cols, reset_index so those become DataFrame columns
    if group_by_cols is not None:
        summary = summary.reset_index()

    # 7) Final step: rename for clarity, e.g. rename 'count' -> 'N' if desired
    rename_final = {
        'count': 'N'
    }
    summary.rename(columns=rename_final, inplace=True)

    return summary

def is_dose_within_range(row, outlier_dict):
    '''
    Check if med_dose_converted is within the outlier-configured range for this med_category.
    Parameters:
        row (pd.Series): A row from a DataFrame, must include 'med_category' and 'med_dose_converted'.
        outlier_dict (dict): Dictionary of min/max pairs from outlier_config.json.
    Returns:
        bool: True if the dose is within range or if med_category is not found, False otherwise.
    '''
    med_category = row['med_category']
    med_dose_converted = row['med_dose_converted']
    dose_range = outlier_dict.get(med_category, None)
    if dose_range is None:
        return False
    min_dose, max_dose = dose_range
    return min_dose <= med_dose_converted <= max_dose

## meds dose conversion helpers

# Define medications and their unit conversion information
meds_list = [
    "norepinephrine", "epinephrine", "phenylephrine",
    "vasopressin", "dopamine", "angiotensin", "metaraminol", "dobutamine"
]

med_unit_info = {
    'norepinephrine': {
        'required_unit': 'mcg/kg/min',
        'acceptable_units': ['mcg/kg/min', 'mcg/kg/hr', 'mg/kg/hr', 'mcg/min', 'mg/hr'],
    },
    'epinephrine': {
        'required_unit': 'mcg/kg/min',
        'acceptable_units': ['mcg/kg/min', 'mcg/kg/hr', 'mg/kg/hr', 'mcg/min', 'mg/hr'],
    },
    'phenylephrine': {
        'required_unit': 'mcg/kg/min',
        'acceptable_units': ['mcg/kg/min', 'mcg/kg/hr', 'mg/kg/hr', 'mcg/min', 'mg/hr'],
    },
    'dopamine': {
        'required_unit': 'mcg/kg/min',
        'acceptable_units': ['mcg/kg/min', 'mcg/kg/hr', 'mg/kg/hr', 'mcg/min', 'mg/hr'],
    },
    'dobutamine': {
        'required_unit': 'mcg/kg/min',
        'acceptable_units': ['mcg/kg/min', 'mcg/kg/hr', 'mg/kg/hr', 'mcg/min', 'mg/hr'],
    },
    'metaraminol': {
        'required_unit': 'mcg/kg/min',
        'acceptable_units': ['mg/hr', 'mcg/min'],
    },
    'angiotensin': {
        'required_unit': 'mcg/kg/min',
        'acceptable_units': ['ng/kg/min', 'ng/kg/hr'],
    },
    'vasopressin': {
        'required_unit': 'units/min',
        'acceptable_units': ['units/min', 'units/hr', 'milliunits/min', 'milliunits/hr', 'milli-units/kg/hr',
                             'milli-units/min', 'milli-units/hr','milli-units/kg/h','milliunits/kg/h',
                             'milli-units/kg/min', 'milliunits/kg/min' ],
    },
}

def check_dose_unit(row):
    med_category = row['med_category']
    med_dose_unit = row['med_dose_unit']
    # Check if med_category exists in med_unit_info
    if med_category in med_unit_info:
        # Check if med_dose_unit is in the acceptable units
        if med_dose_unit in med_unit_info[med_category]['acceptable_units']:
            return "Valid"
        else:
            return "Not an acceptable unit"
    else:
        return "Not a vasoactive"
    
def has_per_hour_or_min(unit):
    if pd.isnull(unit):
        return False
    unit = unit.lower()
    return '/hr' in unit or '/min' in unit

import numpy as np
import pandas as pd          # <- make sure this was imported

def get_conversion_factor(med_category: str,
                          med_dose_unit: str,
                          weight_kg: Union[float, None]) -> Union[float, None]:
    """
    Return a multiplier that converts *med_dose* from its current unit
    to the **required** unit for that medication.

    For units that need weight (mcg/min  → mcg/kg/min, mg/hr → mcg/kg/min …)
    we return *None* when weight_kg is missing so that the caller can decide
    what to do (keep the row with NaN, drop it, etc.).
    """
    med_info = med_unit_info.get(med_category)
    if med_info is None:
        return None                          # not a vaso we care about

    med_dose_unit = (med_dose_unit or "").lower()

    # ── helpers ───────────────────────────────────────────────
    has_weight = weight_kg is not None and not pd.isna(weight_kg)
    def w_needed(factor_if_known):
        return factor_if_known if has_weight else None
    # ──────────────────────────────────────────────────────────

    if med_category in ["norepinephrine", "epinephrine",
                        "phenylephrine", "dopamine",
                        "dobutamine", "metaraminol"]:
        if med_dose_unit == "mcg/kg/min": return 1.0
        elif med_dose_unit == "mcg/kg/hr": return 1/60
        elif med_dose_unit == "mg/kg/hr": return 1000/60
        elif med_dose_unit == "mg/kg/min": return 1000
        elif med_dose_unit == "mcg/min": return w_needed(1/weight_kg)
        elif med_dose_unit == "mg/hr": return w_needed(1000/60/weight_kg)
    elif med_category == "angiotensin":
        if med_dose_unit == "ng/kg/min": return 1/1_000
        elif med_dose_unit == "ng/kg/hr": return 1/1_000/60
        elif med_dose_unit == "mcg/kg/min": return 1.0
    elif med_category == "vasopressin":
        if med_dose_unit == "units/min": return 1.0
        elif med_dose_unit in ["units/hr", "units/hour"]: return 1/60
        elif med_dose_unit == "milliunits/min": return 1/1_000
        elif med_dose_unit == "milli-units/min": return 1/1_000
        elif med_dose_unit == "milliunits/hr": return 1/1_000/60
        elif med_dose_unit == "milli-units/hr": return 1/1_000/60
        elif med_dose_unit == "milliunits/kg/hr": return w_needed(1/1_000/60/weight_kg)
        elif med_dose_unit == "milli-units/kg/hr": return w_needed(1/1_000/60/weight_kg)
        elif med_dose_unit == "milliunits/kg/min": return w_needed(1/1_000/weight_kg)
        elif med_dose_unit == "milli-units/kg/min": return w_needed(1/1_000/weight_kg)
    return None                               # unit not recognised


def convert_dose(row: pd.Series) -> Union[float, None]:
    """
    Convert `row.med_dose` to the medication's **required** unit.

    Returns `np.nan` when:
    * the unit is unrecognised
    * weight is required but missing
    """
    factor = get_conversion_factor(
        row["med_category"],
        row["med_dose_unit"],
        row["weight_kg"]
    )
    return np.nan if factor is None else round(row["med_dose"] * factor, 5)


def categorize_device(row):
    if pd.notna(row['device_category']):
        return row['device_category']
    elif row['mode_category'] in ["simv", "pressure-regulated volume control", "assist control-volume control"]:
        return "vent"
    elif pd.isna(row['device_category']) and row['fio2_set'] == 0.21 and pd.isna(row['lpm_set']) and pd.isna(row['peep_set']) and pd.isna(row['tidal_volume_set']):
        return "room air"
    elif pd.isna(row['device_category']) and pd.isna(row['fio2_set']) and row['lpm_set'] == 0 and pd.isna(row['peep_set']) and pd.isna(row['tidal_volume_set']):
        return "room air"
    elif pd.isna(row['device_category']) and pd.isna(row['fio2_set']) and (0 < row['lpm_set'] <= 20) and pd.isna(row['peep_set']) and pd.isna(row['tidal_volume_set']):
        return "nasal cannula"
    elif pd.isna(row['device_category']) and pd.isna(row['fio2_set']) and row['lpm_set'] > 20 and pd.isna(row['peep_set']) and pd.isna(row['tidal_volume_set']):
        return "high flow nc"
    elif row['device_category'] == "nasal cannula" and pd.isna(row['fio2_set']) and row['lpm_set'] > 20:
        return "high flow nc"
    else:
        return row['device_category']  # Keep original value if no condition is met

# Try to fill in FiO2 based on other values    
def refill_fio2(row):
    if pd.notna(row['fio2_set']):
        return row['fio2_set']  # Already normalized upstream (sofa_score.py:366-377)
    elif pd.isna(row['fio2_set']) and row['device_category'] == "room air":
        return 0.21 
    elif pd.isna(row['fio2_set']) and row['device_category'] == "nasal cannula" and pd.notna(row['lpm_set']):
        return (0.24 + (0.04 * row['lpm_set'])) 
    else:
        return np.nan

def merge_multiple_dfs(*dfs, on=None, how='outer'):
    """
    Merge multiple DataFrames on specified columns.
    Args:
        *dfs: Variable number of DataFrames to merge
        on: Column(s) to merge on
        how: Type of merge to perform
    Returns:
        Merged DataFrame
    """
    return reduce(lambda left, right: pd.merge(left, right, on=on, how=how), dfs)

def impute_fio2_from_nasal_cannula_flow(df):
    """
    Impute missing FiO2 values for nasal cannula based on oxygen flow rate (LPM)
    using the standard clinical conversion table.
    
    Parameters:
    df (pd.DataFrame): DataFrame with device_category, lpm_set, and fio2_set columns
    
    Returns:
    pd.DataFrame: DataFrame with updated fio2_set values
    """
    # Create lookup table from the clinical standard (your image)
    fio2_lookup = {
        1: 0.24,   # 1 L/min → 24%
        2: 0.28,   # 2 L/min → 28%  
        3: 0.32,   # 3 L/min → 32%
        4: 0.36,   # 4 L/min → 36%
        5: 0.40,   # 5 L/min → 40%
        6: 0.44,   # 6 L/min → 44%
        7: 0.48,   # 7 L/min → 48%
        8: 0.52,   # 8 L/min → 52%
        9: 0.56,   # 9 L/min → 56%
        10: 0.60   # 10 L/min → 60%
    }
    
    # Create mask for rows that need imputation
    nasal_cannula_mask = (
        (df['device_category'] == 'nasal cannula') &
        (df['fio2_set'].isna()) &  # Missing FiO2
        (df['lpm_set'].notna()) &  # Have LPM value
        (df['lpm_set'] >= 1) &     # Within lookup range  
        (df['lpm_set'] <= 10) &    # Within lookup range
        (df['lpm_set'] == df['lpm_set'].round())  # Integer values only
    )
    
    # Apply the lookup for eligible rows
    df.loc[nasal_cannula_mask, 'fio2_set'] = df.loc[nasal_cannula_mask, 'lpm_set'].map(fio2_lookup)
    
    # Report what was imputed
    n_imputed = nasal_cannula_mask.sum()
    if n_imputed > 0:
        print(f"[OK] Imputed FiO2 for {n_imputed:,} nasal cannula rows using LPM lookup table")
        
        # Show breakdown by LPM
        imputed_breakdown = df[nasal_cannula_mask]['lpm_set'].value_counts().sort_index()
        print("   Breakdown by LPM:")
        for lpm, count in imputed_breakdown.items():
            fio2_pct = int(fio2_lookup[lpm] * 100)
            print(f"   {lpm}L/min → {fio2_pct}%: {count:,} rows")
    else:
        print(" No nasal cannula rows needed FiO2 imputation")
    
    return df

def build_meds_hourly_scaffold(
        meds_df: pd.DataFrame,
        *,
        id_col: str = "encounter_block",
        ids=None,
        timestamp_col: str = "admin_dttm",
        site_tz: str = helper["timezone"]          # e.g. "US/Central"
) -> pd.DataFrame:
    # ── 0 · filter ------------------------------------------------------
    if ids is not None:
        meds_df = meds_df.loc[meds_df[id_col].isin(ids)].copy()
    if meds_df.empty:
        raise ValueError("After filtering no rows remain.")

    # ── 1 · ensure tz‑aware in the requested local zone ----------------
    ts_local = pd.to_datetime(meds_df[timestamp_col])
    if ts_local.dt.tz is None:
        ts_local = ts_local.dt.tz_localize(site_tz, ambiguous="NaT",
                                           nonexistent="shift_forward")
    else:
        ts_local = ts_local.dt.tz_convert(site_tz)

    meds_df = meds_df[[id_col]].copy()
    meds_df["local_ts"] = ts_local
    meds_df = meds_df.drop_duplicates([id_col, "local_ts"])

    # ── 2 · scaffold (rounding done in UTC → no ambiguity) --------------
    scaffolds = []
    for blk, g in meds_df.groupby(id_col, sort=False):
        # convert to UTC just for min/max/floor
        utc = g["local_ts"].dt.tz_convert("UTC")
        start_utc = utc.min().floor("h")
        end_utc   = utc.max().floor("h")
        hrs_utc   = pd.date_range(start_utc, end_utc, freq="h", tz="UTC")

        hrs_local = hrs_utc.tz_convert(site_tz)   # back to local zone
        scaffolds.append(pd.DataFrame({id_col: blk, "local_ts": hrs_local}))

    scaffold = pd.concat(scaffolds, ignore_index=True)

    # ── 3 · merge scaffold ←→ original rows -----------------------------
    out = (scaffold
           .merge(meds_df, on=[id_col, "local_ts"], how="left")
           .sort_values([id_col, "local_ts"])
           .reset_index(drop=True))

    # ── 4 · derive date/hour & drop duplicate local hours ---------------
    out["recorded_date"] = out["local_ts"].dt.normalize().dt.tz_localize(None)
    out["recorded_hour"] = out["local_ts"].dt.hour

    out = out.drop_duplicates([id_col, "recorded_date", "recorded_hour"],
                              keep="first")

    return (out.loc[:, [id_col, "recorded_date", "recorded_hour"]]
               .sort_values([id_col, "recorded_date", "recorded_hour"])
               .reset_index(drop=True))




# def extend_hourly_dataset(base_df, addon_df, merge_cols):
#     """
#     Extend `base_df` with additional rows from `addon_df` beyond the last hour recorded per encounter_block.
    
#     Parameters
#     ----------
#     base_df : pd.DataFrame
#         Main dataset that contains all expected hours.
#     addon_df : pd.DataFrame
#         Dataset with potentially additional rows that need to be appended after the last timestamp in base_df.
#     merge_cols : list
#         Columns to join on, usually: ['encounter_block', 'recorded_date', 'recorded_hour']
    
#     Returns
#     -------
#     pd.DataFrame
#         Combined DataFrame with original base rows + extra addon rows after the last timepoint in base.
#     """

#     # Step 1: Reconstruct timestamps
#     base_df = base_df.copy()
#     addon_df = addon_df.copy()
    
#     base_df['recorded_dttm'] = pd.to_datetime(base_df['recorded_date']) + pd.to_timedelta(base_df['recorded_hour'], unit='h')
#     addon_df['recorded_dttm'] = pd.to_datetime(addon_df['recorded_date']) + pd.to_timedelta(addon_df['recorded_hour'], unit='h')

#     # Step 2: Merge addon onto base
#     merged = pd.merge(base_df, addon_df.drop(columns='recorded_dttm', errors='ignore'), on=merge_cols, how='left')

#     # Step 3: Get last recorded_dttm per encounter_block
#     max_times = base_df.groupby('encounter_block')['recorded_dttm'].max().reset_index(name='max_seq_dttm')

#     # Step 4: Get extra rows from addon_df
#     extra_candidates = pd.merge(addon_df, max_times, on='encounter_block', how='left')
#     extra_rows = extra_candidates[extra_candidates['recorded_dttm'] > extra_candidates['max_seq_dttm']].drop(
#         columns=['recorded_dttm', 'max_seq_dttm']
#     )

#     # Step 5: Combine base + extra
#     combined_df = pd.concat([merged, extra_rows], ignore_index=True)

#     # Step 6: Clean up
#     combined_df = combined_df.drop(columns='recorded_dttm', errors='ignore')
#     combined_df = combined_df.sort_values(merge_cols).reset_index(drop=True)

#     return combined_df

from datetime import timedelta
import numpy as np
import pandas as pd

def extend_hourly_dataset(base_df, addon_df, merge_cols):
    """
    Extend `base_df` with extra rows from `addon_df` beyond the last hour per encounter_block,
    preserving all columns from both input DataFrames and filling any time gaps.
    
    Parameters
    ----------
    base_df : pd.DataFrame
        Main dataset with hourly scaffold (e.g., hourly_seq_block).
    addon_df : pd.DataFrame
        Dataset with additional data (e.g., hourly_vent_block).
    merge_cols : list of str
        Columns to join on (typically ['encounter_block', 'recorded_date', 'recorded_hour']).
    
    Returns
    -------
    pd.DataFrame
        Combined DataFrame with merged values, extra rows, gap-filled hours,
        and time_from_vent variables added.
    """

    base_df = base_df.copy()
    addon_df = addon_df.copy()

    # ── 1. Add timestamp columns for comparison ────────────────────────
    base_df['recorded_dttm'] = pd.to_datetime(base_df['recorded_date']) + pd.to_timedelta(base_df['recorded_hour'], unit='h')
    addon_df['recorded_dttm'] = pd.to_datetime(addon_df['recorded_date']) + pd.to_timedelta(addon_df['recorded_hour'], unit='h')

    # ── 2. Get last recorded time per encounter ────────────────────────
    max_times = (
        base_df.groupby('encounter_block')['recorded_dttm']
        .max().reset_index()
        .rename(columns={'recorded_dttm': 'max_seq_dttm'})
    )

    # ── 3. Find extra addon rows after last scaffold time ──────────────
    addon_with_max = pd.merge(addon_df, max_times, on='encounter_block', how='left')
    extra_rows = addon_with_max[addon_with_max['recorded_dttm'] > addon_with_max['max_seq_dttm']].copy()

    # ── 4. Fill gaps between scaffold and first extra row ─────────────
    max_times_dict = dict(zip(max_times['encounter_block'], pd.to_datetime(max_times['max_seq_dttm'])))
    gap_rows = []
    for enc_id, group in extra_rows.groupby('encounter_block'):
        max_time = max_times_dict[enc_id]
        first_extra_time = group['recorded_dttm'].min()

        if first_extra_time <= max_time + timedelta(hours=1):
            continue

        gap_range = pd.date_range(start=max_time + timedelta(hours=1), end=first_extra_time - timedelta(hours=1), freq='H')
        for dt in gap_range:
            gap_rows.append({
                'encounter_block': enc_id,
                'recorded_date': dt.normalize().replace(tzinfo=None),
                'recorded_hour': dt.hour,
                'recorded_dttm': dt
            })

    gap_df = pd.DataFrame(gap_rows)

    # ── 5. Ensure gap_df has all columns from base and addon ──────────
    full_cols = list(set(base_df.columns).union(set(addon_df.columns)))
    for col in full_cols:
        if col not in gap_df.columns:
            gap_df[col] = np.nan
    gap_df = gap_df[full_cols]

    # ── 6. Merge scaffold and addon ───────────────────────────────────
    scaffold_merged = pd.merge(
        base_df.drop(columns='recorded_dttm', errors='ignore'),
        addon_df.drop(columns='recorded_dttm', errors='ignore'),
        on=merge_cols,
        how='left'
    )

    # ── 7. Combine scaffold + gaps + extras ───────────────────────────
    combined_df = pd.concat(
        [scaffold_merged, gap_df.drop(columns=['recorded_dttm'], errors='ignore'), extra_rows.drop(columns=['recorded_dttm', 'max_seq_dttm'], errors='ignore')],
        ignore_index=True
    )

    # ── 8. Time from vent ─────────────────────────────────────────────
    combined_df = combined_df.sort_values(by=merge_cols).reset_index(drop=True)
    combined_df['time_from_vent'] = combined_df.groupby('encounter_block').cumcount()
    combined_df['time_from_vent_adjusted'] = np.where(
        combined_df['time_from_vent'] < 4, -1, combined_df['time_from_vent'] - 4
    )

    # ── 9. Column reordering ──────────────────────────────────────────
    id_cols = [col for col in combined_df.columns if col.endswith('id') and col not in ['encounter_block']]
    key_cols = ['encounter_block', 'recorded_date', 'recorded_hour', 'time_from_vent', 'time_from_vent_adjusted']
    other_cols = [col for col in combined_df.columns if col not in id_cols + key_cols]
    combined_df = combined_df[id_cols + key_cols + other_cols]

    combined_df['encounter_block'] = combined_df['encounter_block'].astype(int)
    combined_df['recorded_hour']   = combined_df['recorded_hour'].astype(int)

    return combined_df
