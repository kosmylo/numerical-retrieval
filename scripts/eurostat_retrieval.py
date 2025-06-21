import eurostat
import pandas as pd
import json
import os
import logging
from datetime import datetime

EUROSTAT_DATASETS = {
    "annual_energy_balances": "nrg_bal_s",
    "renewable_energy_share": "nrg_ind_ren",
    "energy_efficiency_indicators": "nrg_ind_eff",
    "ghg_emissions_energy": "env_air_gge",
    "electricity_prices": "nrg_pc_204",
    "gas_prices": "nrg_pc_202",
    "population": "demo_pjan",
    "gdp": "nama_10_gdp",
    "households_number": "lfst_hhnhtych",
    "inability_to_keep_home_adequately_warm": "ilc_mdes01",
    "final_energy_consumption_households_per_capita": "sdg_07_20",
    "energy_intensity_of_economy": "nrg_ind_ei",
    "energy_import_dependency": "nrg_ind_id"  
}

def fetch_eurostat_data(dataset_code, countries):
    logging.info(f"Fetching Eurostat data for dataset {dataset_code}")
    try:
        df = eurostat.get_data_df(dataset_code, flags=False)
    except Exception as e:
        logging.error(f"Failed to fetch dataset {dataset_code}: {e}")
        raise

    geo_col = next((col for col in df.columns if 'geo' in col.lower()), None)
    if not geo_col:
        logging.error(f"No geographic column found for dataset {dataset_code}")
        raise ValueError(f"Geo column not found in dataset {dataset_code}")

    df_filtered = df[df[geo_col].isin(countries)].copy()
    year_cols = [col for col in df_filtered.columns if col[0].isdigit()]

    df_long = df_filtered.melt(
        id_vars=[col for col in df_filtered.columns if col not in year_cols],
        value_vars=year_cols,
        var_name='period',
        value_name='value'
    )

    df_long.dropna(subset=['value'], inplace=True)
    
    df_long['year'] = df_long['period'].str.extract('(\d{4})').astype(int)
    df_long['period_detail'] = df_long['period']

    logging.info(f"Fetched {len(df_long)} records for dataset {dataset_code}")

    return df_long

def retrieve_eurostat_datasets(output_dir, start_year=None, countries=None):
    os.makedirs(output_dir, exist_ok=True)
    for dataset_name, dataset_code in EUROSTAT_DATASETS.items():
        logging.info(f"Retrieving Eurostat dataset: {dataset_name} ({dataset_code})")

        try:
            df = fetch_eurostat_data(dataset_code, countries)
            
            if start_year:
                df = df[df['year'] >= start_year]
                logging.info(f"Filtered data from year {start_year} onwards. Records remaining: {len(df)}")

            if df.empty:
                logging.warning(f"No data available for {dataset_name} after filtering by year {start_year}")
                continue

            csv_path = os.path.join(output_dir, f"{dataset_name}.csv")
            metadata_path = os.path.join(output_dir, f"{dataset_name}_metadata.json")

            df.to_csv(csv_path, index=False)
            logging.info(f"Saved dataset CSV: {csv_path}")

            metadata = {
                "dataset_name": dataset_name,
                "dataset_code": dataset_code,
                "countries": countries,
                "retrieved_timestamp": datetime.now().isoformat(),
                "data_source": "Eurostat",
                "url": f"https://ec.europa.eu/eurostat/databrowser/view/{dataset_code}/default/table",
                "columns": df.columns.tolist(),
                "number_of_records": len(df),
                "time_coverage": {
                    "start_period": df['period_detail'].min(),
                    "end_period": df['period_detail'].max()
                }
            }

            with open(metadata_path, 'w', encoding='utf-8') as meta_file:
                json.dump(metadata, meta_file, ensure_ascii=False, indent=4)
            logging.info(f"Saved metadata JSON: {metadata_path}")

        except Exception as e:
            logging.error(f"Failed retrieving {dataset_name} ({dataset_code}): {e}")