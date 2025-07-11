import os
import pandas as pd
import logging
from pathlib import Path
from glob import glob

def merge_monthly_to_yearly(input_folder, output_folder):
    monthly_folder = Path(input_folder)
    yearly_folder = Path(output_folder)
    yearly_folder.mkdir(parents=True, exist_ok=True)

    # Clearly find all unique combinations of country, bidding_zone, and dataset
    monthly_files = glob(str(monthly_folder / '*.csv'))
    file_groups = {}

    for filepath in monthly_files:
        filename = os.path.basename(filepath)
        parts = filename.split('_')

        if len(parts) >= 5:
            country = parts[0]
            bidding_zone = parts[1]
            dataset = '_'.join(parts[2:-2])  # handles datasets with underscores clearly
            start_date = parts[-2]
            year = start_date[:4]

            key = (country, bidding_zone, dataset, year)
            file_groups.setdefault(key, []).append(filepath)

    # Merge explicitly by each key clearly
    for (country, bidding_zone, dataset, year), files in file_groups.items():
        try:
            logging.info(f"Merging files for {country} {bidding_zone} {dataset} {year}")
            combined_df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)
            
            # Sort explicitly by timestamp to ensure chronological order
            combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
            combined_df.sort_values('timestamp', inplace=True)

            # Drop duplicates explicitly
            combined_df.drop_duplicates(subset=['timestamp', 'production_type'], keep='first', inplace=True)

            # Save explicitly yearly CSV
            yearly_filename = f"{country}_{bidding_zone}_{dataset}_{year}.csv"
            combined_df.to_csv(yearly_folder / yearly_filename, index=False)

            # Explicitly merge metadata
            metadata_files = [f.replace('.csv', '_metadata.json') for f in files if os.path.exists(f.replace('.csv', '_metadata.json'))]
            metadata = {
                "country": country,
                "bidding_zone": bidding_zone,
                "dataset": dataset,
                "unit": combined_df['unit'].iloc[0] if 'unit' in combined_df.columns else "",
                "period_start": combined_df['timestamp'].min().isoformat(),
                "period_end": combined_df['timestamp'].max().isoformat(),
                "retrieval_timestamp": pd.Timestamp.now().isoformat(),
                "source_files": [os.path.basename(f) for f in files]
            }

            yearly_metadata_filename = yearly_filename.replace('.csv', '_metadata.json')
            pd.Series(metadata).to_json(yearly_folder / yearly_metadata_filename, indent=4)

            logging.info(f"Saved yearly data: {yearly_filename}")
            logging.info(f"Saved yearly metadata: {yearly_metadata_filename}")

        except Exception as e:
            logging.error(f"Error processing {country} {bidding_zone} {dataset} {year}: {e}")