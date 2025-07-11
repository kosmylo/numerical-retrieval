import os
import pandas as pd
import json
import logging
from pathlib import Path
from glob import glob

def merge_monthly_weather_to_yearly(input_folder, output_folder):
    input_path = Path(input_folder)
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)

    monthly_files = glob(str(input_path / '*.csv'))
    file_groups = {}

    for filepath in monthly_files:
        filename = os.path.basename(filepath)
        parts = filename.split('_')

        if len(parts) >= 4:
            country = parts[0]
            city = parts[1]
            start_date = parts[2]
            year = start_date[:4]

            key = (country, city, year)
            file_groups.setdefault(key, []).append(filepath)

    # Merge files explicitly per key
    for (country, city, year), files in file_groups.items():
        try:
            logging.info(f"Merging weather data for {city}, {country}, year {year}")

            combined_df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)

            # Convert 'date' explicitly to datetime
            combined_df['date'] = pd.to_datetime(combined_df['date'])
            combined_df.sort_values('date', inplace=True)

            # Drop duplicates explicitly
            combined_df.drop_duplicates(subset=['date'], keep='first', inplace=True)

            # Explicitly construct filename
            yearly_filename = f"{country}_{city}_{year}.csv"
            combined_df.to_csv(output_path / yearly_filename, index=False)

            # Explicitly handle metadata
            metadata_files = [f.replace('.csv', '_metadata.json') for f in files]
            metadata_details = []
            for mf in metadata_files:
                if os.path.exists(mf):
                    with open(mf, 'r', encoding='utf-8') as file:
                        metadata_details.append(json.load(file))

            # Prepare yearly metadata explicitly
            metadata = {
                "country": country,
                "city": city,
                "year": year,
                "variables": metadata_details[0]["variables"] if metadata_details else {},
                "daily_value_description": metadata_details[0]["daily_value_description"] if metadata_details else "",
                "unit": "varies",  # since multiple variables have different units
                "period_start": combined_df['date'].min().strftime('%Y-%m-%d'),
                "period_end": combined_df['date'].max().strftime('%Y-%m-%d'),
                "source": "Open-Meteo Archive API",
                "license": "Open data (Open-Meteo)",
                "retrieval_timestamp": pd.Timestamp.now().isoformat(),
                "source_files": [os.path.basename(f) for f in files]
            }

            metadata_filename = yearly_filename.replace('.csv', '_metadata.json')
            with open(output_path / metadata_filename, 'w', encoding='utf-8') as meta_file:
                json.dump(metadata, meta_file, indent=4)

            logging.info(f"Saved yearly weather data: {yearly_filename}")
            logging.info(f"Saved yearly metadata: {metadata_filename}")

        except Exception as e:
            logging.error(f"Error merging data for {city}, {country}, {year}: {e}")