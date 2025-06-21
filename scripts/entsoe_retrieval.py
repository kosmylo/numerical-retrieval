import os
import requests
import xmltodict
import pandas as pd
import json
import logging
from datetime import datetime, timedelta
import pytz

# ENTSO-E API endpoint and security token from environment variable
BASE_URL = "https://web-api.tp.entsoe.eu/api"
API_TOKEN = os.getenv("ENTSOE_API_TOKEN")

# Mapping of countries to their bidding zone codes (for day-ahead prices)
BIDDING_ZONES = {
    "Germany": [("10Y1001A1001A82H", "DE-LU")],  # Germany–Luxembourg bidding zone
    "Denmark": [("10YDK-1--------W", "DK1"), ("10YDK-2--------M", "DK2")],  # Western & Eastern Denmark
    "Sweden": [("10Y1001A1001A44P", "SE1"), ("10Y1001A1001A45N", "SE2"),
               ("10Y1001A1001A46L", "SE3"), ("10Y1001A1001A47J", "SE4")],  # Sweden's four bidding zones
    "Italy": [("10Y1001A1001A73I", "NORD"), ("10Y1001A1001A70O", "CNOR"),
              ("10Y1001A1001A71M", "CSUD"), ("10Y1001A1001A788", "SUD"),
              ("10Y1001A1001A74G", "SARD"), ("10Y1001A1001A75E", "SICI")]   # Italy's main zones
}

# Production type mapping for generation (ENTSO-E PSR codes to human-readable labels)
PSR_TYPE_MAP = {
    'B01': 'Biomass',
    'B02': 'Fossil Brown coal/Lignite',
    'B03': 'Fossil Coal-derived gas',
    'B04': 'Fossil Gas',
    'B05': 'Fossil Hard coal',
    'B06': 'Fossil Oil',
    'B07': 'Fossil Oil shale',
    'B08': 'Fossil Peat',
    'B09': 'Geothermal',
    'B10': 'Hydro Pumped Storage',
    'B11': 'Hydro Run-of-river',
    'B12': 'Hydro Reservoir',
    'B13': 'Marine',
    'B14': 'Nuclear',
    'B15': 'Other renewable',
    'B16': 'Solar',
    'B17': 'Waste',
    'B18': 'Wind Offshore',
    'B19': 'Wind Onshore',
    'B20': 'Other'
}

# Dataset configurations for different query types
DATASETS = {
    "actual_load": {
        "documentType": "A65",  # Total Load
        "processType": "A16",   # Realized (actual)
        "domain": "outBiddingZone_Domain",  # Use control area domain
        "unit": "MW"
    },
    "actual_generation": {
        "documentType": "A75",  # Actual generation per type
        "processType": "A16",   # Realized
        "domain": "in_Domain",  # Use control area domain
        "unit": "MW"
    },
    "day_ahead_prices": {
        "documentType": "A44",  # Day-Ahead Price Document
        "processType": "A01",   # Day-ahead process
        # domain will be handled separately for zones (in_Domain & out_Domain)
        "domain": ["in_Domain", "out_Domain"], 
        "unit": "EUR/MWh"
    },
    "installed_capacity": {
        "documentType": "A68",  # Installed generation capacity
        "processType": "A33",   # Year-ahead forecast (for installed capacity)
        "domain": "in_Domain",  # Use control area domain
        "unit": "MW"
    }
}

def format_date(dt: datetime) -> str:
    """Format datetime to ENTSO-E API period string (UTC)."""
    # API expects format YYYYMMDDHHMM (UTC)
    return dt.strftime('%Y%m%d%H%M')

def retrieve_entsoe_data(area_code: str, dataset_key: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Retrieve data from ENTSO-E API for a given area code and dataset."""
    dataset = DATASETS[dataset_key]
    params = {
        'securityToken': API_TOKEN,
        'documentType': dataset['documentType'],
        'periodStart': format_date(start_date),
        'periodEnd': format_date(end_date)
    }
    if dataset.get('processType'):
        params['processType'] = dataset['processType']
    
    # Set domain parameters. For prices, in_Domain and out_Domain must be the same.
    domain_field = dataset['domain']
    if isinstance(domain_field, list):
        # For day_ahead_prices, both in_Domain and out_Domain need to be set to area_code
        params['in_Domain'] = area_code
        params['out_Domain'] = area_code
    else:
        params[domain_field] = area_code

    logging.info(f"Requesting {dataset_key} for area {area_code} from {start_date} to {end_date}.")
    response = requests.get(BASE_URL, params=params)
    if response.status_code != 200:
        logging.error(f"Request failed for {dataset_key}, area {area_code}: {response.status_code}, {response.text}")
        return None

    data_dict = xmltodict.parse(response.text)
    # The root tag can vary by document type (GL_MarketDocument or Publication_MarketDocument)
    market_doc = data_dict.get('GL_MarketDocument') or data_dict.get('Publication_MarketDocument')
    if not market_doc or 'TimeSeries' not in market_doc:
        # No data available for this query
        logging.warning(f"No TimeSeries data for {dataset_key}, area {area_code}.")
        return None

    timeseries_list = market_doc['TimeSeries']
    if not isinstance(timeseries_list, list):
        timeseries_list = [timeseries_list]

    records = []
    for series in timeseries_list:
        # Determine production type or price category
        if dataset_key == "day_ahead_prices":
            # For prices, we can treat each series as a price curve (no explicit production type)
            prod_type = "Day-ahead Price"
        else:
            # For generation, the MktPSRType field gives the fuel type code
            psr_code = series.get('MktPSRType', {}).get('psrType')
            prod_type = PSR_TYPE_MAP.get(psr_code, psr_code or dataset_key.title())
        
        # Explicitly handle units to avoid typos/errors
        if dataset_key == "day_ahead_prices":
            unit = "EUR/MWh"
        else:
            unit = "MW"
        
        # Each TimeSeries contains one or more Periods (typically one period per time interval chunk)
        periods = series['Period'] if isinstance(series['Period'], list) else [series['Period']]
        for period in periods:
            interval_start = datetime.fromisoformat(period['timeInterval']['start'].replace('Z', '+00:00'))
            interval_end = datetime.fromisoformat(period['timeInterval']['end'].replace('Z', '+00:00'))
            resolution = period.get('resolution', 'PT60M')
            # Determine interval step in minutes from the resolution code (PT15M, PT60M, etc.)
            if resolution == 'PT15M':
                step_minutes = 15
            elif resolution == 'PT30M':
                step_minutes = 30
            else:
                step_minutes = 60  # default to hourly if not specified or PT60M

            points = period['Point'] if isinstance(period['Point'], list) else [period['Point']]
            for point in points:
                position = int(point['position'])
                # Calculate timestamp for this point: start_time + (position-1)*step_minutes
                timestamp = interval_start + timedelta(minutes=step_minutes * (position - 1))
                # Get value (quantity for load/gen, price.amount for prices)
                value = None
                if 'quantity' in point:
                    value = float(point['quantity'])
                elif 'price.amount' in point:
                    value = float(point['price.amount'])
                else:
                    # If neither field is present, skip
                    continue

                records.append({
                    'timestamp': timestamp.isoformat(),
                    'area_code': area_code,
                    'dataset': dataset_key,
                    'production_type': prod_type,
                    'value': value,
                    'unit': unit
                })
    # Compile into DataFrame
    df = pd.DataFrame(records)
    # Sort by timestamp just in case (ascending chronological order)
    df.sort_values(by='timestamp', inplace=True)
    df.reset_index(drop=True, inplace=True)
    # If multiple time series contributed the same (timestamp, production_type) (which can happen if data overlaps),
    # drop duplicate entries (keeping the first). This prevents double-counting in case of overlapping intervals.
    df = df.drop_duplicates(subset=['timestamp', 'production_type'], keep='first')
    logging.info(f"Retrieved {len(df)} records for {dataset_key}, area {area_code}.")
    return df

def retrieve_entsoe_datasets(countries: dict, datasets: list, start_date: datetime, end_date: datetime, output_folder: str):
    """Retrieve specified datasets for given countries within [start_date, end_date). Save results to CSV and JSON."""
    for country_name, country_code in countries.items():
        for dataset_key in datasets:
            if dataset_key == "day_ahead_prices":
                # Determine which area codes to use for price (bidding zones)
                if country_name in BIDDING_ZONES:
                    zones = BIDDING_ZONES[country_name]
                else:
                    # Default: use the country_code itself if no special zone mapping
                    zones = [(country_code, country_name)]
                for zone_code, zone_label in zones:
                    logging.info(f"Starting retrieval of {dataset_key} for {country_name} (Zone: {zone_label}).")
                    df = retrieve_entsoe_data(zone_code, dataset_key, start_date, end_date)
                    if df is not None and not df.empty:
                        # Construct filename with country and zone label
                        csv_name = os.path.join(output_folder, f"{country_name}_{zone_label}_{dataset_key}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv")
                        df.to_csv(csv_name, index=False)
                        logging.info(f"Saved CSV: {csv_name}")
                        # Metadata with zone info
                        metadata = {
                            "country": country_name,
                            "bidding_zone": zone_label,
                            "area_code": zone_code,
                            "dataset": dataset_key,
                            "unit": DATASETS[dataset_key]['unit'],
                            "period_start": start_date.isoformat(),
                            "period_end": end_date.isoformat(),
                            "retrieval_timestamp": datetime.now(pytz.UTC).isoformat()
                        }
                        meta_name = csv_name.replace('.csv', '_metadata.json')
                        with open(meta_name, 'w') as meta_file:
                            json.dump(metadata, meta_file, indent=4)
                        logging.info(f"Saved metadata: {meta_name}")
                    else:
                        logging.warning(f"No data available for {country_name} (Zone: {zone_label}), dataset: {dataset_key}.")
            else:
                # Non-price datasets (load, generation, etc.) use country_code directly
                logging.info(f"Starting retrieval of {dataset_key} for {country_name}.")
                df = retrieve_entsoe_data(country_code, dataset_key, start_date, end_date)
                if df is not None and not df.empty:
                    csv_name = os.path.join(output_folder, f"{country_name}_{dataset_key}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv")
                    df.to_csv(csv_name, index=False)
                    logging.info(f"Saved CSV: {csv_name}")
                    metadata = {
                        "country": country_name,
                        "area_code": country_code,
                        "dataset": dataset_key,
                        "unit": DATASETS[dataset_key]['unit'],
                        "period_start": start_date.isoformat(),
                        "period_end": end_date.isoformat(),
                        "retrieval_timestamp": datetime.now(pytz.UTC).isoformat()
                    }
                    meta_name = csv_name.replace('.csv', '_metadata.json')
                    with open(meta_name, 'w') as meta_file:
                        json.dump(metadata, meta_file, indent=4)
                    logging.info(f"Saved metadata: {meta_name}")
                else:
                    logging.warning(f"No data available for {country_name}, dataset: {dataset_key}.")

# Adjusted retrieval function for day-ahead prices (monthly)
def retrieve_monthly_entsoe_datasets(countries, datasets, year, output_folder):
    for month in range(1, 13):
        start_date = datetime(year, month, 1, tzinfo=pytz.UTC)
        if month == 12:
            end_date = datetime(year + 1, 1, 1, tzinfo=pytz.UTC)
        else:
            end_date = datetime(year, month + 1, 1, tzinfo=pytz.UTC)
        
        logging.info(f"=== Retrieving data for {start_date.strftime('%B %Y')} ===")
        retrieve_entsoe_datasets(countries, datasets, start_date, end_date, output_folder)

