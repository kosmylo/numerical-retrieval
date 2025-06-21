import os
import requests
import pandas as pd
import json
import logging
from datetime import datetime, timedelta

# Open-Meteo API endpoint
API_URL = "https://archive-api.open-meteo.com/v1/archive"

# Weather variables
WEATHER_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "windspeed_10m_max",
    "shortwave_radiation_sum",
    "sunshine_duration",
    "relative_humidity_2m_mean"
]

def fetch_weather_data(country, city, lat, lon, start_date, end_date):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ','.join(WEATHER_VARIABLES),
        "timezone": "UTC"
    }
    response = requests.get(API_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"API error {response.status_code} for {city}, {country}: {response.text}")
        raise Exception(f"API error {response.status_code}: {response.text}")

def save_weather_data(country, city, data, start_date, end_date, output_dir):
    df = pd.DataFrame(data["daily"])
    df["date"] = pd.to_datetime(df["time"])
    df.drop(columns=["time"], inplace=True)

    csv_filename = f"{country.lower().replace(' ','_')}_{city.lower().replace(' ','_')}_{start_date}_{end_date}.csv"
    csv_path = os.path.join(output_dir, csv_filename)
    df.to_csv(csv_path, index=False)

    metadata = {
        "country": country,
        "city": city,
        "coordinates": {
            "latitude": data["latitude"],
            "longitude": data["longitude"]
        },
        "data_source": "Open-Meteo Archive API",
        "variables": {
            "temperature_2m_max": {"unit": "°C", "description": "Daily maximum air temperature at 2 meters"},
            "temperature_2m_min": {"unit": "°C", "description": "Daily minimum air temperature at 2 meters"},
            "precipitation_sum": {"unit": "mm", "description": "Total daily precipitation"},
            "windspeed_10m_max": {"unit": "km/h", "description": "Maximum daily wind speed at 10 meters"},
            "shortwave_radiation_sum": {"unit": "MJ/m²", "description": "Total daily solar radiation"},
            "sunshine_duration": {"unit": "seconds", "description": "Total daily sunshine duration"},
            "relative_humidity_2m_mean": {"unit": "%", "description": "Mean daily relative humidity at 2 meters"}
        },
        "daily_value_description": "Aggregated daily values (00:00–23:59 UTC)",
        "timezone": "UTC",
        "date_format": "YYYY-MM-DD",
        "start_date": start_date,
        "end_date": end_date,
        "retrieved_timestamp": datetime.utcnow().isoformat() + "Z",
        "csv_file": csv_filename,
        "api_url": API_URL
    }

    metadata_filename = csv_filename.replace('.csv', '_metadata.json')
    metadata_path = os.path.join(output_dir, metadata_filename)

    with open(metadata_path, 'w', encoding='utf-8') as mf:
        json.dump(metadata, mf, ensure_ascii=False, indent=4)

    logging.info(f"Saved data and metadata for {city}, {country} from {start_date} to {end_date}")

def retrieve_yearly_weather(countries_coords, start_year, end_year, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    for year in range(start_year, end_year + 1):
        logging.info(f"Retrieving weather data for year: {year}")

        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)

        for country, cities in countries_coords.items():
            for city, (lat, lon) in cities.items():
                logging.info(f"Retrieving data for {city}, {country}, Year: {year}")
                
                current_start = start_date
                while current_start <= end_date:
                    current_end = min(current_start + timedelta(days=30), end_date)
                    s_date = current_start.strftime('%Y-%m-%d')
                    e_date = current_end.strftime('%Y-%m-%d')

                    try:
                        data = fetch_weather_data(country, city, lat, lon, s_date, e_date)
                        save_weather_data(country, city, data, s_date, e_date, output_dir)
                    except Exception as e:
                        logging.error(f"Failed for {city}, {country} ({s_date} to {e_date}): {e}")

                    current_start = current_end + timedelta(days=1)