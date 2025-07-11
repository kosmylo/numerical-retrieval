import os
import logging
from pathlib import Path
from scripts.bso_retrieval import download_and_process_bso_data
from scripts.entsoe_retrieval import retrieve_monthly_entsoe_datasets
from scripts.eurostat_retrieval import retrieve_eurostat_datasets
from scripts.openmeteo_retrieval import retrieve_yearly_weather
from scripts.entsoe_preprocessing import merge_monthly_to_yearly
from scripts.openmeteo_preprocessing import merge_monthly_weather_to_yearly

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler("logs/numerical_data_app.log"),
            logging.StreamHandler()
        ]
    )

def main():
    configure_logging()
    logging.info("=== Starting Energy Data Retrieval ===")

    # Environment-driven toggles
    RUN_BSO = os.getenv("RUN_BSO", "1") == "1"
    RUN_ENTSOE = os.getenv("RUN_ENTSOE", "1") == "1"
    RUN_EUROSTAT = os.getenv("RUN_EUROSTAT", "1") == "1"
    RUN_OPENMETEO = os.getenv("RUN_OPENMETEO", "1") == "1"
    RUN_ENTSOE_PREPROCESSING = os.getenv("RUN_ENTSOE_PREPROCESSING", "1") == "1"
    RUN_OPENMETEO_PREPROCESSING = os.getenv("RUN_OPENMETEO_PREPROCESSING", "1") == "1"

    logging.info(f"""
        RUN_BSO: {RUN_BSO},
        RUN_ENTSOE: {RUN_ENTSOE},
        RUN_EUROSTAT: {RUN_EUROSTAT},
        RUN_OPENMETEO: {RUN_OPENMETEO},
        RUN_ENTSOE_PREPROCESSING: {RUN_ENTSOE_PREPROCESSING},
        RUN_OPENMETEO_PREPROCESSING: {RUN_OPENMETEO_PREPROCESSING}
    """)

    # Directories setup
    Path("output/bso").mkdir(parents=True, exist_ok=True)
    Path("output/entsoe").mkdir(parents=True, exist_ok=True)
    Path("output/eurostat").mkdir(parents=True, exist_ok=True)
    Path("output/openmeteo").mkdir(parents=True, exist_ok=True)
    Path("output/entsoe/yearly").mkdir(parents=True, exist_ok=True) 
    Path("output/openmeteo/yearly").mkdir(parents=True, exist_ok=True)

    # --- BSO Data Retrieval ---
    if RUN_BSO:
        try:
            bso_output_dir = "output/bso"
            download_and_process_bso_data(output_dir=bso_output_dir)
            logging.info("BSO data retrieved successfully.")
        except Exception as e:
            logging.error(f"BSO retrieval failed: {e}")

    # --- ENTSO-E Data Retrieval ---
    if RUN_ENTSOE:
        try:           
            # Mapping of EU countries to ENTSO-E area (control area or country) EIC codes for general data
            EU_COUNTRIES = {
                "Austria": "10YAT-APG------L",
                "Belgium": "10YBE----------2",
                "Bulgaria": "10YCA-BULGARIA-R",
                "Croatia": "10YHR-HEP------M",
                "Cyprus": "10YCY-1001A0003J",
                "Czech Republic": "10YCZ-CEPS-----N",
                "Denmark": "10Y1001A1001A65H",      # Member State code (DK) – note: will use DK1/DK2 for prices
                "Estonia": "10Y1001A1001A39I",
                "Finland": "10YFI-1--------U",
                "France": "10YFR-RTE------C",
                "Germany": "10Y1001A1001A83F",      # Member State code (DE) – will use DE-LU for prices
                "Greece": "10YGR-HTSO-----Y",
                "Hungary": "10YHU-MAVIR----U",
                "Ireland": "10YIE-1001A00010",
                "Italy": "10YIT-GRTN-----B",       # Italy control area (national) code
                "Latvia": "10YLV-1001A00074",
                "Lithuania": "10YLT-1001A0008Q",
                "Luxembourg": "10YLU-CEGEDEL-NQ",
                "Malta": "10Y1001A1001A93C",
                "Netherlands": "10YNL----------L",
                "Poland": "10YPL-AREA-----S",
                "Portugal": "10YPT-REN------W",
                "Romania": "10YRO-TEL------P",
                "Slovakia": "10YSK-SEPS-----K",
                "Slovenia": "10YSI-ELES-----O",
                "Spain": "10YES-REE------0",
                "Sweden": "10YSE-1--------K"       # Sweden Member State code – will use SE1..SE4 for prices
            }

            entsoe_output_dir = "output/entsoe"
            start_year_entsoe = int(os.getenv("ENTSOE_START_YEAR", 2021))
            end_year_entsoe = int(os.getenv("ENTSOE_END_YEAR", 2024))

            selected_datasets = [
                "actual_load", 
                "actual_generation", 
                "day_ahead_prices", 
                "installed_capacity"
            ]

            for year in range(start_year_entsoe, end_year_entsoe + 1):
                logging.info(f"--- ENTSO-E retrieval for year: {year} ---")
                retrieve_monthly_entsoe_datasets(
                    countries=EU_COUNTRIES,
                    datasets=selected_datasets,
                    year=year,
                    output_folder=entsoe_output_dir
                )

            logging.info("ENTSO-E data retrieved successfully.")
        except Exception as e:
            logging.error(f"ENTSO-E retrieval failed: {e}")

    # --- Eurostat Data Retrieval ---
    if RUN_EUROSTAT:
        try:
            # List of EU countries (ISO2 format)
            EU_COUNTRIES_ISO2 = [
                'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'EL',
                'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK',
                'SI', 'ES', 'SE'
            ]
            eurostat_start_year = int(os.getenv("EUROSTAT_START_YEAR", 2000))            
            eurostat_output_dir = "output/eurostat"
            
            retrieve_eurostat_datasets(
                output_dir=eurostat_output_dir,
                start_year=eurostat_start_year,
                countries=EU_COUNTRIES_ISO2
            )
            logging.info("Eurostat data retrieved successfully.")
        except Exception as e:
            logging.error(f"Eurostat retrieval failed: {e}")

    # --- Open-Meteo Data Retrieval ---
    if RUN_OPENMETEO:
        try:
            openmeteo_output_dir = "output/openmeteo"
            openmeteo_start_year = int(os.getenv("OPENMETEO_START_YEAR", 2021))
            openmeteo_end_year = int(os.getenv("OPENMETEO_END_YEAR", 2024))

            # Three most populated cities per EU country with coordinates
            EU_COUNTRIES_COORDS = {
                "Austria": {
                    "Vienna": (48.2100, 16.3634),
                    "Graz": (47.0767, 15.4214),
                    "Linz": (48.3064, 14.2861)
                },
                "Belgium": {
                    "Brussels": (50.8505, 4.3488),
                    "Antwerp": (51.2205, 4.4003),
                    "Ghent": (51.0500, 3.7167)
                },
                "Bulgaria": {
                    "Sofia": (42.6975, 23.3242),
                    "Plovdiv": (42.1500, 24.7500),
                    "Varna": (43.2167, 27.9167)
                },
                "Croatia": {
                    "Zagreb": (45.8144, 15.9780),
                    "Split": (43.5089, 16.4392),
                    "Rijeka": (45.3267, 14.4424)
                },
                "Cyprus": {
                    "Nicosia": (35.1753, 33.3642),
                    "Limassol": (34.6841, 33.0379),
                    "Larnaca": (34.9221, 33.6279)
                },
                "Czech Republic": {
                    "Prague": (50.0880, 14.4208),
                    "Brno": (49.1952, 16.6080),
                    "Ostrava": (49.8347, 18.2820)
                },
                "Denmark": {
                    "Copenhagen": (55.6759, 12.5655),
                    "Aarhus": (56.1567, 10.2108),
                    "Odense": (55.3959, 10.3883)
                },
                "Estonia": {
                    "Tallinn": (59.4369, 24.7535),
                    "Tartu": (58.3806, 26.7251),
                    "Narva": (59.3772, 28.1903)
                },
                "Finland": {
                    "Helsinki": (60.1695, 24.9355),
                    "Espoo": (60.2052, 24.6522),
                    "Tampere": (61.4991, 23.7871)
                },
                "France": {
                    "Paris": (48.8534, 2.3488),
                    "Marseille": (43.2969, 5.3811),
                    "Lyon": (45.7485, 4.8467)
                },
                "Germany": {
                    "Berlin": (52.5244, 13.4105),
                    "Hamburg": (53.5507, 9.9930),
                    "Munich": (48.1374, 11.5755)
                },
                "Greece": {
                    "Athens": (37.9838, 23.7278),
                    "Thessaloniki": (40.6436, 22.9309),
                    "Patras": (38.2444, 21.7344)
                },
                "Hungary": {
                    "Budapest": (47.4983, 19.0405),
                    "Debrecen": (47.5317, 21.6244),
                    "Szeged": (46.2530, 20.1482)
                },
                "Ireland": {
                    "Dublin": (53.3331, -6.2489),
                    "Cork": (51.8980, -8.4706),
                    "Limerick": (52.6647, -8.6231)
                },
                "Italy": {
                    "Rome": (41.8919, 12.5113),
                    "Milan": (45.4643, 9.1895),
                    "Naples": (40.8522, 14.2681)
                },
                "Latvia": {
                    "Riga": (56.9489, 24.1064),
                    "Daugavpils": (55.8750, 26.5356),
                    "Liepaja": (56.5117, 21.0136)
                },
                "Lithuania": {
                    "Vilnius": (54.6892, 25.2798),
                    "Kaunas": (54.8972, 23.8861),
                    "Klaipeda": (55.7125, 21.1350)
                },
                "Luxembourg": {
                    "Luxembourg City": (49.6116, 6.1319),
                    "Esch-sur-Alzette": (49.4969, 5.9806),
                    "Differdange": (49.5242, 5.8914)
                },
                "Malta": {
                    "Birkirkara": (35.8972, 14.4611),
                    "Mosta": (35.9097, 14.4261),
                    "Qormi": (35.8794, 14.4722)
                },
                "Netherlands": {
                    "Amsterdam": (52.3728, 4.8936),
                    "Rotterdam": (51.9225, 4.4792),
                    "The Hague": (52.0767, 4.2986)
                },
                "Poland": {
                    "Warsaw": (52.2297, 21.0122),
                    "Krakow": (50.0647, 19.9450),
                    "Lodz": (51.7592, 19.4560)
                },
                "Portugal": {
                    "Lisbon": (38.7223, -9.1393),
                    "Porto": (41.1579, -8.6291),
                    "Vila Nova de Gaia": (41.1245, -8.6140)
                },
                "Romania": {
                    "Bucharest": (44.4268, 26.1025),
                    "Cluj-Napoca": (46.7712, 23.6236),
                    "Timisoara": (45.7489, 21.2087)
                },
                "Slovakia": {
                    "Bratislava": (48.1439, 17.1097),
                    "Kosice": (48.7164, 21.2611),
                    "Presov": (48.9985, 21.2339)
                },
                "Slovenia": {
                    "Ljubljana": (46.0569, 14.5058),
                    "Maribor": (46.5547, 15.6459),
                    "Celje": (46.2389, 15.2673)
                },
                "Spain": {
                    "Madrid": (40.4168, -3.7038),
                    "Barcelona": (41.3851, 2.1734),
                    "Valencia": (39.4699, -0.3763)
                },
                "Sweden": {
                    "Stockholm": (59.3293, 18.0686),
                    "Gothenburg": (57.7089, 11.9746),
                    "Malmo": (55.6049, 13.0038)
                }
            }

            retrieve_yearly_weather(
                countries_coords=EU_COUNTRIES_COORDS,
                start_year=openmeteo_start_year,
                end_year=openmeteo_end_year,
                output_dir=openmeteo_output_dir
            )

            logging.info("Open-Meteo weather data retrieved successfully.")
        except Exception as e:
            logging.error(f"Open-Meteo retrieval failed: {e}")
    
    # --- ENTSO-E Data Preprocessing (Yearly) ---
    if RUN_ENTSOE_PREPROCESSING:
        try:
            logging.info("=== Starting ENTSO-E Data Preprocessing ===")
            entsoe_monthly_output_dir = "output/entsoe"
            entsoe_yearly_output_dir = "output/entsoe/yearly"
            merge_monthly_to_yearly(
                input_folder=entsoe_monthly_output_dir,
                output_folder=entsoe_yearly_output_dir
            )
            logging.info("ENTSO-E data preprocessing successfully completed.")
        except Exception as e:
            logging.error(f"ENTSO-E preprocessing failed: {e}")

    # --- Open-Meteo Data Preprocessing (Yearly) ---
    if RUN_OPENMETEO_PREPROCESSING:
        try:
            logging.info("=== Starting Open-Meteo Data Preprocessing ===")
            openmeteo_monthly_output_dir = "output/openmeteo"
            openmeteo_yearly_output_dir = "output/openmeteo/yearly"
            merge_monthly_weather_to_yearly(
                input_folder= openmeteo_monthly_output_dir,
                output_folder=openmeteo_yearly_output_dir
            )
            logging.info("Open-Meteo yearly data preprocessing successfully completed.")
        except Exception as e:
            logging.error(f"Open-Meteo retrieval failed: {e}")

    logging.info("=== Energy Data Retrieval Completed ===")

if __name__ == "__main__":
    main()