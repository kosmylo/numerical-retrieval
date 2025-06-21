# Numerical-Retrieval

A Docker Compose–based pipeline designed to **retrieve, process, and store structured numerical datasets** from authoritative EU and global sources, including ENTSO-E, Eurostat, EU Building Stock Observatory (BSO), and Open-Meteo. This repository is ideal for creating comprehensive numerical datasets suitable for training multimodal AI models focused on the energy domain.

## 🚀 Features

- **Multi-source numerical data retrieval**:
  - **ENTSO-E**: Electricity load, generation, installed capacity, and day-ahead prices via the ENTSO-E API.
  - **Eurostat**: Energy balances, renewable energy statistics, greenhouse gas emissions, energy prices, and demographic/economic indicators.
  - **EU Building Stock Observatory (BSO)**: Detailed EU building data including energy performance, renovation rates, heating systems, and energy poverty metrics.
  - **Open-Meteo**: Historical weather data including temperature, humidity, wind speed, solar irradiance, sunshine duration, and precipitation for major cities in EU countries.

- **Structured metadata annotation**:
  - JSON metadata files automatically generated per CSV dataset, detailing variables, units, geographical coverage, data retrieval parameters, and timestamps.

- **Flexible and configurable pipeline**:
  - Control each data retrieval via environment flags (`RUN_ENTSOE`, `RUN_EUROSTAT`, `RUN_BSO`, `RUN_OPENMETEO`).
  - Customize retrieval date ranges and parameters directly in `.env`.

- **Detailed logging**:
  - Comprehensive logs stored in `logs/numerical_data_app.log`, capturing retrieval progress, successes, and errors.

## 🗂 Repository Structure

```text
numerical_retrieval
├── .env
├── .gitignore
├── Dockerfile
├── README.md
├── docker-compose.yaml
├── logs
│   └── numerical_data_app.log
├── main.py
├── output
│   ├── entsoe
│   ├── eurostat
│   ├── bso
│   └── openmeteo
├── requirements.txt
└── scripts
    ├── entsoe_retrieval.py
    ├── eurostat_retrieval.py
    ├── bso_retrieval.py
    └── openmeteo_retrieval.py
```

- `main.py`: Coordinates retrieval processes based on environment configurations.

- `scripts/`: Modular scripts for each numerical data source.

- `docker-compose.yaml`: Defines container configurations and environment variables.

- `Dockerfile`: Container setup and Python dependencies.

## 🔧 Prerequisites

- Docker & Docker Compose installed locally.
- ENTSO-E API token (register at [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/)).

## ⚙️ Configuration

Define your retrieval settings in a `.env` file at the repository root:

```bash
# API Tokens
ENTSOE_API_TOKEN="YOUR_ENTSOE_API_TOKEN"

# Retrieval Toggles (0: off, 1: on)
RUN_ENTSOE=1
RUN_EUROSTAT=1
RUN_BSO=1
RUN_OPENMETEO=1

# Retrieval Parameters
ENTSOE_START_YEAR=2021
ENTSOE_END_YEAR=2024
EUROSTAT_START_YEAR=2000
OPENMETEO_START_YEAR=2021
OPENMETEO_END_YEAR=2024
```

Create your .env file in the repository root with:

```bash
touch .env
```

Open the file with a text editor to add your values, for example:

```bash
nano .env
```

Enter your preferences and save.

### Data Source Coverage:

- **ENTSO-E**: Electricity load, generation, prices, and installed capacities across all EU countries.
- **Eurostat**: Annual energy balances, renewable energy shares, energy efficiency indicators, greenhouse gas emissions, energy prices, economic, and demographic data.
- **EU Building Stock Observatory (BSO)**: Detailed building stock characteristics, renovation rates, energy performance, and nearly zero-energy building statistics.
- **Open-Meteo**: Daily weather data (temperature, precipitation, solar irradiance, wind speed, humidity) for the three most populated cities in each EU country.

## 📂 Output

Datasets and metadata structured as follows:

```text
output/
├── entsoe/
│   ├── Germany_actual_load_20210101_20210131.csv
│   ├── Germany_actual_load_20210101_20210131_metadata.json
│   └── ...
├── eurostat/
│   ├── renewable_energy_share.csv
│   ├── renewable_energy_share_metadata.json
│   └── ...
├── bso/
│   ├── building_stock_characteristics.csv
│   ├── building_stock_characteristics_metadata.json
│   └── ...
└── openmeteo/
    ├── germany_berlin_20210101_20210131.csv
    ├── germany_berlin_20210101_20210131_metadata.json
    └── ...
```

## 🐳 Build & Run

1. **Build the Docker image** from the repository root:

   ```bash
   docker build -t numerical-retrieval .
   ```

2. **Start the data retrieval pipeline** using Docker Compose:

   ```bash
   docker-compose up
   ```

   All scraper settings can be customized via environment variables in `.env`.

3. **Stop the service** when finished:

   ```bash
   docker-compose down
   ```

All datasets and metadata will be available in the `output/` directory, ready for immediate use in data analysis, modeling, or integration into AI pipelines.

