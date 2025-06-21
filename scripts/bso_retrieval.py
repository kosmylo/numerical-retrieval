import os
import requests
import pandas as pd
import json
import logging
from datetime import datetime

BSO_URL = "https://energy.ec.europa.eu/document/download/f09b2e17-00e4-46e0-88ae-970fc15a716f_en?filename=data0.xlsx"
BSO_FILENAME = "bso.xlsx"

def download_bso_excel(output_dir: str) -> str:
    file_path = os.path.join(output_dir, BSO_FILENAME)
    if not os.path.exists(file_path):
        logging.info("Downloading BSO Excel file...")
        response = requests.get(BSO_URL)
        if response.status_code == 200:
            with open(file_path, "wb") as f:
                f.write(response.content)
            logging.info(f"File downloaded and saved as {file_path}")
        else:
            logging.error(f"Failed to download file: HTTP {response.status_code}")
            raise Exception(f"Failed to download file: HTTP {response.status_code}")
    else:
        logging.info(f"Using cached file at {file_path}")
    return file_path

def process_bso_excel(file_path: str, output_dir: str):
    logging.info("Processing BSO Excel file...")
    xl = pd.ExcelFile(file_path)

    sheet_name = 'Export'
    logging.info(f"Processing sheet: {sheet_name}")
    
    df = xl.parse(sheet_name)
    df.dropna(how='all', inplace=True)

    if 'Domain' not in df.columns:
        logging.error("Column 'Domain' not found in sheet. Aborting.")
        raise ValueError("Column 'Domain' not found in Excel sheet.")

    for domain in df['Domain'].dropna().unique():
        domain_df = df[df['Domain'] == domain]
        
        domain_clean = domain.lower().replace(' ', '_').replace('/', '_').replace('\\', '_')

        csv_filename = f"{domain_clean}.csv"
        csv_path = os.path.join(output_dir, csv_filename)
        domain_df.to_csv(csv_path, index=False)

        metadata = {
            "original_excel_file": BSO_FILENAME,
            "sheet_name": sheet_name,
            "csv_file": csv_filename,
            "num_records": len(domain_df),
            "columns": domain_df.columns.tolist(),
            "domain": domain,
            "retrieved_timestamp": datetime.now().isoformat(),
            "source_url": BSO_URL,
            "data_source": "EU Building Stock Observatory (BSO)",
            "description": f"Extracted data for the domain '{domain}' from the EU BSO Excel workbook.",
            "geographic_coverage": "EU countries",
            "update_frequency": "Regularly updated by EU DG Energy"
        }

        metadata_filename = csv_filename.replace('.csv', '_metadata.json')
        metadata_path = os.path.join(output_dir, metadata_filename)

        with open(metadata_path, 'w') as mf:
            json.dump(metadata, mf, indent=4)

        logging.info(f"Saved {csv_filename} and corresponding metadata.")

def download_and_process_bso_data(output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    try:
        excel_path = download_bso_excel(output_dir)
        process_bso_excel(excel_path, output_dir)
        logging.info("All BSO sheets processed successfully.")
    except Exception as e:
        logging.error(f"BSO data retrieval failed: {e}")
        raise