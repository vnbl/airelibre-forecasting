import os
import requests
import pandas as pd
import datetime
import logging
import argparse
from time import sleep

# Configure logging
logging.basicConfig(
    filename="fetch_raw_data.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Define the raw data folder (absolute path)
RAW_DATA_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/raw"))
os.makedirs(RAW_DATA_FOLDER, exist_ok=True)

# Define the output CSV file path
CSV_FILE_PATH = os.path.join(RAW_DATA_FOLDER, "airelibre_data_test.csv")


def fetch_data_for_day(start_date, end_date):
    """Fetch data from the API for a given day."""
    url = f"https://rald-dev.greenbeep.com/api/v1/measurements?start={start_date}&end={end_date}"
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data for {start_date} - {end_date}: {e}")
        return None


def main(start_date, end_date):
    """Fetch data for the specified range and save it to CSV."""
    logging.info(f"Starting raw data fetching process from {start_date} to {end_date}...")
    current_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end_date_obj - current_date).days
    processed_days = 0

    while current_date < end_date_obj:
        start_date_str = current_date.strftime("%Y-%m-%dT00:00")
        next_date = current_date + datetime.timedelta(days=1)
        end_date_str = next_date.strftime("%Y-%m-%dT00:00")

        logging.info(f"Fetching data for {start_date_str} to {end_date_str}...")
        data = fetch_data_for_day(start_date_str, end_date_str)

        if data:
            try:
                df = pd.DataFrame(data)

                # Append to CSV file
                file_exists = os.path.isfile(CSV_FILE_PATH)
                df.to_csv(CSV_FILE_PATH, mode='a', header=not file_exists, index=False)
                logging.info(f"Data for {start_date_str} saved successfully.")
            except Exception as e:
                logging.error(f"Error saving data for {start_date_str}: {e}")
        else:
            logging.warning(f"No data retrieved for {start_date_str}.")

        # Progress tracking
        processed_days += 1
        logging.info(f"Progress: {processed_days}/{total_days} days processed.")

        # Sleep to avoid overloading the server
        sleep(1)

        current_date = next_date

    logging.info(f"Raw data fetching process completed. Data saved to {CSV_FILE_PATH}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch raw data and save to CSV.")
    parser.add_argument(
        "--start_date", type=str, required=True, help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--end_date", type=str, required=True, help="End date in YYYY-MM-DD format"
    )
    args = parser.parse_args()

    main(args.start_date, args.end_date)
