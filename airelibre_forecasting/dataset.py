from pathlib import Path

import typer
from loguru import logger
from tqdm import tqdm
import pandas as pd

from airelibre_forecasting.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

app = typer.Typer()

airelibre_raw_dtype = {
    "sensor": "string",
    "source": "string",
    "description": "string",
    "version": "string",
    "pm1dot0": "float64",
    "pm2dot5": "float64",
    "pm10": "float64",
    "humidity": "float64",
    "temperature": "float64",
    "pressure": "float64",
    "co2": "float64",
    "longitude": "float64",
    "latitude": "float64",
}

# Adapt to meteostat data
weather_raw_dtype = {
    "date_utc": "datetime64[ns]",
    "temperature": "float64",
    "humidity": "float64",
    "pressure": "float64",
    "wind_speed": "float64",
    "wind_deg": "float64",
    "rain_1h": "float64",
    "rain_3h": "float64",
    "snow_1h": "float64",
    "snow_3h": "float64",
    "clouds_all": "float64",
}

stations_dtype_mapping = {
    "id": "string",
    "name": "string",
    "longitude": "float64",
    "latitude": "float64",
}

airelibre_features_dtype = {
    "station_id": "string",
    "region_id": "int64",
    "date_utc": "datetime64[ns]",
    "pm1": "float64",
    "pm2_5": "float64",
    "pm10": "float64",

}


def clean_data(df):
    """Clean the data."""
    # Convert columns to appropriate dtypes
    df = df.astype(airelibre_raw_dtype)
    
    # Set datetime index and resample to hourly frequency using mean
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    df = df.resample('H').mean()
    
    # Interpolate missing data
    df.interpolate(method='linear', inplace=True)
    
    # Rename columns
    df.rename(columns={
        'pm1dot0': 'pm1',
        'pm2dot5': 'pm2_5',
        'pm10': 'pm10',
    }, inplace=True)
    
    # Generate new columns if needed (example: AQI calculation)
    
    return df

def generate_features(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_data(df)



@app.command()
def main(
    # ---- DEFAULT PATHS ----
    airelibre_input_path: Path = RAW_DATA_DIR / "airelibre_data.csv",
    #weather_input_path: Path = RAW_DATA_DIR / "weather_data.csv",

    features_output_path: Path = PROCESSED_DATA_DIR / "airelibre_dataset.csv",
    stations_output_path: Path = PROCESSED_DATA_DIR / "stations_dataset.csv",

    # ----------------------------------------------
):
    logger.info("Generating features from dataset...")

    # Load the dataset
    df = pd.read_csv(airelibre_input_path)
    
    # Generate features
    df = generate_features(df)
    
    # Save the features to a new CSV file
    df.to_csv(features_output_path, index=False)
    
    logger.success("Features generation complete.")


if __name__ == "__main__":
    app()
