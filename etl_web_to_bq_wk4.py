# imports
import os
from datetime import datetime
from pathlib import Path
import urllib.request
import pandas as pd
from prefect import task, flow
from prefect_gcp import GcpCredentials

print("Setup Complete")

# Get data from Github url
def get_data_from_web(dataset_url: str):
    filename, _ = urllib.request.urlretrieve(dataset_url)
    return filename


# Read and tweak to fix the dtypes of pick-up and drop-off
def read_tweak_df(src: str, color: str) -> pd.DataFrame:
    dict_types = {"store_and_fwd_flag": str}
    cols_dict = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
    }

    df = (
        pd.read_csv(src, parse_dates=[1, 2], dtype=dict_types, compression="gzip")
        .assign(category=color)
        .rename(columns=cols_dict)
        .fillna(value={"passenger_count": 0})
    )
    print(f"Data frame number of rows: {df.shape[0]}")
    return df


# Write DataFrame to BigQuery
def write_bq(df: pd.DataFrame, year: int) -> None:
    gcp_credentials_block = GcpCredentials.load("ny-taxi-gcp-creds")
    df.to_gbq(
        destination_table=f"ny_taxi.yellow_tripdata_{year}",
        project_id="dtc-de-2023",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )
    return


# Define ETL
def etl_web_to_bq(year: int, month: int, color: str):
    color = "yellow"
    year = 2019
    month = 1
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02}.csv.gz"

    # Execution
    # Extract data from web
    data_file = get_data_from_web(dataset_url)
    # Read and tweak data frame
    df = read_tweak_df(data_file, color=color)
    # Write to BQ
    write_bq(df)
    return


# Parent ETL
def parent_etl_web_to_bq(
    year: int,
    months: list(int) = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    colors: list(str) = ["green", "yellow"],
):
    year = 2019
    for color in colors:
        for month in months:
            etl_web_to_bq(year, month, color)


# Run Main
if __name__ == "__main__":
    parent_etl_web_to_bq()
