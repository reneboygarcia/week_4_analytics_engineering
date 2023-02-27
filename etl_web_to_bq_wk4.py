# imports
import typing
import urllib.request
import pandas as pd
from prefect import task, flow
from prefect_gcp import GcpCredentials
from google.cloud import bigquery

print("Setup Complete")

# Get data from Github url
@task(
    log_prints=True,
    name="get-data-from-web",
    retries=3,
)
def get_data_from_web(dataset_url: str):
    filename, _ = urllib.request.urlretrieve(dataset_url)
    print(f"Downloading from: {dataset_url}")
    return filename


# Read and tweak to fix the dtypes of pick-up and drop-off
@task(log_prints=True, name="read-tweak-df")
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
@task(log_prints=True, name="Upload Data frame to BigQuery")
def write_bq(df: pd.DataFrame, year: int, month: int, color: str):
    gcp_credentials_block = GcpCredentials.load("ny-taxi-gcp-creds")
    df.to_gbq(
        destination_table=f"ny_taxi.{color}_tripdata_{year}",
        project_id="dtc-de-2023",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
        progress_bar=True,
    )
    print(f"Successfully uploaded: {color}_tripdata_{year}-{month:02} to BigQuery")
    return


@task(log_prints=True, name="get-gcp-creds")
def get_bigquery_client():
    gcp_creds_block = GcpCredentials.load("prefect-gcs-2023-creds")
    gcp_creds = gcp_creds_block.get_credentials_from_service_account()
    client = bigquery.Client(credentials=gcp_creds)
    return client


@task(log_prints=True, name="Removing Duplicates")
def deduplicate_data(color: str, year: int):

    client = get_bigquery_client()
    # this will remove the duplicates
    query_dedup = f"CREATE OR REPLACE TABLE \
                        `dtc-de-2023.ny_taxi.{color}_tripdata_{year}`  AS ( \
                            SELECT DISTINCT * \
                            FROM `dtc-de-2023.ny_taxi.{color}_tripdata_{year}_{year}` \
                            )"

    # limit query to 10GB
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
    # priority=bigquery.QueryPriority.BATCH
    # query
    query_job = client.query(query_dedup, job_config=safe_config)

    # Check progress
    query_job = typing.cast(
        "bigquery.QueryJob",
        client.get_job(
            query_job.job_id, location=query_job.location
        ),  # Make an API request.
    )
    print("Completed removing duplicates")
    print(f"Job {query_job.job_id} is currently in state {query_job.state}")


# Define ETL
@flow(log_prints=True, name=f"etl-web-to-bq")
def etl_web_to_bq(year: int, month: int, color: str):
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02}.csv.gz"

    # Execution
    # Extract data from web
    data_file = get_data_from_web(dataset_url)
    # Read and tweak data frame
    df = read_tweak_df(data_file, color=color)
    # Write to BQ
    write_bq(df, year, month, color)
    # Removing Duplicates
    deduplicate_data(color, year)
    return


# Parent ETL
@flow(log_prints=True, name="parent-etl-web-to-bq")
def parent_etl_web_to_bq(
    year: int,
    months: list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    colors: list = ["green", "yellow"],
):
    for color in colors:
        for month in months:
            etl_web_to_bq(year, month, color)


# Run Main
if __name__ == "__main__":
    year = 2020
    months = [4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2, 3]
    colors = ["green", "yellow"]
    parent_etl_web_to_bq()
