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
def read_tweak_df(src: str) -> pd.DataFrame:
    cols_dict = {
        "pickup_datetime": "pickup_datetime",
        "dropOff_datetime": "dropoff_datetime",
    }

    dtype_cols = {
        "dispatching_base_num": "string",
        "PUlocationID": "float64",
        "DOlocationID": "float64",
        "SR_Flag": "float64",
        "Affiliated_base_number": "string",
    }

    df = pd.read_csv(
        src, parse_dates=[1, 2], dtype=dtype_cols, compression="gzip", engine="python"
    ).rename(columns=cols_dict)
    print(f"Data frame number of rows: {df.shape[0]}")
    return df


# Write DataFrame to BigQuery
@task(log_prints=True, name="Upload Data frame to BigQuery")
def write_bq(df: pd.DataFrame, year: int, month: int):
    gcp_credentials_block = GcpCredentials.load("ny-taxi-gcp-creds")
    # schema = {
    #     "dispatching_base_num": "STRING",
    #     "pickup_datetime": "TIMESTAMP",
    #     "dropoff_datetime": "TIMESTAMP",
    #     "PUlocationID": "FLOAT",
    #     "DOlocationID": "FLOAT",
    #     "SR_Flag": "FLOAT",
    #     "Affiliated_base_number": "STRING",
    # }
    df.to_gbq(
        destination_table=f"ny_taxi.fhv_tripdata_2019_2020",
        project_id="dtc-de-2023",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
        progress_bar=True,
    )
    print(f"Successfully uploaded: fhv_tripdata_{year}-{month:02} to BigQuery")
    return


@task(log_prints=True, name="get-gcp-creds")
def get_bigquery_client():
    gcp_creds_block = GcpCredentials.load("ny-taxi-gcp-creds")
    gcp_creds = gcp_creds_block.get_credentials_from_service_account()
    client = bigquery.Client(credentials=gcp_creds)
    return client


@flow(log_prints=True, name="Removing Duplicates")
def deduplicate_data(year: int):

    client = get_bigquery_client()
    # this will remove the duplicates
    query_dedup = f"CREATE OR REPLACE TABLE \
                        `dtc-de-2023.ny_taxi.fhv_tripdata_2019_2020`  AS ( \
                            SELECT DISTINCT * \
                            FROM `dtc-de-2023.ny_taxi.fhv_tripdata_2019_2020` \
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
def etl_web_to_bq(year: int, month: int):
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz"

    # Execution
    # Extract data from web
    data_file = get_data_from_web(dataset_url)
    # Read and tweak data frame
    df = read_tweak_df(data_file)
    # Write to BQ
    write_bq(df, year, month)
    # Removing Duplicates
    deduplicate_data(year)
    return


# Parent ETL
@flow(log_prints=True, name="parent-etl-web-to-bq")
def parent_etl_web_to_bq(
    years: list = [2019, 2020], months: list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
):
    for year in years:
        for month in months:
            etl_web_to_bq(year, month)


# Run Main
if __name__ == "__main__":
    years = [2019, 2020]
    months = [4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2, 3]

    parent_etl_web_to_bq(years, months)
