# imports
import os
from datetime import datetime
from pathlib import Path
import urllib.request
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket

print("Setup Complete")

# Deployment 1
# Fetch the data from Github url
@task(log_prints=True, name="fetch-file-dataset_url", retries=3)
def fetch(dataset_url: str):
    filename, _ = urllib.request.urlretrieve(dataset_url)
    return filename


# Read and tweak to fix the dtypes of pick-up and drop-off
@task(log_prints=True, name="read-and-tweak-df")
def read_tweak_df(src: str) -> pd.DataFrame:
    dtype_cols = {
        "dispatching_base_num": "string",
        "PUlocationID": "float64",
        "DOlocationID": "float64",
        "SR_Flag": "float64",
        "Affiliated_base_number": "string",
    }
    df = pd.read_csv(
        src,
        parse_dates=[1, 2],
        dtype=dtype_cols,
        compression="gzip",
        encoding="ISO-8859-1",
    )
    return df


# Write DataFrame to a specific folder after tweaking the DataFrame
@task(log_prints=True, name="write-to-local-file")
def write_local(df: pd.DataFrame, year: int, dataset_file: str) -> Path:
    directory = Path(f"{year}")
    path_name = directory / f"{dataset_file}.parquet"
    try:
        os.makedirs(directory, exist_ok=True)
        df.to_parquet(path_name, compression="gzip", index=False)
    except OSError as error:
        print(error)
    return path_name


# https://app.prefect.cloud/account/975bd9ed-5aef-4c8a-a413-23073fef3acb/workspace/a711abba-f5ae-4b00-9315-34f92f089b77/blocks/catalog
# Upload local csv.gz file to GCS
@task(log_prints=True, name="write-gcs", retries=3)
def write_gcs(path: Path) -> None:
    gcs_block = GcsBucket.load("prefect-gcs-block-ny-taxi")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    print("Loaded data to GCS...Hooray!")
    return


# Delete file after uploads
@task(log_prints=True, name="deduplicate-local-data")
def deduplicate(path: Path) -> None:
    try:
        os.remove(path)
        os.rmdir(path)
        print("Successfully deleted directory and local files...hep hep hooray")
    except OSError as error:
        print(f"The system cannot find the file specified: {error}")
    return


@flow(log_prints=True, name="etl-web-gcs")
def etl_web_gcs(year: int, month: int):
    # Parameters
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    # Execution
    # Fetch the data from Github url
    source = fetch(dataset_url)

    # no choice but to tweak the data types for us to upload this to BQ
    # Read and tweak to fix the dtypes of pick-up and drop-off
    df_tweak = read_tweak_df(source)

    # write df to local
    path_local = write_local(df_tweak, year, dataset_file)

    # Upload dataset from local to gcs
    write_gcs(path_local)

    # Delete file after uploads
    deduplicate(path_local)

    # Next step is to create a separate deployment from GCS to BigQuery


# Create Parent flow to loop
@flow(name="etl-parent-web-gcs")
def etl_parent_web_gcs(years: list, months: list):
    for year in years:
        for month in months:
            etl_web_gcs(year, month)


# Run main
if __name__ == "__main__":
    years = [2019, 2020]
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    etl_parent_web_gcs(years, months)
