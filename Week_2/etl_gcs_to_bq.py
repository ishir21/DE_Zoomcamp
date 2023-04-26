from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color,year,month):
  """Download tripd data from GCS"""
  gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
  gcs_block = GcsBucket.load("zoomcamp-bucket")
  # gcs_block.download_object_to_path(file_path="data\", output_file=gcs_path)
  gcs_block.get_directory(from_path=gcs_path, local_path='./')
  return Path(gcs_path)

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-credentials")
    df.to_gbq(
        destination_table='trips_data_all.trips_green',
        project_id='zoomcamp-384014',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'
    )



@flow()
def etl_gcs_to_bq(color:str,year:int,month:int):
  """ Main ETL flow to load data into Big Query"""
  # color = "yellow"
  # year = 2021
  # month = 1
  path = extract_from_gcs(color, year, month)
  df = transform(path)
  write_bq(df)

@flow()
def etl_gcs_to_bq_parent(color:str,year:int,months:list):
  for month in months:
    etl_gcs_to_bq(color,year,month)
  
  
  
  
if __name__ == "__main__":
  color="green"
  year=2022
  months=[1,2,3,4,5,6,7,8,9,10,11,12]
  etl_gcs_to_bq_parent(color,year,months)