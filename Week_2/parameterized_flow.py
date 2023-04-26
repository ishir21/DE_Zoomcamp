from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta
from prefect.tasks import task_input_hash


@task(retries=1)
def fetch(dataset_url,cache_key_fn=task_input_hash,cache_expiration=timedelta(days=1)):
    df = pd.read_parquet(dataset_url)
    return df
  
  
@task(log_prints=True)
def clean(df):
  df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
  df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
  return df 

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str):
    """Write DataFrame out as parquet file"""
    data_dir = f'data/{color}'
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    path = Path(f'{data_dir}/{dataset_file}.parquet').as_posix()
    df.to_parquet(path, compression='gzip')
    return path
  
@task()
def write_gcs(path):
  gcs_block = GcsBucket.load("zoomcamp-bucket")
  gcs_block.upload_from_path(
    from_path = f"{path}",
    to_path=path
  )

@flow()
def etl_web_to_gcs(color:str, year:int, month:int):
    """The main etl function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    # dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df, color, dataset_file)
    write_gcs(path)
    
@flow()
def etl_parent_flow(months: list[int] = [1,2], year: int=2021, color: str="yellow"):
    for month in months:
        etl_web_to_gcs(color, year, month)


if __name__ == "__main__":
  color="fhv"
  months=[1,2,3,4,5,6,7,8,9,10,11,12]
  year=2022
  etl_parent_flow(months, year, color)
  