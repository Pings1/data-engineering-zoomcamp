from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=0)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web inmto pandas dataframe"""
    df = pd.read_csv(dataset_url, low_memory=False)
    return df

@task()
def write_local(df:pd.DataFrame,datasetfile:str) -> Path:
    """write df as a parquet file"""
    path = Path(f"/Users/ravilsagatdinov/Documents/python_project_vs_code/DE_ZOOMCAMP/week_3/{datasetfile}.csv.gz")
    df.to_csv(path, index=False, compression="gzip")
    return path

@task(log_prints=True, retries=0)
def write_gcp( year:int, month:int) -> None :
    """Uploading parquet file to GCS"""
    gcs_block = GcsBucket.load("de-zoomcamp-gcs-ravil")
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    path = Path(f"/Users/ravilsagatdinov/Documents/python_project_vs_code/DE_ZOOMCAMP/week_3/{dataset_file}.csv.gz")
    file = gcs_block.upload_from_path(from_path = f"{path}",to_path = f"data/fhw/csv/fhv_tripdata_{year}-{month:02}.csv.gz")
    return 

@flow()
def upload_flow(year, month):
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz"
    print(dataset_url)
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    df = fetch(dataset_url)
    write_local(df, dataset_file)
    write_gcp(year, month)



@flow()
def parent_flow(year,months):
    for month in months:
        upload_flow(year, month)

if __name__=="__main__":
    year = 2019
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    parent_flow(year,months)



