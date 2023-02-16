from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True, retries=0)
def fetch(color:str,year:int, month:int) -> Path:
    """Read data from web inmto pandas dataframe"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}" 
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = pd.read_csv(dataset_url, low_memory=False)
    print(f"length of {dataset_file}:", len(df))
    path = Path(f"/Users/ravilsagatdinov/Documents/python_project_vs_code/DE_ZOOMCAMP/week_2/{dataset_file}.parquet")
    df.to_parquet(path,compression="gzip")
    return path

    

@task(log_prints=True, retries=0)
def write_gcp(color:str, year:int, month:int) -> None :
    """Uploading parquet file to GCS"""
    gcs_block = GcsBucket.load("de-zoomcamp-gcs-ravil")
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    path = Path(f"/Users/ravilsagatdinov/Documents/python_project_vs_code/DE_ZOOMCAMP/week_2/{dataset_file}.parquet")
    file = gcs_block.upload_from_path(from_path = f"{path}",to_path = f"data/yellow/{color}_tripdata_{year}-{month:02}.parquet")
    return 


@task(log_prints=True, retries=3)
def extract_from_gcs(color:str ,year:int ,month:int) -> Path:
    """Download tripdata from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("de-zoomcamp-gcs-ravil")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task(log_prints=True, retries=0)
def write_bq(df: pd.DataFrame) -> None:
    """Writing to BQ"""
    gcp_credentials_block = GcpCredentials.load("de-zoomcamp-ravil-creds")
    df.to_gbq(
        destination_table="de-zoomcamp-376116.de_zoomcamp_dataset.week_2_hw",
        project_id="de-zoomcamp-376116",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append"
    )


@flow
def main_etl(color, year, month):
    fetch(color,year, month)
    write_gcp(color,year, month)
    path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    write_bq(df)



@flow
def parent_flow(color, year, months):
    for month in months:
        main_etl(color, year, month)

if __name__=="__main__":
    color = "yellow"
    year = 2019
    months = [2,3]
    parent_flow(color, year,months)
    
#for running deployment: prefect deployment build week2_hw.py:parent_flow -n "week2_hw_deployment"
#prefect agent start --work-queue "default" 
#prefect deployment apply parent_flow-deployment.yamlpr