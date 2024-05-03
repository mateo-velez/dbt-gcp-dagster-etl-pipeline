from dagster import Definitions, load_assets_from_modules
from .config.constants import RAW_BUCKET_NAME,STAGING_BUCKET_NAME,DBT_DATASET_ID,PROJECT_ID
from .config.dataproc_config import dataproc_create_cluster_config as cluster_config
from .resources.BigQueryDataset import BigQueryDatasetResource
from .resources.SparkClusterResource import SparkClusterResource
from .resources.ObjectStorageResource import ObjectStorageResource
from .resources.AlbionAPIResource import AlbionAPIResource
from .assets import dbt,albion


all_assets = load_assets_from_modules([dbt,albion])

defs = Definitions(
    assets=all_assets,
    resources={
        "raw":ObjectStorageResource(bucket_name=RAW_BUCKET_NAME),
        "staging":ObjectStorageResource(bucket_name=STAGING_BUCKET_NAME),
        "spark_cluster":SparkClusterResource(cluster_config=cluster_config),
        "dbt_bigquery":BigQueryDatasetResource(project_id=PROJECT_ID,dataset_id=DBT_DATASET_ID),
        "albion_api":AlbionAPIResource(host="www.albion-online-data.com"),
        "dbt":dbt.DbtCliResource(project_dir="dbt_assets")
    }
)
