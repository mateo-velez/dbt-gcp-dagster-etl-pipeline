import json
import os
from typing import List
from dagster import asset, AssetExecutionContext, DailyPartitionsDefinition
from ..utils.pandas import read_partitioned, write_partitioned
from ..resources.AlbionAPIResource import AlbionAPIResource
from ..resources.ObjectStorageResource import ObjectStorageResource
from ..resources.BigQueryDataset import BigQueryDatasetResource
from ..resources.SparkClusterResource import SparkClusterResource
from datetime import datetime, timedelta
from ..config.dataproc_config import dataproc_pyspark_job_config
import pandas as pd
import json
from ..utils.storage import (
    extract_key_from_path,
    make_workspace_dirs,
    get_utc_timestamp,
)


def get_concat_items_chunks(json_file_path: str, max_size: int = 3500) -> List[str]:
    with open(json_file_path, "r") as file:
        items = json.load(file)
    chunks = []
    current_chunk = []
    current_chunk_size = 0
    for item in items:
        current_chunk.append(item)
        current_chunk_size += len(item)
        if current_chunk_size > max_size:
            chunks.append(current_chunk)
            current_chunk = []
            current_chunk_size = 0
    return chunks


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
    group_name="albion",
)
def albion_api_historical_prices(
    context: AssetExecutionContext,
    albion_api: AlbionAPIResource,
    raw: ObjectStorageResource,
):
    name = "albion_api_historical_prices"
    current_timestamp = get_utc_timestamp()
    raw_dir, _ = make_workspace_dirs()

    to_date = context.partition_key
    from_date = (datetime.strptime(to_date, "%Y-%m-%d") - timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    chunks = get_concat_items_chunks("./files/items_id.json")

    context.log.info(
        f"Fetching prices from API: from_date={from_date}, to_date={to_date}, len(chunks)={len(chunks)}"
    )
    dfs = [
        albion_api.get_historical_prices(
            from_date=from_date, to_date=to_date, items=chunk
        )
        for chunk in chunks
    ]

    df = pd.concat(dfs)

    context.log.info(
        f"Writing dataframe to disk: len(df)={len(df)}, df.columns={df.columns}, df.dtypes={df.dtypes}, current_timestmap={current_timestamp}"
    )
    write_partitioned(
        df,
        f"{raw_dir}/{name}/ingestion_timestamp={current_timestamp}",
        [],
        lambda df, path: df.to_parquet(path, compression="snappy", index=False),
        suffix="parquet",
    )

    context.log.info(f"Uploading files to raw")
    raw.put(raw_dir)


@asset(deps=[albion_api_historical_prices], group_name="albion")
def inc_albion_api_market_price(
    context: AssetExecutionContext,
    raw: ObjectStorageResource,
    staging: ObjectStorageResource,
    dbt_bigquery: BigQueryDatasetResource,
):
    name = "inc_albion_api_market_price"
    dep_name = "albion_api_historical_prices"
    raw_dir, stg_dir = make_workspace_dirs()

    context.log.info(
        f"Listing staging mathcing files"
    )
    staging_blobs = staging.list(f"{name}/ingestion_timestamp=*/*.parquet")

    if len(staging_blobs) == 0:
        latest_ingestion_date = "0001-01-01T00:00:00"
    else:
        latest_ingestion_date = max(
            [
                extract_key_from_path(blob, "ingestion_timestamp")
                for blob in staging_blobs
            ]
        )

    context.log.info(
        f"Listing latest raw files: latest_ingestion_date={latest_ingestion_date}, len(staging_blobs)={len(staging_blobs)}"
    )
    raw_blobs = raw.list(f"{dep_name}/ingestion_timestamp=*/*.parquet")

    pruned_blobs = [
        blob
        for blob in raw_blobs
        if extract_key_from_path(blob, "ingestion_timestamp") > latest_ingestion_date
    ]

    if len(pruned_blobs) == 0:
        context.log.info(f"No new data to process")
    else:

        context.log.info(
            f"Getting blobs to local raw dir: len(pruned_blobs)={len(pruned_blobs)}, len(raw_blobs)={len(raw_blobs)}"
        )
        raw.get(pruned_blobs, raw_dir)

        context.log.info(f"Reading fetched data")
        df = read_partitioned(f"{raw_dir}/{dep_name}/*/*.parquet", pd.read_parquet)

        assert set(df.columns) == set(
            [
                "quality",
                "item_count",
                "avg_price",
                "timestamp",
                "location",
                "item_id",
                "ingestion_timestamp",
            ]
        )
        df["quality"] = df["quality"].astype(int)
        df["item_count"] = df["item_count"].astype(int)
        df["avg_price"] = df["avg_price"].astype(int)
        df["location"] = df["location"].astype(str)
        df["item_id"] = df["item_id"].astype(str)
        df["timestamp"] = pd.to_datetime(df["timestamp"]).astype("datetime64[s]")
        assert all(
            df[["item_id", "timestamp", "location", "quality", "ingestion_timestamp"]]
            .isnull()
            .sum()
            == 0
        )

        context.log.info(
            f"Writing transformed data to disk: len(df)={len(df)}, df.columns={df.columns}, df.dtypes={df.dtypes}"
        )
        write_partitioned(
            df,
            f"{stg_dir}/{name}/",
            ["ingestion_timestamp"],
            lambda df, path: df.to_parquet(path, compression="snappy", index=False),
            suffix="parquet",
        )

        context.log.info(f"Uploading files to staging")
        staging.put(stg_dir)

    context.log.info(f"Creating external table: name={name}, staging.get_bucket_name()={staging.get_bucket_name()}")
    dbt_bigquery.create_external_table(
        name,
        f"gs://{staging.get_bucket_name()}/{name}/*.parquet",
        format="parquet",
        partitioned=True,
    )


@asset(group_name="albion")
def full_albion_dump_market_price(
    context: AssetExecutionContext,
    raw: ObjectStorageResource,
    staging: ObjectStorageResource,
    dbt_bigquery: BigQueryDatasetResource,
    spark_cluster: SparkClusterResource,
):
    name = "full_albion_dump_market_price"

    src_path = f"gs://{raw.get_bucket_name()}/albion_dump_market_data/*/*.sql"
    dst_path = f"gs://{staging.get_bucket_name()}/{name}/"
    dataproc_pyspark_job_config["job"]["pysparkJob"]["args"] = [src_path,dst_path]

    context.log.info(f"Submitting spark job: dataproc_pyspark_job_config={dataproc_pyspark_job_config}")
    spark_cluster.submit_job(dataproc_pyspark_job_config)

    context.log.info(f"Creating external table albion_dump_market_price")
    dbt_bigquery.create_external_table(name, os.path.join(dst_path, "*.parquet"),format="parquet")
