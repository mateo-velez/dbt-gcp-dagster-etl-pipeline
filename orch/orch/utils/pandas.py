import os
import pandas as pd
import uuid
from .storage import extract_keys_from_path
from glob import glob
from typing import List
from datetime import datetime


def read_partitioned(path: str, read: callable) -> pd.DataFrame:
    """
    Read partitioned data from multiple files and concatenate them into a single DataFrame.

    Args:
        path (str): The path pattern to match the files.
        read (callable): A function to read each file and return a DataFrame.

    Returns:
        pd.DataFrame: The concatenated DataFrame.

    Example:
        >>> read_partitioned("my_folder/year=2022/month=*/day=*/*.csv", pd.read_csv)


        >>> read_partitioned("my_table/*.csv")

    """
    file_paths = glob(path)
    dfs = []
    for file_path in file_paths:
        try:
            df = read(file_path)
            keys = extract_keys_from_path(file_path)
            for key, value in keys.items():
                df[key] = value
            dfs.append(df)
        except pd.errors.EmptyDataError:
            continue

    return pd.concat(dfs, ignore_index=True)


def write_partitioned(
    df: pd.DataFrame,
    path: str,
    partition_columns: List[str],
    write: callable,
    suffix: str,
) -> None:
    """
    Write a DataFrame to disk, partitioned by specified columns.

    Args:
        df (pd.DataFrame): The DataFrame to be written.
        path (str): The base path where the partitioned data will be stored.
        partition_columns (List[str]): The list of column names to partition the data by.
        write (callable): A function that writes a DataFrame to disk.
        suffix (str): The file extension to be used for the partitioned files.

    Raises:
        AssertionError: If any of the partition columns are not present in the DataFrame.

    Returns:
        None

    Examples:
        >>> write_partitioned(df, "my_folder", ["year","month","day"], lambda df, path: df.to_parquet(df,path,index=False), "parquet")

    """
    if len(partition_columns) == 0:
        os.makedirs(path, exist_ok=True)
        write(df,os.path.join(path,f"{uuid.uuid4()}.{suffix}"))
    else:
        assert set(partition_columns).issubset(
            df.columns
        ), "All partition columns must be present in the DataFrame"
        cols = list(filter(lambda col: col not in partition_columns, df.columns))
        for k, v in df.groupby(partition_columns):
            partition_path = os.path.join(
                path, *[f"{col}={value}" for col, value in zip(partition_columns, k)]
            )
            os.makedirs(partition_path, exist_ok=True)
            write(v[cols], os.path.join(partition_path, f"{uuid.uuid4()}.{suffix}"))


if __name__ == "__main__":
    path = "/home/mateo/Desktop/sherlock/etl/data/raw-2024-03-16T14:12:59/albion_api_historical_prices/*/*.csv"
    df = read_partitioned(path, pd.read_csv)
    print(df)

    write_partitioned(
        df,
        f"/home/mateo/Desktop/sherlock/etl/data/tmp-{datetime.now().isoformat()}/albion_api_historical_prices",
        ["ingestion_timestamp", "quality", "location"],
        lambda df, path: df.to_csv(path, index=False),
        "csv",
    )
