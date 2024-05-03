
import glob
import pandas as pd
from .utils import generate_dummy_dataframe
from orch.utils.pandas import write_partitioned, read_partitioned
from tempfile import mkdtemp


def test_partitioned_no_partitions_parquet():
    # Create a temporary directory for testing
    tmpdir = mkdtemp()

    # Create a sample DataFrame
    df = generate_dummy_dataframe(1000)

    # Define the write function
    def write_func(df, path):
        df.to_parquet(path, compression="snappy",index=False)

    # Call the write_partitioned function
    write_partitioned(df, f"{tmpdir}/dummy_table/", [], write_func, "parquet")

    # Check if the files are written correctly
    assert  len(glob.glob(f"{tmpdir}/dummy_table/*.parquet")) != 0
    assert  len(glob.glob(f"{tmpdir}/dummy_table/*/*.parquet")) == 0

    df2 = read_partitioned(f"{tmpdir}/dummy_table/*.parquet",pd.read_parquet)
    
    assert set(df.columns) == set(df2.columns)
    assert len(df) == len(df2)
    assert set(df.dtypes) == set(df2.dtypes)


def test_partitioned_no_partitions_csv():
    # Create a temporary directory for testing
    tmpdir = mkdtemp()

    # Create a sample DataFrame
    df = generate_dummy_dataframe(1000)

    # Define the write function
    def write_func(df, path):
        df.to_csv(path,index=False)

    # Call the write_partitioned function
    write_partitioned(df, f"{tmpdir}/dummy_table/", [], write_func, "csv")

    # Check if the files are written correctly
    assert  len(glob.glob(f"{tmpdir}/dummy_table/*.csv")) != 0
    assert  len(glob.glob(f"{tmpdir}/dummy_table/*/*.csv")) == 0

    df2 = read_partitioned(f"{tmpdir}/dummy_table/*.csv",pd.read_csv)

    assert set(df.columns) == set(df2.columns)
    assert len(df) == len(df2)

def test_partitioned_with_partitions():
    # Create a temporary directory for testing
    tmpdir = mkdtemp()

    # Create a sample DataFrame
    df = generate_dummy_dataframe(1000)

    # Define the write function
    def write_func(df, path):
        df.to_parquet(path, compression="snappy",index=False)

    # Call the write_partitioned function
    write_partitioned(df, f"{tmpdir}/dummy_table/", ["gender","state"], write_func, "parquet")

    # Check if the files are written correctly
    assert  len(glob.glob(f"{tmpdir}/dummy_table/*.parquet")) == 0
    assert  len(glob.glob(f"{tmpdir}/dummy_table/*/*.parquet")) == 0
    assert  len(glob.glob(f"{tmpdir}/dummy_table/*/*/*.parquet")) != 0
    assert  len(glob.glob(f"{tmpdir}/dummy_table/*/*/*/*.parquet")) == 0

    df2 = read_partitioned(f"{tmpdir}/dummy_table/*/*/*.parquet",pd.read_parquet)

    assert set(df.columns) == set(df2.columns)
    assert len(df) == len(df2)
    assert set(df.dtypes) == set(df2.dtypes)

