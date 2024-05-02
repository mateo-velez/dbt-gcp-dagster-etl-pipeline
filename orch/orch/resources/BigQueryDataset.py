from datetime import datetime
from typing import List, Union
from dagster import ConfigurableResource
import logging
from ..config.constants import (
    PROJECT_ID,
    BRONZE_BUCKET_NAME,
    BRONZE_DATASET_ID,
)
from google.cloud import bigquery
import json


class BigQueryDatasetResource(ConfigurableResource):
    """Provides functions for interacting with a BigQuery data warehouse."""

    project_id: str
    dataset_id: str

    def load_truncate_data_from_uris(
        self, table_name: str, source_uris: Union[str, List[str]], format: str
    ) -> None:
        """
        Loads and truncates data from the specified source URIs into the specified table.

        Args:
            table_name (str): The name of the table to load the data into.
            source_uris (Union[str, List[str]]): The URIs of the source data to load into the table.
            format (str): The format of the source data. Supported formats are 'parquet' and 'avro'.

        Raises:
            ValueError: If the specified format is not supported.

        Returns:
            None

        Examples:
            >>> warehouse.load_truncate_data_from_uris("my_table", "gs://my-bucket/data.parquet", "parquet")

            >>> warehouse.load_truncate_data_from_uris("my_table", ["gs://my-bucket/*.avro", "gs://my-bucket/data2.avro"], "avro")
        """

        table_id = f"{self.dataset_id}.{table_name}"

        client = bigquery.Client(project=self.project_id)

        load_config = bigquery.LoadJobConfig()

        format = format.lower()
        if format == "parquet":
            load_config.source_format = bigquery.SourceFormat.PARQUET
        elif format == "avro":
            load_config.source_format = bigquery.SourceFormat.AVRO
        else:
            raise ValueError(f"Unsupported format: {format}")

        load_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        logging.debug(f"Loading data from {source_uris} to {table_id}")
        load_job = client.load_table_from_uri(
            source_uris=source_uris, destination=table_id, job_config=load_config
        )
        load_job.result()
        return None

    def export_data_to_uri(
        self, table_name: str, destination_uri: str, format: str
    ) -> None:
        """
        Export data from a table to a specified URI.

        Args:
            table_name (str): The name of the table to export data from.
            destination_uri (str): The URI where the exported data will be saved.
            format (str): The format in which the data will be exported. Supported formats are 'parquet' and 'avro'.

        Raises:
            ValueError: If the specified format is not supported.

        Returns:
            None

        Examples:
            >>> warehouse.export_data_to_uri("my_table", "gs://my-bucket/my_table/*", "parquet")

            >>> warehouse.export_data_to_uri("my_table", "gs://my-bucket/my_table.parquet", "avro")
        """
        table_id = f"{self.dataset_id}.{table_name}"
        client = bigquery.Client(project=self.project_id)
        format = format.lower()
        extract_config = bigquery.ExtractJobConfig()

        if format == "parquet":
            extract_config.compression = bigquery.Compression.SNAPPY
            extract_config.destination_format = bigquery.DestinationFormat.PARQUET
        elif format == "avro":
            extract_config.destination_format = bigquery.DestinationFormat.AVRO
        else:
            raise ValueError(f"Unsupported format: {format}")

        logging.debug("Extracting data from {table_name} to {destination_uri}")
        extract_job = client.extract_table(
            source=table_id,
            destination_uris=destination_uri,
            job_config=extract_config,
        )

        extract_job.result()
        ...

    def create_external_table(
        self, table_name: str, source_uri: str, format: str, partitioned: bool = False
    ) -> None:
        """
        Creates an external table.

        Args:
            table_name (str): The name of the table to be created.
            source_uri (str): The URI of the data source.
            format (str): The format of the data source. Supported formats are "PARQUET" and "AVRO".
            partitioned (bool, optional): Whether to enable partitioning for the external table. Defaults to False.

        Examples:
            >>> warehouse.create_external_table("my_external_table", "gs://my-bucket/*.parquet", "PARQUET")

            >>> warehouse.create_external_table("my_external_table", "gs://my-bucket/*.avro", "AVRO", partitioned=True)
        """
        format = format.upper()
        assert format in ["PARQUET", "AVRO"], "Unsupported format"
        table_id = f"{self.dataset_id}.{table_name}"

        if partitioned:
            logging.debug(f"Creating external table {table_id} with partitioning")
            self.query(
                f"""

            CREATE OR REPLACE EXTERNAL TABLE {table_id}
            WITH PARTITION COLUMNS
            OPTIONS(
                format = '{format}',
                uris = ['{source_uri}'],
                hive_partition_uri_prefix = '{source_uri.split("*")[0]}'
            );
            """
            )
        else:
            logging.debug(f"Creating external table {table_id}")
            self.query(
                f"""
            CREATE OR REPLACE EXTERNAL TABLE {table_id}
            OPTIONS(
                format = '{format}',
                uris = ['{source_uri}']
            );
            """
            )

    def query(self, query_string: str, **table_ids) -> None:
        """
        Executes a query on the data warehouse.

        Args:
            query_string (str): The SQL query string to be executed.
            **table_names: Keyword arguments representing the table names to be used in the query.

        Returns:
            None

        Examples:
            >>> warehouse.query("SELECT * FROM {table_name}", table_name="my_table")
        """
        formatted_query = query_string.format(
            **dict(
                (k, f"{self.dataset_id}.{v}")
                for k, v in table_ids.items()
                if isinstance(v, str)
            )
        )

        logging.debug(f"Executing query: {formatted_query}")
        client = bigquery.Client(project=self.project_id)
        job = client.query(query=formatted_query, timeout=300)
        job.result()

        stats = job._job_statistics()
        keys = [
            "totalBytesProcessed",
            "totalPartitionsProcessed",
            "totalBytesBilled",
            "billingTier",
            "totalSlotMs",
            "cacheHit",
            "numDmlAffectedRows",
            "statementType",
            "ddlOperationPerformed",
            "transferredBytes",
        ]
        stats_dict = {}
        for key in keys:
            if key in stats:
                stats_dict[key] = stats[key]
            else:
                stats_dict[key] = "Key not found in stats"

        stats_json = json.dumps(stats_dict)
        logging.debug(stats_json)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    warehouse = BigQueryDatasetResource(
        project_id=PROJECT_ID, dataset_id=BRONZE_DATASET_ID
    )

    dummy_table = "dummy_user"
    tm = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    logging.debug("Creating dummy table")

    warehouse.query(
        """
        CREATE OR REPLACE TABLE {dummy_table} AS (
        SELECT  id
            ,CONCAT(SPLIT('alice,bob,charlie,david,eve',',')[MOD(id,5)],MOD(id*31,991)) AS user_name
            ,DATE("1990-01-01") + INTERVAL MOD(id*31,991) HOUR                          AS creation_date
        FROM UNNEST
        (GENERATE_ARRAY(1, 10000)
        ) AS id);
    """,
        dummy_table=dummy_table,
    )

    logging.debug("Exporting data to URI")

    warehouse.export_data_to_uri(
        dummy_table,
        f"gs://{BRONZE_BUCKET_NAME}/{dummy_table}/export_timestamp={tm}/*.parquet",
        "parquet",
    )

    logging.debug("Creating external table")

    warehouse.create_external_table(
        f"external_{dummy_table}",
        f"gs://{BRONZE_BUCKET_NAME}/{dummy_table}/*.parquet",
        format="PARQUET",
        partitioned=True,
    )

    logging.debug("Creating latest table")

    warehouse.query(
        """
            CREATE OR REPLACE TABLE {latest_dummy_table} AS (
            SELECT *
            FROM {external_dummy_table}
            LIMIT 10000)
        """,
        external_dummy_table=f"external_{dummy_table}",
        latest_dummy_table=f"latest_{dummy_table}",
    )

    logging.debug("Loading data from external table to BigQuery")

    warehouse.load_truncate_data_from_uris(
        f"loaded_{dummy_table}",
        f"gs://{BRONZE_BUCKET_NAME}/{dummy_table}/*.parquet",
        "parquet",
    )
