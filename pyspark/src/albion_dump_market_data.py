from pyspark.sql import SparkSession, functions as F
import sys

def create_spark_session():
    """Create a spark session."""
    spark = SparkSession.builder.appName("Structuring Albion Market Data").getOrCreate()
    return spark

def prepare_data(data):
    """Cleans up SQL statements and splits them for Spark data frame creation."""
    cleaned_data = data.value[36:-1].replace("),(", ")\n(").translate(str.maketrans('', '', "'()"))
    return [record.split(',') for record in cleaned_data.split("\n")]

def process_data(spark, src,dst):
    """Loads and processes data from path, writes it as parquet."""
    raw_df = spark.read.text(src)
    clean_rdd = raw_df.filter(raw_df.value.startswith("INSERT INTO `market_history`")).rdd.flatMap(prepare_data)

    # Define schema
    schema_names = ["id", "item_amount", "silver_amount", "item_id", "location", "quality", "timestamp", "aggregation"]
    df = spark.createDataFrame(clean_rdd, schema_names)

    # Convert types
    df = df.selectExpr(
        "CAST(id AS LONG)",
        "CAST(item_amount AS LONG)",
        "CAST(silver_amount AS LONG)",
        "CAST(item_id AS STRING)",
        "CAST(location AS INT)",
        "CAST(quality AS INT)",
        "CAST(timestamp AS TIMESTAMP)",
        "CAST(aggregation AS INT)"
    )

    df.write.mode("overwrite").option("compression", "snappy").parquet(dst)


if __name__ == "__main__":
    spark = create_spark_session()
    src = sys.argv[1] # path to sql "gs://raw/dumps/*.sql"
    dst = sys.argv[2] # path to table destination "gs://staging/market_history/"
    process_data(spark, src, dst)