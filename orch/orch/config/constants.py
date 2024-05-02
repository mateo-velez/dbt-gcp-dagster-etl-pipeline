from os import getenv


PROJECT_ID = getenv("PROJECT_ID")
REGION_ID = getenv("REGION_ID")
BRONZE_BUCKET_NAME = getenv("BRONZE_BUCKET_NAME")
SILVER_BUCKET_NAME = getenv("SILVER_BUCKET_NAME")
GOLD_BUCKET_NAME = getenv("GOLD_BUCKET_NAME")
MISC_BUCKET_NAME = getenv("MISC_BUCKET_NAME")
BRONZE_DATASET_ID = getenv("BRONZE_DATASET_ID")
SILVER_DATASET_ID = getenv("SILVER_DATASET_ID")
GOLD_DATASET_ID = getenv("GOLD_DATASET_ID")