from dagster import Definitions, load_assets_from_modules
from .assets import dbt

all_assets = load_assets_from_modules([dbt])

defs = Definitions(
    assets=all_assets,
    resources={
       "dbt":dbt.DbtCliResource(project_dir="dbt_assets")
    }
)
