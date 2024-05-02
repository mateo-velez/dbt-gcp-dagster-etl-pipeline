from dagster_dbt import dbt_assets, DbtCliResource
from dagster import AssetExecutionContext


@dbt_assets(manifest="dbt_assets/target/manifest.json")
def dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    args = ["build"]
    if context.has_tag("dbt_args"):
        args.append(context.get_tag("dbt_args"))

    yield from dbt.cli(args=args, context=context).stream()
