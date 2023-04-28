from datetime import datetime
from typing import List

from dagster import (
    AssetSelection,
    Nothing,
    OpExecutionContext,
    ScheduleDefinition,
    String,
    asset,
    define_asset_job,
    load_assets_from_current_module,
)
from workspaces.types import Aggregation, Stock
from workspaces.config import REDIS, S3


@asset(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    description="Get a list of Stocks from an S3 file",
    op_tags={"kind": "s3"},
)
def get_s3_data(context: OpExecutionContext) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    stocks = [Stock.from_list(record) for record in context.resources.s3.get_data(s3_key)]
    return stocks

@asset(
    description="Given a list of stocks return the aggregation with the greatest value",
    op_tags={"kind": "python"},
)
def process_data(get_s3_data: List[Stock]) -> Aggregation:
    # aggregation = Aggregation(date=datetime(2022, 1, 1, 0, 0), high=10.0)
    max_item = max(get_s3_data, key=lambda x: x.high)
    aggregation = Aggregation(date=max_item.date, high=max_item.high)
    return aggregation


@asset(
    required_resource_keys={"redis"},
    description="Upload an Aggregation to Redis",
    op_tags={"kind": "redis"},
)
def put_redis_data(context: OpExecutionContext, process_data: Aggregation):
    name = process_data.date.strftime('%Y-%m-%d %H:%M:%S')
    value = str(process_data.high)
    context.resources.redis.put_data(name=name, value=value)


@asset(
    required_resource_keys={"s3"},
    description="Upload an Aggregation to S3 file",
    op_tags={"kind": "s3"},
)
def put_s3_data(context: OpExecutionContext, process_data: Aggregation):
    key_name = process_data.date.strftime('%Y-%m-%d %H:%M:%S')
    context.resources.s3.put_data(key_name=key_name, data=process_data)


local = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

project_assets = load_assets_from_current_module(
    group_name="pipeline"
)

machine_learning_asset_job = define_asset_job(
    name="machine_learning_asset_job",
    selection=AssetSelection.groups("pipeline"),
    config=local
)

machine_learning_schedule = ScheduleDefinition(job=machine_learning_asset_job, cron_schedule="*/15 * * * *")
