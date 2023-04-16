from datetime import datetime
from typing import List
import json

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    description="Get a list of Stocks from an S3 file",
    tags={"kind": "s3"},
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    out={"stocks": Out(List[Stock], description="A list of Stock items")},
)
def get_s3_data(context: OpExecutionContext):
    s3_key = context.op_config["s3_key"]
    stocks = [Stock.from_list(record) for record in context.resources.s3.get_data(s3_key)]
    return stocks

@op(
    description="Given a list of stocks return the aggregation with the greatest value",
    ins={"stocks": In(List[Stock], description="A list of Stock items")},
    out={"aggregation": Out(Aggregation, description="A custom aggregation that takes the max of the high item")},
)
def process_data(stocks: List[Stock]):
    # aggregation = Aggregation(date=datetime(2022, 1, 1, 0, 0), high=10.0)
    max_item = max(stocks, key=lambda x: x.high)
    aggregation = Aggregation(date=max_item.date, high=max_item.high)
    return aggregation


@op(
    tags={"kind": "redis"},
    description="Upload an Aggregation to Redis",
    required_resource_keys={"redis"},
    ins={"aggregation": In(Aggregation, description="A custom aggregation that takes the max of the high item")},
    out={"nothing": Out(dagster_type=Nothing, description="Nothing to return")},
)
def put_redis_data(context: OpExecutionContext, aggregation: Aggregation):
    name = aggregation.date.strftime('%Y-%m-%d %H:%M:%S')
    value = str(aggregation.high)
    context.resources.redis.put_data(name=name, value=value)


@op(
    tags={"kind": "s3"},
    description="Upload an Aggregation to S3 file",
    required_resource_keys={"s3"},
    ins={"aggregation": In(Aggregation, description="A custom aggregation that takes the max of the high item")},
    out={"nothing": Out(dagster_type=Nothing, description="Nothing to return")},
)
def put_s3_data(context: OpExecutionContext, aggregation: Aggregation):
    key_name = aggregation.date.strftime('%Y-%m-%d %H:%M:%S')
    context.resources.s3.put_data(key_name=key_name, data=aggregation)



@graph
def machine_learning_graph():
    aggregation = process_data(get_s3_data())
    put_redis_data(aggregation)
    put_s3_data(aggregation)


local = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": redis_resource},
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
