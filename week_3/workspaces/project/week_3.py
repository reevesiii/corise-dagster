from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.config import REDIS, S3
from workspaces.project.sensors import get_s3_keys
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


@static_partitioned_config(partition_keys=list(map(str, (range(1, 11)))))
def docker_config(partition_keys):
    return {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_keys}.csv"}}},
}


machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": redis_resource},
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker_config,
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)

machine_learning_schedule_local = ScheduleDefinition(job=machine_learning_job_local, cron_schedule="*/15 * * * *")

@schedule(job=machine_learning_job_docker, cron_schedule="0 * * * *")
def machine_learning_schedule_docker():
    for partition_key in docker_config.get_partition_keys():
        yield RunRequest(
            run_key=partition_key,
            run_config=docker_config.get_run_config_for_partition_key(partition_key)
        )


@sensor(job=machine_learning_job_docker)
def machine_learning_sensor_docker():
    s3_keys = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url="http://localstack:4566")
    if s3_keys:
        for key in s3_keys:
            yield RunRequest(
                run_key=key,
                run_config={"resources": {"s3": {"config": S3}, "redis": {"config": REDIS}},
                            "ops": {"get_s3_data": {"config": {"s3_key": key}}}}
            )
    else:
        yield SkipReason("No new s3 files found in bucket.")


