import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
    job,
    op,
    graph,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    name="get_s3_data",
    description="Get a list of Stocks from an S3 file",
    tags={"kind": "s3"},
    config_schema={"s3_key": str},
    out={"stocks": Out(List[Stock], description="A list of Stock items")},
)
def get_s3_data_op(context) -> List[Stock]:
    stocks = list(csv_helper(context.op_config["s3_key"]))
    return stocks

@op(
    name="process_data",
    description="Given a list of stocks return the aggregation with the greatest value",
    ins={"stocks": In(List[Stock], description="A list of Stock items")},
    out={"aggregation": Out(Aggregation, description="A custom aggregation that takes the max of the high item")},
)
def process_data_op(stocks: List[Stock]) -> Aggregation:
    # aggregation = Aggregation(date=datetime(2022, 1, 1, 0, 0), high=10.0)
    max_item = max(stocks, key=lambda x: x.high)
    aggregation = Aggregation(date=max_item.date, high=max_item.high)
    return aggregation

@op(
    name="put_redis_data",
    tags={"kind": "redis"},
    description="Upload an Aggregation to Redis",
    ins={"aggregation": In(Aggregation, description="A custom aggregation that takes the max of the high item")},
    out={"nothing": Out(dagster_type=Nothing, description="Nothing to return")},
)
def put_redis_data_op(aggregation: Aggregation) -> Nothing:
    pass

@op(
    name="put_s3_data",
    tags={"kind": "s3"},
    description="Upload an Aggregation to S3 file",
    ins={"aggregation": In(Aggregation, description="A custom aggregation that takes the max of the high item")},
    out={"nothing": Out(dagster_type=Nothing, description="Nothing to return")},
)
def put_s3_data_op(aggregation: Aggregation) -> Nothing:
    pass

@job
def machine_learning_job():
    stocks = get_s3_data_op()
    aggregation = process_data_op(stocks)
    put_redis_data_op(aggregation)
    put_s3_data_op(aggregation)
