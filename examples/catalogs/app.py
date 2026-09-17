import os

from sqlalchemy import Column, String, Integer
from sqlalchemy_pysqlite3 import SQLiteDialect_pysqlite3

from datapipe.compute import Catalog
from datapipe.compute import DatapipeApp
from datapipe.compute import Pipeline
from datapipe.compute import Table
from datapipe.executor import ExecutorConfig
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn
from datapipe.store.pandas import TableStoreJsonLine

from .lib.file_list import ScanFileList
from .transformations import parse_cars
from .transformations import agg__price_by_manufacture_country
from .transformations import agg__price_by_color
from .transformations import agg__price_by_manufacture_country_and_color
from .transformations import generate__cars_docs
from .transformations import parse_countries
from .transformations import generate__cars_price_with_vat


SQLiteDialect_pysqlite3.supports_statement_cache = True


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

FILEPATH__RAW__CARS = os.path.join(CURRENT_DIR, "data/raw/cars/{file_name}.json")
FILEPATH__RAW__COUNTRIES = os.path.join(CURRENT_DIR, "data/raw/countries/{file_name}.json")

FILEPATH__PROCESSED__CARS = os.path.join(CURRENT_DIR, "data/processed/cars/{data}.jsonl")
FILEPATH__PROCESSED__COUNTRIES = os.path.join(CURRENT_DIR, "data/processed/countries/{data}.jsonl")


try:
    import pysqlite3
    sqla_engine = "sqlite+pysqlite3"
except ImportError:
    sqla_engine = "sqlite"


dbconn = DBConn(f"{sqla_engine}:///store.sqlite")
ds = DataStore(dbconn)


catalog = Catalog(
    {
        "cars_parsed": Table(
            store=TableStoreJsonLine(
                filename=FILEPATH__PROCESSED__CARS.format(data="cars_parsed"),
                primary_schema=[
                    Column("file_name", String, primary_key=True),
                    Column("model_id", Integer, primary_key=True),
                    Column("manufacture_country", String, primary_key=True),
                    Column("color", String, primary_key=True),
                ],
            )
        ),
        "price_by_manufacture_country": Table(
            store=TableStoreJsonLine(
                filename=FILEPATH__PROCESSED__CARS.format(data="price_by_manufacture_country"),
                primary_schema=[
                    Column("manufacture_country", String, primary_key=True),
                ],
            )
        ),
        "price_by_color": Table(
            store=TableStoreJsonLine(
                filename=FILEPATH__PROCESSED__CARS.format(data="price_by_color"),
                primary_schema=[
                    Column("color", String, primary_key=True),
                ],
            )
        ),
        "price_by_manufacture_country_and_color": Table(
            store=TableStoreJsonLine(
                filename=FILEPATH__PROCESSED__CARS.format(data="price_by_manufacture_country_and_color"),
                primary_schema=[
                    Column("manufacture_country", String, primary_key=True),
                    Column("color", String, primary_key=True),
                ],
            )
        ),
        "cars_docs": Table(
            store=TableStoreJsonLine(
                filename=FILEPATH__PROCESSED__CARS.format(data="cars_docs"),
                primary_schema=[
                    Column("file_name", String, primary_key=True),
                    Column("doc_name", String, primary_key=True),
                ],
            )
        ),
        "countries_parsed": Table(
            store=TableStoreJsonLine(
                filename=FILEPATH__PROCESSED__COUNTRIES.format(data="countries_parsed"),
                primary_schema=[
                    Column("file_name", String, primary_key=True),
                    Column("manufacture_country", String, primary_key=True),
                ],
            )
        ),
        "cars_price_with_vat": Table(
            store=TableStoreJsonLine(
                filename=FILEPATH__PROCESSED__CARS.format(data="cars_price_with_vat"),
                primary_schema=[
                    Column("file_name", String, primary_key=True),
                    Column("manufacture_country", String, primary_key=True),
                ],
            )
        ),
    }
)


pipeline = Pipeline(
    [
        ScanFileList(
            filename_pattern=FILEPATH__RAW__CARS,
            filename_output=FILEPATH__PROCESSED__CARS,
            output="cars_scanned",
            labels=[
                ("entity", "cars"),
                ("layer", "scan"),
                ("environment", "prod"),
            ],
        ),
        BatchTransform(
            parse_cars,
            inputs=["cars_scanned"],
            outputs=["cars_parsed"],
            kwargs={},
            chunk_size=1,
            executor_config=ExecutorConfig(parallelism=1),
            transform_keys=[
                "file_name",
            ],
            labels=[
                ("entity", "cars"),
                ("layer", "parse"),
                ("environment", "prod"),
            ],
        ),
        BatchTransform(
            agg__price_by_manufacture_country,
            inputs=["cars_parsed"],
            outputs=["price_by_manufacture_country"],
            kwargs={},
            chunk_size=1,
            executor_config=ExecutorConfig(parallelism=1),
            transform_keys=[
                "manufacture_country",
            ],
            labels=[
                ("entity", "cars"),
                ("layer", "agg"),
                ("environment", "prod"),
            ],
        ),
        BatchTransform(
            agg__price_by_color,
            inputs=["cars_parsed"],
            outputs=["price_by_color"],
            kwargs={},
            chunk_size=1,
            executor_config=ExecutorConfig(parallelism=1),
            transform_keys=[
                "color",
            ],
            labels=[
                ("entity", "cars"),
                ("layer", "agg"),
                ("environment", "prod"),
            ],
        ),
        BatchTransform(
            agg__price_by_manufacture_country_and_color,
            inputs=["cars_parsed"],
            outputs=["price_by_manufacture_country_and_color"],
            kwargs={},
            chunk_size=1,
            executor_config=ExecutorConfig(parallelism=1),
            transform_keys=[
                "manufacture_country",
                "color",
            ],
            labels=[
                ("entity", "cars"),
                ("layer", "agg"),
                ("environment", "prod"),
            ],
        ),
        BatchTransform(
            generate__cars_docs,
            inputs=["cars_parsed"],
            outputs=["cars_docs"],
            kwargs={
                "docs": ["passport", "license"],
            },
            chunk_size=10,
            executor_config=ExecutorConfig(parallelism=1),
            transform_keys=[
                "file_name",
            ],
            labels=[
                ("entity", "cars"),
                ("layer", "generate"),
                ("environment", "prod"),
            ],
        ),
        ScanFileList(
            filename_pattern=FILEPATH__RAW__COUNTRIES,
            filename_output=FILEPATH__PROCESSED__COUNTRIES,
            output="countries_scanned",
            labels=[
                ("entity", "countries"),
                ("layer", "scan"),
                ("environment", "prod"),
            ],
        ),
        BatchTransform(
            parse_countries,
            inputs=["countries_scanned"],
            outputs=["countries_parsed"],
            kwargs={},
            chunk_size=2,
            executor_config=ExecutorConfig(parallelism=1),
            transform_keys=[
                "file_name",
            ],
            labels=[
                ("entity", "cars"),
                ("layer", "parse"),
                ("environment", "prod"),
            ],
        ),
        BatchTransform(
            generate__cars_price_with_vat,
            inputs=["cars_parsed", "countries_parsed"],
            outputs=["cars_price_with_vat"],
            kwargs={},
            chunk_size=1,
            executor_config=ExecutorConfig(parallelism=1),
            transform_keys=[
                "manufacture_country",
            ],
            labels=[
                ("entity", "cars"),
                ("layer", "generate"),
                ("environment", "prod"),
            ],
        ),
    ]
)


app = DatapipeApp(ds, catalog, pipeline)
