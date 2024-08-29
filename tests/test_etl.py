from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructField, StructType, TimestampType
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual

from main import prepare_entity_df


@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark


def test_prepare_entity_df_schemas_match(spark_fixture: SparkSession):
    source_path = Path("tests/source_folder")
    entity = "impressions"
    specific_user_agent = "some user agent"
    impressions_df = prepare_entity_df(
        spark=spark_fixture,
        specific_user_name=specific_user_agent,
        entity=entity,
        source_data_path=str(source_path),
    )
    expected_schema = StructType(
        [
            StructField("datetime", TimestampType(), False),
            StructField("impressions_count", LongType(), False),
        ]
    )
    assertSchemaEqual(impressions_df.schema, expected=expected_schema)


def test_prepare_entity_df_schemas_match(spark_fixture: SparkSession):
    source_path = Path("tests/source_folder")
    entity = "impressions"
    specific_user_agent = "some user agent"
    impressions_df = prepare_entity_df(
        spark=spark_fixture,
        specific_user_name=specific_user_agent,
        entity=entity,
        source_data_path=str(source_path),
    )
    expected_data = [
        {"datetime": datetime(2022, 5, 26, 11), "impressions_count": 4},
        {"datetime": datetime(2022, 5, 26, 12), "impressions_count": 0},
        {"datetime": datetime(2022, 5, 26, 13), "impressions_count": 0},
        {"datetime": datetime(2022, 5, 26, 14), "impressions_count": 0},
        {"datetime": datetime(2022, 5, 26, 15), "impressions_count": 0},
        {"datetime": datetime(2022, 5, 26, 16), "impressions_count": 0},
        {"datetime": datetime(2022, 5, 26, 17), "impressions_count": 0},
        {"datetime": datetime(2022, 5, 26, 18), "impressions_count": 0},
        {"datetime": datetime(2022, 5, 26, 19), "impressions_count": 7},
    ]
    schema = StructType(
        [
            StructField("datetime", TimestampType(), False),
            StructField("impressions_count", LongType(), False),
        ]
    )
    expected_data = spark_fixture.createDataFrame(expected_data, schema=schema)
    assertDataFrameEqual(impressions_df, expected=expected_data)
