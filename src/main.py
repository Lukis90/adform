import os
from datetime import datetime
from functools import reduce
from pathlib import Path

import pyspark.sql.functions as F
from dotenv import load_dotenv
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

load_dotenv()

SOURCE_DATA_PATH = Path("../raw_data/")
OUTPUT_PATH = Path("../output")
SPECIFIC_USER_AGENT = os.environ["SPECIFIC_USER_AGENT"]
ENTITIES = ("clicks", "impressions")
JDBC_DRIVER_PATH = os.environ["JDBC_DRIVER_PATH"]

with open(Path(__file__).parent.parent / "sec/db_root_password.txt") as f:
    DB_PASS = f.read()
TABLE_NAME = os.environ["TABLE_NAME"]
DB_NAME = os.environ["DB_NAME"]
POSTGRE_URL = f"jdbc:postgresql://localhost:5432/{DB_NAME}"
POSTGRE_PROPS = {
    "user": os.environ["DB_USER"],
    "password": DB_PASS,
    "driver": "org.postgresql.Driver",
}


def prepare_entity_df(
    spark: SparkSession, entity: str, specific_user_name: str, source_data_path: str
) -> DataFrame:
    try:
        data = spark.read.load(
            source_data_path,
            format="parquet",
            pathGlobFilter=f"{entity}*.parquet",
        )
    except AnalysisException as e:
        # most likely cause, no entity files were in source folder
        print(e)
        return
    data = data.select("device_settings").filter(
        data.device_settings["user_agent"] == F.lit(specific_user_name)
    )
    data = __add_columns(data)
    groups = data.groupby(["datetime"]).count()
    dates = __form_min_max_date_range(spark=spark, data=data)
    groups = dates.join(groups, on="datetime", how="left").select(
        dates["datetime"],
        F.coalesce(groups["count"], F.lit(0)).alias(f"{entity}_count"),
    )
    return groups


def __form_min_max_date_range(spark: SparkSession, data: DataFrame) -> DataFrame:
    date_range = data.select(
        F.min(F.col("datetime")).alias("min_datetime"),
        F.max(F.col("datetime")).alias("max_datetime"),
    ).first()

    return spark.sql(
        f"""
            SELECT explode(
                sequence(
                    to_timestamp('{date_range["min_datetime"]}'),
                    to_timestamp('{date_range["max_datetime"]}'),
                    interval 1 hour
                )
            ) AS datetime
        """
    )


def __add_columns(data: DataFrame) -> DataFrame:
    data = data.withColumn("filename", F.col("_metadata.file_name"))
    parts = F.split(data["filename"], "_")
    data = data.withColumn("datetime", F.substring(parts.getItem(3), 0, 10))
    data = data.withColumn("datetime", F.to_timestamp(F.col("datetime"), "yyyyMMddHH"))
    return data


def main():
    spark = (
        SparkSession.builder.appName("Adform Exercise")
        .config("spark.jars", JDBC_DRIVER_PATH)
        .getOrCreate()
    )

    result = prepare_output_result(
        spark=spark,
        entities=ENTITIES,
        specific_user_name=SPECIFIC_USER_AGENT,
        source_data_path=str(SOURCE_DATA_PATH),
    )
    result.select("Date", "hour", "impressions_count", "clicks_count").write.format(
        "csv"
    ).save(str(OUTPUT_PATH / f"result_{datetime.now()}"), header=True)
    result.select("datetime", "impressions_count", "clicks_count").write.jdbc(
        POSTGRE_URL, TABLE_NAME, mode="append", properties=POSTGRE_PROPS
    )

    # clean source folder
    [f.unlink() for f in SOURCE_DATA_PATH.glob("*.parquet")]


def prepare_output_result(
    spark: SparkSession,
    entities: list[str],
    specific_user_name: str,
    source_data_path: str,
) -> list[DataFrame] | None:
    bucket = []
    for entity in entities:
        res = prepare_entity_df(
            spark=spark,
            entity=entity,
            specific_user_name=specific_user_name,
            source_data_path=source_data_path,
        )
        if res:
            bucket.append(res)

    if len(bucket) == 0:
        print("Nothing to process.")
        return

    elif len(bucket) > 1:
        result = reduce(lambda a, b: a.join(b, on="datetime", how="outer"), bucket)
    elif "impressions_count" not in bucket[0].schema.names:
        result = bucket[0]
        result = result.withColumn("impressions_count", F.lit(0))
    else:
        result = bucket[0]
        result = result.withColumn("clicks_count", F.lit(0))
    result = result.na.fill(
        value=0, subset=[col for col in result.schema.names if col != "datetime"]
    )
    # add missing columns for csv output
    result = result.withColumn("hour", F.hour(F.col("datetime")))
    result = result.withColumn("Date", F.to_date(F.col("datetime")))
    return result


if __name__ == "__main__":
    main()
