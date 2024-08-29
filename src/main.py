from datetime import datetime
from functools import reduce
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

SOURCE_DATA_PATH = Path("../raw_data/")
OUTPUT_PATH = Path("../output")
SPECIFIC_USER_AGENT = "some user agent"
ENTITIES = ("clicks", "impressions")


def prepare_entity_df(
    spark: SparkSession, entity: str, specific_user_name: str
) -> DataFrame:
    data = spark.read.load(
        str(SOURCE_DATA_PATH),
        format="parquet",
        pathGlobFilter=f"{entity}*.parquet",
    )
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
    spark = SparkSession.builder.appName("Adform Exercise").getOrCreate()

    bucket = []
    for entity in ENTITIES:
        bucket.append(
            prepare_entity_df(
                spark=spark, entity=entity, specific_user_name=SPECIFIC_USER_AGENT
            )
        )

    result = reduce(lambda a, b: a.join(b, on="datetime", how="outer"), bucket)
    result = result.na.fill(
        value=0, subset=[col for col in result.schema.names if col != "datetime"]
    )
    result = result.withColumn("hour", F.hour(F.col("datetime")))
    result = result.withColumn("Date", F.to_date(F.col("datetime")))
    result_columns = ["Date", "hour"] + [f"{entity}_count" for entity in ENTITIES]
    result.select(result_columns).write.format("csv").save(
        str(OUTPUT_PATH / f"result_{datetime.now()}"), header=True
    )

    [f.unlink() for f in SOURCE_DATA_PATH.glob("*.parquet")]


if __name__ == "__main__":
    main()
