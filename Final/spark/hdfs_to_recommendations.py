#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_PATH = "hdfs://namenode:8020/data/analytics-confluent/event"
JDBC_URL = "jdbc:postgresql://postgres:5432/shop"
JDBC_USER = "shop"
JDBC_PASSWORD = "shop"
TABLE = "recommendations"


def main() -> None:
    spark = (
        SparkSession.builder.appName("hdfs-to-recommendations")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    events = spark.read.format("avro").load(HDFS_PATH)

    recs = (
        events.filter(F.col("user").isNotNull() & (F.length(F.trim(F.col("user"))) > 0))
        .select(
            F.col("user"),
            F.explode("product_ids").alias("product_id"),
        )
        .filter(F.col("product_id").isNotNull() & (F.length(F.trim(F.col("product_id"))) > 0))
        .select(
            F.trim(F.col("user")).alias("user"),
            F.trim(F.col("product_id")).alias("product_id"),
        )
        .dropDuplicates(["user", "product_id"])
    )

    n = recs.count()
    print(f"rows to write: {n}")
    recs.show(20, truncate=False)

    (
        recs.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", TABLE)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

    print(f"wrote {n} rows into {TABLE}")
    spark.stop()


if __name__ == "__main__":
    main()
