"""Spark BATCH layer: seasonal density baselines using training-period data only."""

import os

from pyspark.sql import SparkSession, functions as F, types as T


def main():
    spark = (SparkSession.builder.appName("PEMS04-Lambda-Batch")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    schema = T.StructType([
        T.StructField("step", T.IntegerType()),
        T.StructField("timestamp", T.StringType()),
        T.StructField("sensor_id", T.IntegerType()),
        T.StructField("flow", T.DoubleType()),
        T.StructField("occupancy", T.DoubleType()),
        T.StructField("speed", T.DoubleType()),
        T.StructField("density", T.DoubleType()),
        T.StructField("day_of_week", T.IntegerType()),
        T.StructField("hour", T.IntegerType()),
    ])
    source = os.getenv("BATCH_INPUT", "/shared/batch_input/history.csv")
    output = os.getenv("BATCH_OUTPUT", "/shared/batch_output/seasonal")
    readings = spark.read.option("header", "true").schema(schema).csv(source)
    result = (readings.groupBy("sensor_id", "day_of_week", "hour")
              .agg(F.avg("density").alias("historical_mean_density"),
                   F.count("density").alias("sample_count"),
                   F.avg("speed").alias("historical_mean_speed"),
                   F.avg("occupancy").alias("historical_mean_occupancy")))
    result.coalesce(4).write.mode("overwrite").json(output)
    print(f"SPARK BATCH COMPLETE output={output}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
