"""Spark Structured Streaming SPEED layer reading Kafka bridge's atomic JSON files."""

import json
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T


def main():
    landing = Path(os.getenv("SPARK_LANDING_DIR", "/shared/landing"))
    output = Path(os.getenv("SPEED_OUTPUT", "/shared/speed_output"))
    checkpoint = os.getenv("SPEED_CHECKPOINT", "/shared/checkpoints/speed")
    landing.mkdir(parents=True, exist_ok=True)
    output.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.appName("PEMS04-Lambda-Speed")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    schema = T.StructType([
        T.StructField("step", T.IntegerType()),
        T.StructField("timestamp", T.StringType()),
        T.StructField("sensor_id", T.IntegerType()),
        T.StructField("flow", T.DoubleType()),
        T.StructField("occupancy", T.DoubleType()),
        T.StructField("speed", T.DoubleType()),
        T.StructField("density", T.DoubleType()),
        T.StructField("synthetic", T.BooleanType()),
    ])
    incoming = (spark.readStream.schema(schema).option("maxFilesPerTrigger", 2)
                .json(str(landing)))

    def process_batch(microbatch, batch_id):
        observations = microbatch.groupBy("step", "timestamp").agg(
            F.count("sensor_id").alias("sensor_count"),
            F.avg("density").alias("mean_density"),
            F.max("density").alias("max_density"),
            F.avg("speed").alias("mean_speed"),
            F.sum(F.when(F.col("synthetic"), 1).otherwise(0)).alias("synthetic_observations"),
        )
        records = [row.asDict(recursive=True) for row in observations.collect()]
        target = output / f"batch_{int(batch_id):08d}.json"
        temporary = output / f".batch_{int(batch_id):08d}.{os.getpid()}.tmp"
        with temporary.open("w", encoding="utf-8") as fp:
            json.dump({"batch_id": int(batch_id), "rows": records}, fp)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(temporary, target)  # idempotent when a batch retries
        print(f"SPARK SPEED batch={batch_id} tick_count={len(records)}", flush=True)

    writer = (incoming.writeStream.foreachBatch(process_batch)
              .option("checkpointLocation", checkpoint))
    if os.getenv("SPEED_TRIGGER_AVAILABLE_NOW") == "1":
        # Bounded local/CI validation of the exact Structured Streaming path.
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="3 seconds")
    query = writer.start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
